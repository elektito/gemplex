package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/PuerkitoBio/purell"
	"github.com/a-h/gemini"
	"github.com/elektito/gcrawler/pkg/config"
	_ "github.com/elektito/gcrawler/pkg/mgmt"
	_ "github.com/lib/pq"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/transform"
)

const (
	maxTitleLength               = 100
	permanentErrorRetry          = "1 month"
	tempErrorMinRetry            = "1 day"
	revisitTimeIncrementNoChange = "2 days"
	revisitTimeAfterChange       = "2 days"
	maxRevisitTime               = "1 month"
	minRedirectRetryAfterChange  = "1 week"
)

type VisitResult struct {
	url            string
	error          error
	statusCode     int
	links          []string
	contents       string
	contentType    string
	redirectTarget string
	title          string
	visitTime      time.Time
}

func readGeminia(ctx context.Context, client *gemini.Client, u *url.URL) (body []byte, code int, meta string, err error) {
	resp, certs, auth, ok, err := client.RequestURL(ctx, u)
	if err != nil {
		fmt.Printf("Request error: ok=%t auth=%t certs=%d err=%s\n", ok, auth, len(certs), err)
		return
	}

	if ok {
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}
	}

	if len(certs) == 0 {
		err = fmt.Errorf("No TLS certificates received.")
		return
	}

	// Add certificate (trust on first use) and retry
	client.AddServerCertificate(u.Host, certs[0])

	resp, certs, auth, ok, err = client.RequestURL(ctx, u)
	if err != nil {
		fmt.Printf("Request error: ok=%t auth=%t certs=%d err=%s\n", ok, auth, len(certs), err)
		return
	}

	if ok {
		meta = resp.Header.Meta
		code, err = strconv.Atoi(string(resp.Header.Code))
		if err != nil {
			err = fmt.Errorf("Invalid response code: %s", resp.Header.Code)
			return
		}

		if code/10 == 2 { // SUCCESS response
			if !strings.HasPrefix(resp.Header.Meta, "text/") {
				err = fmt.Errorf("Not gemtext doc: %s", resp.Header.Meta)
				return
			}

			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}
			return
		}

		return
	}

	err = fmt.Errorf("Request error")
	return
}

func convertToString(body []byte, contentType string) (s string, err error) {
	encoding, _, _ := charset.DetermineEncoding(body, contentType)

	reader := transform.NewReader(bytes.NewBuffer(body), encoding.NewEncoder())
	docBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		err = fmt.Errorf("Error converting text encoding: %w", err)
		return
	}

	s = string(docBytes)

	// postgres doesn't like null character in strings, even though it's valid
	// utf-8.
	s = strings.ReplaceAll(s, "\x00", "")

	s = strings.ToValidUTF8(s, "")

	return
}

func isMostlyAlphanumeric(s string) bool {
	if s == "" {
		return false
	}

	n := 0
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			n += 1
		}
	}

	return float64(n)/float64(len(s)) > 0.6
}

func parsePage(body []byte, base *url.URL, contentType string) (text string, links []string, title string, err error) {
	text, err = convertToString(body, contentType)
	if err != nil {
		fmt.Printf("Error converting to string: url=%s content-type=%s: %s\n", base.String(), contentType, err)
		return
	}

	switch {
	case strings.HasPrefix(contentType, "text/gemini"):
	case strings.HasPrefix(contentType, "text/plain"):
	case strings.HasPrefix(contentType, "text/markdown"):
	default:
		err = fmt.Errorf("Cannot process text type: %s", contentType)
		return
	}

	isGemtext := strings.HasPrefix(contentType, "text/gemini")
	lines := strings.Split(text, "\n")
	inPre := false
	foundCanonicalTitle := false
	for _, line := range lines {
		if strings.HasPrefix(line, "```") {
			inPre = !inPre
			continue
		}

		if inPre {
			continue
		}

		if strings.HasPrefix(line, "#") {
			if title != "" && foundCanonicalTitle {
				continue
			}

			line = strings.TrimSpace(line)
			title = strings.TrimSpace(line[1:])
			if !isMostlyAlphanumeric(title) {
				title = ""
			}
			foundCanonicalTitle = true
			continue
		}

		if !isGemtext || !strings.HasPrefix(line, "=>") {
			if title != "" {
				continue
			}

			title = strings.TrimSpace(line)
			if len(title) > maxTitleLength {
				title = title[:maxTitleLength]
			}

			if !isMostlyAlphanumeric(title) {
				title = ""
			}

			continue
		}

		if !isGemtext {
			continue
		}

		line = line[2:]
		line = strings.TrimLeft(line, " ")
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 0 {
			continue
		}

		linkUrlStr := strings.TrimSpace(parts[0])
		linkUrlStr = strings.ToValidUTF8(linkUrlStr, "")

		linkText := linkUrlStr
		if len(parts) == 2 {
			linkText = strings.TrimSpace(parts[1])
		}

		if title == "" {
			title = linkText
		}

		linkUrl, err := url.Parse(linkUrlStr)
		if err != nil {
			continue
		}

		// convert relative urls to absolute
		linkUrl = base.ResolveReference(linkUrl)

		linkUrl, err = normalizeUrl(linkUrl)
		if err != nil {
			continue
		}

		linkUrlStr = linkUrl.String()

		if linkUrl.Scheme != "gemini" {
			continue
		}

		links = append(links, linkUrlStr)
	}

	if title == "" {
		title = base.String()
	}

	title = strings.ToValidUTF8(title, "")

	return
}

func visitor(idx int, urls <-chan string, results chan<- VisitResult) {
	ctx := context.Background()
	client := gemini.NewClient()

	for urlStr := range urls {
		fmt.Printf("[%d] Processing: %s\n", idx, urlStr)
		u, _ := url.Parse(urlStr)

		body, code, meta, err := readGeminia(ctx, client, u)
		if err != nil {
			fmt.Println("Error: url=", urlStr, " ", err)
			results <- VisitResult{
				url:        urlStr,
				error:      err,
				statusCode: -1,
			}
			continue
		}

		switch {
		case code == 20: // SUCCESS
			contentType := meta
			text, links, title, err := parsePage(body, u, contentType)
			if err != nil {
				fmt.Printf("Error parsing page: %s\n", err)
				continue
			}
			results <- VisitResult{
				url:         urlStr,
				statusCode:  code,
				links:       links,
				contents:    text,
				contentType: contentType,
				title:       title,
				visitTime:   time.Now(),
			}
		case code/10 == 3: // REDIRECT
			results <- VisitResult{
				url:            urlStr,
				statusCode:     code,
				redirectTarget: meta,
			}
		default:
			results <- VisitResult{
				url:        urlStr,
				error:      fmt.Errorf("STATUS: %d META: %s", code, meta),
				statusCode: code,
			}
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Printf("[%d] Exited visitor.\n", idx)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func parseContentType(ct string) (contentType string, args string) {
	parts := strings.SplitN(ct, ";", 2)
	contentType = strings.TrimSpace(parts[0])
	if len(parts) == 2 {
		args = strings.TrimSpace(parts[1])
	}
	return
}

func calcContentHash(contents string) string {
	hash := md5.Sum([]byte(contents))
	return hex.EncodeToString(hash[:])
}

func updateDbSuccessfulVisit(db *sql.DB, r VisitResult) {
	tx, err := db.Begin()
	panicOnErr(err)

	ct, ctArgs := parseContentType(r.contentType)
	contentHash := calcContentHash(r.contents)

	// insert contents with a dummy update on conflict so that we can
	// get the id even in case of already existing data.
	var contentId int64
	err = db.QueryRow(
		`insert into contents
			    (hash, content, content_type, content_type_args, title, fetch_time)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (hash)
                do update set hash = excluded.hash
                returning id
                `,
		contentHash, r.contents, ct, ctArgs, r.title, r.visitTime,
	).Scan(&contentId)
	panicOnErr(err)

	var urlId int64
	err = db.QueryRow(
		`update urls set
                 last_visit_time = now(),
                 content_id = $1,
                 error = null,
                 status_code = $2,
                 redirect_target = null,
                 retry_time = case when content_id = $1 then least(retry_time + $3, $4) else $5 end
                 where url = $6
                 returning id`,
		contentId, r.statusCode, revisitTimeIncrementNoChange, maxRevisitTime, revisitTimeAfterChange, r.url,
	).Scan(&urlId)
	if err == sql.ErrNoRows {
		fmt.Println("WARNING: URL not in the database, even though it should be; this is a bug!")
		return
	}
	panicOnErr(err)

	for _, link := range r.links {
		u, err := url.Parse(link)
		if err != nil {
			continue
		}
		var destUrlId int64
		err = db.QueryRow(
			`insert into urls (url, hostname) values ($1, $2)
                     on conflict (url) do update set url = excluded.url
                     returning id`,
			link, u.Hostname(),
		).Scan(&destUrlId)
		panicOnErr(err)

		_, err = db.Exec(
			`insert into links values ($1, $2)
                     on conflict do nothing`,
			urlId, destUrlId)
		panicOnErr(err)
	}

	err = tx.Commit()
	panicOnErr(err)
}

func updateDbRedirect(db *sql.DB, r VisitResult) {
	tx, err := db.Begin()
	panicOnErr(err)

	_, err = db.Exec(
		`update urls set
                 last_visit_time = now(),
                 content_id = null,
                 error = null,
                 status_code = $1,
                 redirect_target = $2,
                 retry_time = case when redirect_target = $2 then retry_time * 2 else $3 end
                 where url = $4`,
		r.statusCode, r.redirectTarget, minRedirectRetryAfterChange, r.url)
	panicOnErr(err)

	u, err := url.Parse(r.redirectTarget)
	if err != nil {
		fmt.Printf("Invalid redirect target: %s\n", r.redirectTarget)
	} else {
		// insert redirect target as a possibly new url (we won't add it to
		// the links table though)
		_, err = db.Exec(
			`insert into urls (url, hostname) values ($1, $2) on conflict do nothing`,
			r.redirectTarget, u.Hostname())
		panicOnErr(err)
	}

	err = tx.Commit()
	panicOnErr(err)
}

func updateDbPermanentError(db *sql.DB, r VisitResult) {
	_, err := db.Exec(
		`update urls set
                 last_visit_time = now(),
                 error = $1,
                 status_code = $2,
                 retry_time = $3
                 where url = $4`,
		r.error.Error(), r.statusCode, permanentErrorRetry, r.url)
	panicOnErr(err)
}

func updateDbTempError(db *sql.DB, r VisitResult) {
	// exponential retry
	_, err := db.Exec(
		`update urls set
                 last_visit_time = now(),
                 error = $1,
                 status_code = $2,
                 retry_time = case when retry_time is null then $3 else least(retry_time * 2, $4) end
                 where url = $5`,
		r.error.Error(), r.statusCode, tempErrorMinRetry, maxRevisitTime, r.url)
	panicOnErr(err)
}

func flusher(c <-chan VisitResult, done chan bool) {
	db, err := sql.Open("postgres", config.GetDbConnStr())
	panicOnErr(err)
	defer db.Close()

loop:
	for {
		select {
		case r := <-c:
			switch {
			case r.statusCode/10 == 2:
				updateDbSuccessfulVisit(db, r)
			case r.statusCode/10 == 3: // REDIRECT
				updateDbRedirect(db, r)
			case r.statusCode/10 == 5: // TEMPORARY ERROR
				fallthrough
			case r.statusCode/10 == 1: // REQUIRES INPUT
				// for our purposes we'll consider requiring input the same as
				// permanent errors. we'll retry it, but a long time later.
				updateDbPermanentError(db, r)
			default:
				updateDbTempError(db, r)
			}
		case <-done:
			break loop
		}
	}

	done <- true
	fmt.Println("Exited flusher.")
}

func hashString(input string) uint64 {
	h := fnv.New64()
	h.Write([]byte(input))
	return h.Sum64()
}

func normalizeUrl(u *url.URL) (outputUrl *url.URL, err error) {
	// remove default gemini port, since purell only supports doing this with
	// http and https.
	if u.Scheme == "gemini" && u.Port() == "1965" {
		u.Host = strings.ReplaceAll(u.Host, ":1965", "")
	}

	flags := purell.FlagLowercaseScheme |
		purell.FlagLowercaseHost |
		purell.FlagUppercaseEscapes |
		purell.FlagDecodeUnnecessaryEscapes |
		purell.FlagEncodeNecessaryEscapes |
		purell.FlagRemoveEmptyQuerySeparator |
		purell.FlagRemoveDotSegments |
		purell.FlagRemoveDuplicateSlashes |
		purell.FlagSortQuery |
		purell.FlagRemoveEmptyPortSeparator |
		purell.FlagRemoveUnnecessaryHostDots
	urlStr := purell.NormalizeURL(u, flags)

	outputUrl, err = url.Parse(urlStr)

	return
}

func isBlacklisted(link string, parsedLink *url.URL) bool {
	blacklistedDomains := map[string]bool{
		"localhost":             true,
		"127.0.0.1":             true,
		"guardian.shit.cx":      true,
		"mastogem.picasoft.net": true, // wants us to slow down (status code: 44)
	}

	if _, ok := blacklistedDomains[parsedLink.Hostname()]; ok {
		return true
	}

	blacklistedPrefixes := []string{
		"gemini://gemi.dev/cgi-bin/",
		"gemini://caolan.uk/cgi-bin/weather.py/wxfcs",
		"gemini://illegaldrugs.net/cgi-bin/",
		"gemini://hoagie.space/proxy/",
		"gemini://tlgs.one/v/",
		"gemini://tlgs.one/search/",
		"gemini://tlgs.one/search_jump/",
		"gemini://tlgs.one/backlinks",
		"gemini://geminispace.info/search",
		"gemini://geminispace.info/v/",
		"gemini://gemini.bunburya.eu/remini/",
	}

	for _, prefix := range blacklistedPrefixes {
		if strings.HasPrefix(link, prefix) {
			return true
		}
	}

	return false
}

func coordinator(nprocs int, visitorInputs []chan string, urlChan <-chan string, done chan bool) {
	host2ip := map[string]string{}
	seen := map[string]bool{}

loop:
	for {
		select {
		case link := <-urlChan:
			if _, ok := seen[link]; ok {
				continue
			}

			seen[link] = true

			// urls should already be error checked (in GetLinks), so we ignore the
			// error here
			u, _ := url.Parse(link)

			host := u.Hostname()
			ip, ok := host2ip[host]
			if !ok {
				ips, err := net.LookupIP(host)
				if err != nil {
					fmt.Printf("Error resolving host %s: %s\n", host, err)
					host2ip[host] = ""
					continue
				}
				if len(ips) == 0 {
					fmt.Printf("Error resolving host %s: empty response\n", host)
					host2ip[host] = ""
					continue
				}
				ip = ips[0].String()
				host2ip[host] = ip
			}

			n := int(hashString(ip) % uint64(nprocs))

			select {
			case visitorInputs[n] <- link:
			case <-done:
				break loop
			default:
				// channel buffer is full. we won't do anything for now. the url
				// will be picked up again by the seeder later.
			}
		case <-done:
			break loop
		default:
		}
	}

	fmt.Println("Exited coordinator")
	done <- true
}

func getDueUrls(c chan<- string) {
	db, err := sql.Open("postgres", config.GetDbConnStr())
	panicOnErr(err)
	defer db.Close()

	rows, err := db.Query(
		`select url from urls
                 where last_visit_time is null or
                       (status_code = 30 and last_visit_time + retry_time < now()) or
                       (status_code / 10 = 4 and last_visit_time + retry_time < now()) or
                       (last_visit_time is not null and last_visit_time + retry_time < now())`,
	)
	panicOnErr(err)
	defer rows.Close()
	for rows.Next() {
		var url string
		rows.Scan(&url)
		c <- url
	}
	close(c)
}

func seeder(output chan<- string, done chan bool) {
loop:
	for {
		c := make(chan string)
		go getDueUrls(c)
		for urlString := range c {
			urlParsed, err := url.Parse(urlString)
			if err != nil {
				continue
			}
			if isBlacklisted(urlString, urlParsed) {
				continue
			}

			select {
			case output <- urlString:
			case <-done:
				break loop
			}
		}

		// since we just exhausted all urls, we'll wait a bit to allow for more
		// urls to be added to the database.
		select {
		case <-time.After(1 * time.Second):
		case <-done:
			break loop
		}
	}

	done <- true
	fmt.Println("Exited seeder.")
}

func logSizeGroups(sizeGroups map[int]int) {
	sortedSizes := make([]int, 0)
	for k := range sizeGroups {
		sortedSizes = append(sortedSizes, k)
	}
	sort.Ints(sortedSizes)

	msg := "channels [size:count]:"
	for _, size := range sortedSizes {
		count := sizeGroups[size]
		msg += fmt.Sprintf(" %d:%d", size, count)
	}
	fmt.Println(msg)
}

func main() {
	// Setup an http server for pprof and management ui
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	nprocs := 500

	// create an array of channel, which will each serve as the input to each
	// processor.
	inputUrls := make([]chan string, nprocs)
	for i := 0; i < nprocs; i++ {
		inputUrls[i] = make(chan string, 1000)
	}

	visitResults := make(chan VisitResult, 10000)

	for i := 0; i < nprocs; i += 1 {
		go visitor(i, inputUrls[i], visitResults)
	}

	urlChan := make(chan string, 100000)
	coordDone := make(chan bool)
	seedDone := make(chan bool)
	flushDone := make(chan bool)
	go coordinator(nprocs, inputUrls, urlChan, coordDone)
	go seeder(urlChan, seedDone)
	go flusher(visitResults, flushDone)

	// setup signal handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

loop:
	for {
		nLinks := 0
		sizeGroups := map[int]int{}
		for _, channel := range inputUrls {
			size := len(channel)
			nLinks += size

			if _, ok := sizeGroups[size]; ok {
				sizeGroups[size] += 1
			} else {
				sizeGroups[size] = 1
			}
		}
		fmt.Println("Links in queue: ", nLinks, " outputQueue: ", len(visitResults))
		logSizeGroups(sizeGroups)

		select {
		case <-sigs:
			fmt.Println("Received signal.")
			signal.Stop(sigs)
			break loop
		case <-time.After(1 * time.Second):
		}
	}

	fmt.Println("Shutting down workers...")
	seedDone <- true
	<-seedDone
	coordDone <- true
	<-coordDone
	flushDone <- true
	<-flushDone

	fmt.Println("Closing channels...")
	for _, c := range inputUrls {
		close(c)
	}

	fmt.Println("Draining channels...")
	urls := make([][]string, nprocs)
	for i := 0; i < nprocs; i++ {
		urls[i] = make([]string, 0)
	}
	for i, c := range inputUrls {
		for u := range c {
			urls[i] = append(urls[i], u)
		}
	}

	f, err := os.Create("state.gc")
	panicOnErr(err)
	defer f.Close()

	for i := 0; i < nprocs; i++ {
		if len(urls[i]) == 0 {
			continue
		}

		f.WriteString(fmt.Sprintf("---- channel %d ----\n", i))
		for _, u := range urls[i] {
			f.WriteString(u + "\n")
		}
	}

	fmt.Println("Wrote channel contents to state.gc")
	fmt.Println("Done.")
}
