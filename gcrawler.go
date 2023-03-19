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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/purell"
	"github.com/a-h/gemini"
	_ "github.com/lib/pq"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/transform"
)

const (
	dbConnStr                    = "dbname=gcrawler sslmode=disable host=/var/run/postgresql"
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
	contents       []byte
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
			if !strings.HasPrefix(resp.Header.Meta, "text/gemini") {
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

	return
}

func parsePage(body []byte, base *url.URL, contentType string) (links []string, title string) {
	doc, err := convertToString(body, contentType)
	if err != nil {
		fmt.Printf("Error converting to string: url=%s content-type=%s\n", base.String(), contentType)
		return
	}

	lines := strings.Split(doc, "\n")
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
			foundCanonicalTitle = true
			continue
		}

		if !strings.HasPrefix(line, "=>") {
			if title != "" {
				continue
			}

			title = strings.TrimSpace(line)

			if len(title) > maxTitleLength {
				title = title[:maxTitleLength]
			}

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

		if isBlacklisted(linkUrlStr, linkUrl) {
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
			links, title := parsePage(body, u, contentType)
			results <- VisitResult{
				url:         urlStr,
				statusCode:  code,
				links:       links,
				contents:    body,
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

		time.Sleep(2 * time.Second)
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
	contentType = parts[0]
	if len(parts) == 2 {
		args = strings.TrimSpace(parts[1])
	}
	return
}

func calcContentHash(contents []byte) string {
	hash := md5.Sum(contents)
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
		var destUrlId int64
		err = db.QueryRow(
			`insert into urls (url) values ($1)
                     on conflict (url) do update set url = excluded.url
                     returning id`,
			link,
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

	// insert redirect target as a possibly new url (we won't add it to
	// the links table though)
	_, err = db.Exec(`insert into urls (url) values ($1) on conflict do nothing`, r.redirectTarget)
	panicOnErr(err)

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

func flusher(c <-chan VisitResult) {
	db, err := sql.Open("postgres", dbConnStr)
	panicOnErr(err)
	defer db.Close()

	for r := range c {
		// update the original url record
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
	}
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

	outputUrl, _ = url.Parse(urlStr)

	return
}

func isBlacklisted(link string, parsedLink *url.URL) bool {
	blacklistedDomains := map[string]bool{
		"guardian.shit.cx": true,
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
		"gemini://geminispace.info/search/",
		"gemini://geminispace.info/v/",
	}

	for _, prefix := range blacklistedPrefixes {
		if strings.HasPrefix(link, prefix) {
			return true
		}
	}

	return false
}

func coordinator(nprocs int, visitorInputs []chan string, urlChan <-chan string) {
	host2ip := map[string]string{}

	seen := map[string]bool{}
	for link := range urlChan {
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
		default:
			// channel buffer is full. we won't do anything for now. the url
			// will be picked up again by the seeder later.
		}
	}

	fmt.Println("Exited coordinator")
}

func getDueUrls(c chan<- string) {
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(
		`select url from urls
                 where last_visit_time is null or
                       (status_code = 30 and last_visit_time + retry_time < now()) or
                       (status_code / 10 = 4 and last_visit_time + retry_time < now()) or
                       (last_visit_time is not null and last_visit_time + retry_time < now())`,
	)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var url string
		rows.Scan(&url)
		c <- url
	}
	close(c)
}

func seeder(output chan<- string) {
	for {
		c := make(chan string)
		go getDueUrls(c)
		for url := range c {
			output <- url
		}

		// since we just exhausted all urls, we'll wait a bit to allow for more
		// urls to be added to the database.
		time.Sleep(1 * time.Second)
	}
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
	// Setup an http server to make pprof stats available
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Set gemini default port for url normalizer
	unorm.DefaultPorts["gemini"] = 1965

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
	go coordinator(nprocs, inputUrls, urlChan)
	go seeder(urlChan)
	go flusher(visitResults)

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

		time.Sleep(1 * time.Second)
	}
}
