package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a-h/gemini"
	"github.com/elektito/gemplex/pkg/gcrawler"
	"github.com/elektito/gemplex/pkg/gparse"
	"github.com/elektito/gemplex/pkg/utils"
)

const (
	permanentErrorRetry          = "1 month"
	tempErrorMinRetry            = "1 day"
	revisitTimeIncrementNoChange = "2 days"
	revisitTimeAfterChange       = "2 days"
	maxRevisitTime               = "1 month"
	minRedirectRetryAfterChange  = "1 week"
	maxRedirects                 = 5
	crawlerUserAgent             = "elektito/gemplex"
	robotsErrorWaitTime          = 1 * time.Hour
)

type VisitResult struct {
	url         string
	error       error
	statusCode  int
	meta        string
	page        gparse.Page
	contents    []byte
	contentType string
	visitTime   time.Time
	banned      bool
}

// error type used to say there was an error fetching robots.txt
type RecentRobotsError struct{}

func (e RecentRobotsError) Error() string {
	return "Recent robots.txt error"
}

var _ error = (*RecentRobotsError)(nil)

func readGemini(ctx context.Context, client *gemini.Client, u *url.URL, visitorId string) (body []byte, code int, meta string, err error) {
	redirs := 0
redirect:
	resp, certs, auth, ok, err := client.RequestURL(ctx, u)
	if err != nil {
		log.Printf(
			"[crawl][%s] Request error: ok=%t auth=%t certs=%d err=%s\n",
			visitorId, ok, auth, len(certs), err)
		return
	}

	if ok {
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return
		}
	}

	if len(certs) == 0 {
		err = fmt.Errorf("[crawl] No TLS certificates received.")
		return
	}

	// Add certificate (trust on first use) and retry
	client.AddServerCertificate(u.Host, certs[0])

	resp, certs, auth, ok, err = client.RequestURL(ctx, u)
	if err != nil {
		log.Printf(
			"[crawl][%s] Request error: ok=%t auth=%t certs=%d err=%s\n",
			visitorId, ok, auth, len(certs), err)
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
				err = fmt.Errorf("Non-text doc: %s", resp.Header.Meta)
				return
			}

			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}
			return
		}

		if code/10 == 3 { // REDIRECT
			var target *url.URL
			target, err = url.Parse(meta)
			if err != nil {
				err = fmt.Errorf("Invalid redirect url '%s': %w", meta, err)
				return
			}

			redirs++
			if redirs == maxRedirects {
				err = fmt.Errorf("Too many redirects")
				return
			}
			log.Printf(
				"[crawl][%s] Redirecting to: %s (from %s)\n",
				visitorId, target.String(), u.String())
			u = target
			goto redirect
		}

		return
	}

	err = fmt.Errorf("Request error")
	return
}

func visitor(visitorId string, urls <-chan string, results chan<- VisitResult) {
	ctx := context.Background()
	client := gemini.NewClient()

	for urlStr := range urls {
		log.Printf("[crawl][%s] Processing: %s\n", visitorId, urlStr)
		u, _ := url.Parse(urlStr)

		body, code, meta, err := readGemini(ctx, client, u, visitorId)
		if err != nil {
			log.Printf("[crawl][%s] Error: %s url=%s\n", visitorId, err, urlStr)
			results <- VisitResult{
				url:        urlStr,
				meta:       meta,
				error:      err,
				statusCode: -1,
			}
			continue
		}

		if code/10 == 2 { // SUCCESS
			contentType := meta
			page, err := gparse.ParsePage(body, u, contentType)
			if err != nil {
				log.Printf("[crawl][%s]Error parsing page: %s\n", visitorId, err)
				results <- VisitResult{
					url:         urlStr,
					statusCode:  code,
					meta:        meta,
					contentType: contentType,
					visitTime:   time.Now(),
					error:       err,
				}
			} else {
				results <- VisitResult{
					url:         urlStr,
					statusCode:  code,
					meta:        meta,
					page:        page,
					contents:    body,
					contentType: contentType,
					visitTime:   time.Now(),
				}
			}
		} else {
			results <- VisitResult{
				url:        urlStr,
				meta:       meta,
				error:      fmt.Errorf("STATUS: %d META: %s", code, meta),
				statusCode: code,
			}
		}

		time.Sleep(1 * time.Second)
	}

	log.Printf("[crawl][%s] Exited.\n", visitorId)
}

func parseContentType(ct string) (contentType string, args string) {
	parts := strings.SplitN(ct, ";", 2)
	contentType = strings.TrimSpace(parts[0])
	if len(parts) == 2 {
		args = strings.TrimSpace(parts[1])
	}
	return
}

func calcContentHash(contents []byte) string {
	hash := md5.Sum(contents)
	return hex.EncodeToString(hash[:])
}

func updateDbBanned(db *sql.DB, r VisitResult) {
	q := `
update urls
set banned = $1
where url = $2
`
	_, err := db.Exec(q, r.banned, r.url)
	utils.PanicOnErr(err)
}

func updateDbSuccessfulVisit(db *sql.DB, r VisitResult) {
	tx, err := db.Begin()
	utils.PanicOnErr(err)

	ct, ctArgs := parseContentType(r.contentType)
	contentHash := calcContentHash(r.contents)

	var contentId int64
	var lang sql.NullString
	if r.page.Lang != "" {
		lang.String = r.page.Lang
		lang.Valid = true
	}

	var kind sql.NullString
	if r.page.Kind != "" {
		kind.String = r.page.Kind
		kind.Valid = true
	}

	// insert contents with a dummy update on conflict so that we can
	// get the id even in case of already existing data.
	err = db.QueryRow(
		`insert into contents
			    (hash, content, content_text, lang, kind, content_type, content_type_args, title, fetch_time)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                on conflict (hash)
                do update set hash = excluded.hash
                returning id
                `,
		contentHash, r.contents, r.page.Text, r.page.Lang, kind, ct, ctArgs, r.page.Title, r.visitTime,
	).Scan(&contentId)
	if err != nil {
		log.Println("[crawl] Database error when inserting contents for url:", r.url)
		panic(err)
	}

	var urlId int64
	err = db.QueryRow(
		`update urls set
                 last_visited = now(),
                 content_id = $1,
                 error = null,
                 status_code = $2,
                 retry_time = case when content_id = $1 then least(retry_time + $3, $4) else $5 end
                 where url = $6
                 returning id`,
		contentId, r.statusCode, revisitTimeIncrementNoChange, maxRevisitTime, revisitTimeAfterChange, r.url,
	).Scan(&urlId)
	if err == sql.ErrNoRows {
		log.Printf("[crawl] WARNING: URL not in the database, even though it should be; this is a bug! (%s)\n", r.url)
		return
	}
	if err != nil {
		log.Println("[crawl] Database error when updating url info:", r.url)
		panic(err)
	}

	// remove all existing links for this url
	_, err = db.Exec(`delete from links where src_url_id = $1`, urlId)
	if err != nil {
		log.Println("[crawl] Database error when deleting existing links for url:", r.url)
		panic(err)
	}

	for _, link := range r.page.Links {
		u, err := url.Parse(link.Url)
		if err != nil {
			continue
		}
		var destUrlId int64
		err = db.QueryRow(
			`insert into urls (url, hostname, first_added) values ($1, $2, now())
                     on conflict (url) do update set url = excluded.url
                     returning id`,
			link.Url, u.Host,
		).Scan(&destUrlId)
		if err != nil {
			log.Println("[crawl] DB error inserting link url:", link.Url)
		}
		utils.PanicOnErr(err)

		_, err = db.Exec(
			`insert into links values ($1, $2, $3)
                     on conflict do nothing`,
			urlId, destUrlId, link.Text)
		utils.PanicOnErr(err)
	}

	err = tx.Commit()
	utils.PanicOnErr(err)
}

func updateDbSlowDownError(db *sql.DB, r VisitResult) {
	// do whatever we do for temporary errors first
	updateDbTempError(db, r)

	// then also mark the hostname for slowdown
	uparsed, err := url.Parse(r.url)
	if err != nil {
		return
	}

	intervalInt, err := strconv.Atoi(r.meta)
	if err != nil {
		return
	}

	interval := time.Duration(intervalInt) * time.Second
	hostname := uparsed.Host

	q := `
update hosts
set slowdown_until = now() + $1
where hostname = $2
`
	_, err = db.Exec(q, interval, hostname)
	utils.PanicOnErr(err)
}

func updateDbPermanentError(db *sql.DB, r VisitResult) {
	_, err := db.Exec(
		`update urls set
                 last_visited = now(),
                 error = $1,
                 status_code = $2,
                 retry_time = $3
                 where url = $4`,
		r.error.Error(), r.statusCode, permanentErrorRetry, r.url)
	utils.PanicOnErr(err)
}

func updateDbTempError(db *sql.DB, r VisitResult) {
	// exponential retry
	_, err := db.Exec(
		`update urls set
                 last_visited = now(),
                 error = $1,
                 status_code = $2,
                 retry_time = case when retry_time is null then $3 else least(retry_time * 2, $4) end
                 where url = $5`,
		r.error.Error(), r.statusCode, tempErrorMinRetry, maxRevisitTime, r.url)
	utils.PanicOnErr(err)
}

func flusher(c <-chan VisitResult, done chan bool) {
	db, err := sql.Open("postgres", Config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

loop:
	for {
		select {
		case r := <-c:
			switch {
			// the error check in this clause is in case there was a
			// parsing/encoding error after the page was successfully fetched.
			case r.statusCode/10 == 2 && r.error == nil:
				updateDbSuccessfulVisit(db, r)
			case r.statusCode == 44: // SLOW DOWN
				updateDbSlowDownError(db, r)
			case r.statusCode/10 == 5: // TEMPORARY ERROR
				fallthrough
			case r.statusCode/10 == 1: // REQUIRES INPUT
				// for our purposes we'll consider requiring input the same as
				// permanent errors. we'll retry it, but a long time later.
				updateDbPermanentError(db, r)
			case r.banned:
				updateDbBanned(db, r)
			default:
				updateDbTempError(db, r)
			}
		case <-done:
			break loop
		}
	}

	done <- true
	log.Println("[crawl][flusher] Exited.")
}

func hashString(input string) uint64 {
	h := fnv.New64()
	h.Write([]byte(input))
	return h.Sum64()
}

func isBanned(parsedLink *url.URL, robotsPrefixes []string) bool {
	for _, prefix := range robotsPrefixes {
		if strings.HasPrefix(parsedLink.Path, prefix) {
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
					log.Printf("[crawl][coord] Error resolving host %s: %s\n", host, err)
					host2ip[host] = ""
					continue
				}
				if len(ips) == 0 {
					log.Printf("[crawl][coord] Error resolving host %s: empty response\n", host)
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
		}
	}

	log.Println("[crawl][coord] Exited.")
	done <- true
}

func getDueUrls(c chan<- string, done chan bool) {
	db, err := sql.Open("postgres", Config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

	rows, err := db.Query(`
select url from urls u
join hosts h on u.hostname = h.hostname
where not banned and (h.slowdown_until is null or h.slowdown_until < now()) and
   (last_visited is null or
    (status_code / 10 = 4 and last_visited + retry_time < now()) or
    (last_visited is not null and last_visited + retry_time < now()))
`)
	utils.PanicOnErr(err)
	defer rows.Close()

loop:
	for rows.Next() {
		var url string
		rows.Scan(&url)

		select {
		case c <- url:
		case <-done:
			break loop
		}
	}
	close(c)
}

func fetchRobotsRules(u *url.URL, client *gemini.Client, visitorId string) (prefixes []string, err error) {
	prefixes = make([]string, 0)

	robotsUrl, err := url.Parse("gemini://" + u.Host + "/robots.txt")
	if err != nil {
		return
	}

	body, code, _, err := readGemini(context.Background(), client, robotsUrl, visitorId)
	if err != nil {
		return
	}

	if code/10 == 5 {
		// no such file; return an empty list
		return
	}

	if code/10 != 2 {
		err = fmt.Errorf("Cannot read robots.txt for hostname %s: got code %d", u.Host, code)
		return
	}

	log.Println("[crawl] Found robots.txt for:", u.String())

	text := string(body)
	lines := strings.Split(text, "\n")
	curUserAgents := []string{"*"}
	readingUserAgents := true
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}

		directive := "user-agent:"
		if len(line) > len(directive) && strings.ToLower(line[:len(directive)]) == directive {
			if !readingUserAgents {
				curUserAgents = make([]string, 0)
			}
			readingUserAgents = true
			curUserAgents = append(curUserAgents, strings.TrimSpace(line[len(directive):]))
			continue
		}

		directive = "disallow:"
		if len(line) > len(directive) && strings.ToLower(line[:len(directive)]) == directive {
			readingUserAgents = false
			prefix := strings.TrimSpace(line[len(directive):])

		uaLoop:
			for _, ua := range curUserAgents {
				switch ua {
				case "*":
					fallthrough
				case crawlerUserAgent:
					fallthrough
				case "crawler":
					fallthrough
				case "indexer":
					fallthrough
				case "researcher":
					// an empty disallow (i.e "Disallow:"), means everything is
					// allowed.
					if prefix != "" {
						prefixes = append(prefixes, prefix)
					}
					break uaLoop
				}
			}
		}

		// ignore everything else as required in the spec
	}

	return
}

func seeder(output chan<- string, visitResults chan VisitResult, done chan bool) {
	client := gemini.NewClient()
	robotsRules := map[string][]string{}
	recentRobotsErrors := map[string]time.Time{}
	getOrFetchRobotsPrefixes := func(u *url.URL) (results []string, err error) {
		timestamp, ok := recentRobotsErrors[u.Host]
		if ok {
			wait := time.Now().Sub(timestamp)
			if wait > robotsErrorWaitTime {
				delete(recentRobotsErrors, u.Host)
			} else {
				err = &RecentRobotsError{}
				return
			}
		}

		results, ok = robotsRules[u.Host]
		if !ok {
			results, err = fetchRobotsRules(u, client, "seeder")
			if err != nil {
				recentRobotsErrors[u.Host] = time.Now()
				return
			}
			robotsRules[u.Host] = results
		}
		return
	}

	getDueDone := make(chan bool)
loop:
	for {
		c := make(chan string)
		go getDueUrls(c, getDueDone)
		for urlString := range c {
			urlParsed, err := url.Parse(urlString)
			if err != nil {
				continue
			}

			if gcrawler.IsBlacklisted(urlString, urlParsed) {
				continue
			}

			robotsPrefixes, err := getOrFetchRobotsPrefixes(urlParsed)
			if err != nil {
				if _, ok := err.(*RecentRobotsError); ok {
					// don't report these so logs aren't spammed
				} else {
					log.Printf("[crawl][seeder] Cannot read robots.txt for url %s: %s\n", urlString, err)
				}
				continue
			}
			if isBanned(urlParsed, robotsPrefixes) {
				visitResults <- VisitResult{
					url:    urlString,
					banned: true,
				}
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
		case <-time.After(10 * time.Second):
		case <-done:
			break loop
		}
	}

	getDueDone <- true
	done <- true
	log.Println("[crawl][seeder] Exited.")
}

func cleaner(done chan bool) {
	db, err := sql.Open("postgres", Config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

loop:
	for {
		result, err := db.Exec(`
delete from contents c
where not exists (
    select id from urls where content_id=c.id)`)
		utils.PanicOnErr(err)

		affected, err := result.RowsAffected()
		utils.PanicOnErr(err)
		if affected > 0 {
			log.Printf("[crawl][cleaner] Removed %d dangling objects from contents table.\n", affected)
		}

		select {
		case <-time.After(15 * time.Minute):
		case <-done:
			break loop
		}
	}

	done <- true
	log.Println("[crawl][cleaner] Exited.")
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
	log.Println(msg)
}

func dumpCrawlerState(filename string, nprocs int, urls [][]string) {
	f, err := os.Create(filename)
	utils.PanicOnErr(err)
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

	log.Println("[crawl] Dumped state to:", filename)
}

func crawl(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	nprocs := 500

	// create an array of channel, which will each serve as the input to each
	// processor.
	inputUrls := make([]chan string, nprocs)
	for i := 0; i < nprocs; i++ {
		inputUrls[i] = make(chan string, 1000)
	}

	visitResults := make(chan VisitResult, 10000)

	for i := 0; i < nprocs; i += 1 {
		go visitor(strconv.Itoa(i), inputUrls[i], visitResults)
	}

	urlChan := make(chan string, 100000)
	coordDone := make(chan bool)
	seedDone := make(chan bool)
	flushDone := make(chan bool)
	cleanDone := make(chan bool)
	go coordinator(nprocs, inputUrls, urlChan, coordDone)
	go seeder(urlChan, visitResults, seedDone)
	go flusher(visitResults, flushDone)
	go cleaner(cleanDone)

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
		log.Println("[crawl] Links in queue: ", nLinks, " outputQueue: ", len(visitResults))
		logSizeGroups(sizeGroups)

		select {
		case <-done:
			break loop
		case <-time.After(1 * time.Second):
		}
	}

	log.Println("[crawl] Shutting down workers...")
	seedDone <- true
	<-seedDone
	coordDone <- true
	<-coordDone
	flushDone <- true
	<-flushDone
	cleanDone <- true
	<-cleanDone

	log.Println("[crawl] Closing channels...")
	for _, c := range inputUrls {
		close(c)
	}

	log.Println("[crawl] Draining channels...")
	urls := make([][]string, nprocs)
	for i := 0; i < nprocs; i++ {
		urls[i] = make([]string, 0)
	}
	for i, c := range inputUrls {
		for u := range c {
			urls[i] = append(urls[i], u)
		}
	}

	if *CrawlerStateFile != "" {
		dumpCrawlerState(*CrawlerStateFile, nprocs, urls)
	}

	log.Println("[crawl] Done.")
}
