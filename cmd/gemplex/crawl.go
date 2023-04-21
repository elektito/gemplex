package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
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

	"git.sr.ht/~elektito/gemplex/pkg/gcrawler"
	"git.sr.ht/~elektito/gemplex/pkg/gparse"
	"git.sr.ht/~elektito/gemplex/pkg/utils"
	"github.com/a-h/gemini"
)

const (
	permanentErrorRetry          = "1 month"
	tempErrorMinRetry            = "1 day"
	revisitTimeIncrementNoChange = "2 days"
	revisitTimeAfterChange       = "2 days"
	maxRevisitTime               = "1 month"
	maxRedirects                 = 5
	crawlerUserAgent             = "elektito/gemplex"
	robotsTxtValidity            = "1 day"
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

	// set when this was a host-level visit (like robots.txt) and urls table
	// should not be updated.
	isHostVisit bool
}

type GeminiSlowdownError struct {
	Meta string
}

func (e *GeminiSlowdownError) Error() string {
	return fmt.Sprintf("Slow down: %s seconds", e.Meta)
}

func errGeminiSlowdown(meta string) *GeminiSlowdownError {
	return &GeminiSlowdownError{
		Meta: meta,
	}
}

var _ error = (*GeminiSlowdownError)(nil)

var ErrRobotsBackoff = fmt.Errorf("Backing off from fetching robots.txt")
var Db *sql.DB

func readGemini(ctx context.Context, client *gemini.Client, u *url.URL, visitorId string) (body []byte, code int, meta string, finalUrl *url.URL, err error) {
	redirs := 0
	finalUrl = u
redirect:
	resp, certs, auth, ok, err := client.RequestURL(ctx, u)
	if err != nil {
		log.Printf(
			"[crawl][%s] Request error for %s: ok=%t auth=%t certs=%d err=%s\n",
			visitorId, u, ok, auth, len(certs), err)
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
			"[crawl][%s] Request error for %s: ok=%t auth=%t certs=%d err=%s\n",
			visitorId, u, ok, auth, len(certs), err)
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
			finalUrl, err = gparse.NormalizeUrl(target)
			if err != nil {
				finalUrl = u
			}
			goto redirect
		}

		return
	}

	err = fmt.Errorf("Request error")
	return
}

func visitor(visitorId string, urls <-chan string, results chan<- VisitResult, done <-chan bool) {
	client := gemini.NewClient()
	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		<-done
		cancelFunc()
	}()

	for urlStr := range urls {
		log.Printf("[crawl][%s] Processing: %s\n", visitorId, urlStr)
		u, _ := url.Parse(urlStr)

		body, code, meta, u, err := readGemini(ctx, client, u, visitorId)
		if errors.Is(err, context.Canceled) {
			break
		}
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

func updateDbBanned(r VisitResult) {
	q := `
update urls
set banned = $1
where url = $2
`
	_, err := Db.Exec(q, r.banned, r.url)
	utils.PanicOnErr(err)
}

func updateDbSuccessfulVisit(r VisitResult) {
	tx, err := Db.Begin()
	utils.PanicOnErr(err)
	defer tx.Rollback()

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
	err = tx.QueryRow(
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
	err = tx.QueryRow(
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
	_, err = tx.Exec(`delete from links where src_url_id = $1`, urlId)
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
		err = tx.QueryRow(
			`insert into urls (url, hostname, first_added) values ($1, $2, now())
                     on conflict (url) do update set url = excluded.url
                     returning id`,
			link.Url, u.Host,
		).Scan(&destUrlId)
		if err != nil {
			log.Println("[crawl] DB error inserting link url:", link.Url)
		}
		utils.PanicOnErr(err)

		_, err = tx.Exec(
			`insert into links values ($1, $2, $3)
                     on conflict do nothing`,
			urlId, destUrlId, link.Text)
		utils.PanicOnErr(err)
	}

	err = tx.Commit()
	utils.PanicOnErr(err)
}

func updateDbSlowDownError(r VisitResult) {
	// if it's not a host-level visit (like robots.txt which is for an entire
	// host, not just a single url)...
	if !r.isHostVisit {
		// do whatever we do for temporary errors first
		updateDbTempError(r)
	}

	// then also mark the hostname for slowdown
	uparsed, err := url.Parse(r.url)
	if err != nil {
		return
	}

	intervalSeconds, err := strconv.Atoi(r.meta)
	if err != nil {
		return
	}

	q := `
update hosts
set slowdown_until = now() + make_interval(secs => $1)
where hostname = $2
`
	_, err = Db.Exec(q, intervalSeconds, uparsed.Host)
	utils.PanicOnErr(err)
}

func updateDbPermanentError(r VisitResult) {
	_, err := Db.Exec(
		`update urls set
                 last_visited = now(),
                 error = $1,
                 status_code = $2,
                 retry_time = $3
                 where url = $4`,
		r.error.Error(), r.statusCode, permanentErrorRetry, r.url)
	utils.PanicOnErr(err)
}

func updateDbTempError(r VisitResult) {
	// exponential retry
	_, err := Db.Exec(
		`update urls set
                 last_visited = now(),
                 error = $1,
                 status_code = $2,
                 retry_time = case when retry_time is null then $3 else least(retry_time * 2, $4) end
                 where url = $5`,
		r.error.Error(), r.statusCode, tempErrorMinRetry, maxRevisitTime, r.url)
	utils.PanicOnErr(err)
}

func flusher(c <-chan VisitResult, done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

loop:
	for {
		select {
		case r := <-c:
			switch {
			// the error check in this clause is in case there was a
			// parsing/encoding error after the page was successfully fetched.
			case r.statusCode/10 == 2 && r.error == nil:
				updateDbSuccessfulVisit(r)
			case r.statusCode == 44: // SLOW DOWN
				updateDbSlowDownError(r)
			case r.statusCode/10 == 5: // TEMPORARY ERROR
				fallthrough
			case r.statusCode/10 == 1: // REQUIRES INPUT
				// for our purposes we'll consider requiring input the same as
				// permanent errors. we'll retry it, but a long time later.
				updateDbPermanentError(r)
			case r.banned:
				updateDbBanned(r)
			default:
				updateDbTempError(r)
			}
		case <-done:
			break loop
		}
	}

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

func coordinator(nprocs int, visitorInputs []chan string, urlChan <-chan string, done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

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
}

func getDueUrls(ctx context.Context, c chan<- string) {
	rows, err := Db.QueryContext(ctx, `
select url from urls u
left join hosts h on u.hostname = h.hostname
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
		err = rows.Scan(&url)
		if errors.Is(err, context.Canceled) {
			break
		}
		utils.PanicOnErr(err)

		select {
		case c <- url:
		case <-ctx.Done():
			break loop
		}
	}
	close(c)
}

func fetchRobotsRules(ctx context.Context, u *url.URL, client *gemini.Client, visitorId string) (prefixes []string, err error) {
	prefixes = make([]string, 0)

	robotsUrl, err := url.Parse("gemini://" + u.Host + "/robots.txt")
	if err != nil {
		return
	}

	body, code, meta, finalUrl, err := readGemini(ctx, client, robotsUrl, visitorId)
	if err != nil {
		return
	}

	if code == 44 {
		err = errGeminiSlowdown(meta)
		return
	}

	if code/10 == 5 {
		// no such file; return an empty list
		return
	} else if code/10 != 2 {
		// we'll still treat it as an empty list, but we'll log something about
		// it
		log.Printf("Cannot read robots.txt for hostname %s: got code %d. Treating it as no robots.txt.", u.Host, code)
		return
	} else if finalUrl.String() != robotsUrl.String() {
		log.Printf("robots.txt redirected from %s to %s; treating it as no robots.txt.", robotsUrl.String(), finalUrl.String())
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

func getRobotsPrefixesFromDb(u *url.URL) (prefixes []string, validUntil time.Time, err error) {
	var prefixesStr sql.NullString
	var nextTryTime sql.NullTime
	var validUntilNullable sql.NullTime
	q := `
select
    robots_prefixes, robots_valid_until, robots_last_visited + robots_retry_time
from hosts
where hostname = $1`
	row := Db.QueryRow(q, u.Host)
	err = row.Scan(&prefixesStr, &validUntilNullable, &nextTryTime)
	if err == sql.ErrNoRows {
		return
	}
	utils.PanicOnErr(err)

	if nextTryTime.Time.After(time.Now()) {
		err = ErrRobotsBackoff
		return
	}

	if !prefixesStr.Valid {
		err = fmt.Errorf("No prefixes available")
		return
	}

	prefixes = strings.Split(prefixesStr.String, "\n")

	return
}

func updateRobotsRulesInDbWithError(u *url.URL, permanentError bool) {
	var err error
	if permanentError {
		q := `
insert into hosts
    (hostname, robots_last_visited, robots_retry_time, slowdown_until)
values
    ($1, now(), $2, now() + $2)
on conflict (hostname) do update
set robots_prefixes = null,
    robots_last_visited = now(),
    robots_retry_time = $2,
    slowdown_until = now() + $2`
		_, err = Db.Exec(q, u.Host, permanentErrorRetry)
	} else {
		q := `
insert into hosts
    (hostname, robots_last_visited, robots_retry_time, slowdown_until)
values
    ($1, now(), $2, now() + $2)
on conflict (hostname) do update
set robots_prefixes = null,
    robots_last_visited = now(),
    robots_retry_time = case when excluded.robots_retry_time is null
                        then $2
                        else least(excluded.robots_retry_time * 2, $3) end,
    slowdown_until = now() + (case when excluded.robots_retry_time is null
                              then $2
                              else least(excluded.robots_retry_time * 2, $3) end)`
		_, err = Db.Exec(q, u.Host, tempErrorMinRetry, maxRevisitTime)
	}

	utils.PanicOnErr(err)
}

func updateRobotsRulesInDbWithSuccess(u *url.URL, prefixes []string) {
	prefixesStr := strings.Join(prefixes, "\n")
	q := `
insert into hosts
    (hostname, robots_prefixes, robots_valid_until, robots_last_visited, robots_retry_time)
values
    ($3, $1, now() + $2, now(), null)
on conflict (hostname) do update set
    robots_prefixes = $1,
    robots_valid_until = now() + $2,
    robots_last_visited = now(),
    robots_retry_time = null
`
	_, err := Db.Exec(q, prefixesStr, robotsTxtValidity, u.Host)
	utils.PanicOnErr(err)
}

func isPermanentNetworkError(err error) bool {
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		return false
	}

	if strings.Contains(err.Error(), "no such host") {
		return true
	}

	if strings.Contains(err.Error(), "no route to host") {
		return true
	}

	return false
}

func seeder(output chan<- string, visitResults chan VisitResult, done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	client := gemini.NewClient()
	type RobotsRecord struct {
		prefixes   []string
		validUntil time.Time
		err        error
	}
	robotsCache := map[string]RobotsRecord{}
	getOrFetchRobotsPrefixes := func(ctx context.Context, u *url.URL) (results []string, err error) {
		hit, ok := robotsCache[u.Host]
		if ok && hit.validUntil.Before(time.Now()) {
			results = hit.prefixes
			err = hit.err
			return
		} else if ok {
			delete(robotsCache, u.Host)
		}

		results, validUntil, err := getRobotsPrefixesFromDb(u)
		if err == nil {
			robotsCache[u.Host] = RobotsRecord{
				prefixes:   results,
				validUntil: validUntil,
			}
			return
		} else if err == ErrRobotsBackoff {
			return
		}
		err = nil

		results, err = fetchRobotsRules(ctx, u, client, "seeder")
		var slowdownErr *GeminiSlowdownError
		if errors.Is(err, context.Canceled) {
			return
		} else if errors.As(err, &slowdownErr) {
			updateDbSlowDownError(VisitResult{
				url:         u.String(),
				meta:        slowdownErr.Meta,
				isHostVisit: true,
			})
			err = ErrRobotsBackoff
			return
		} else if err != nil {
			isPermanent := isPermanentNetworkError(err)
			updateRobotsRulesInDbWithError(u, isPermanent)
			err = ErrRobotsBackoff
			return
		}

		updateRobotsRulesInDbWithSuccess(u, results)
		return
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		<-done
		cancelFunc()
	}()

loop:
	for {
		c := make(chan string)
		go getDueUrls(ctx, c)
		for urlString := range c {
			urlParsed, err := url.Parse(urlString)
			if err != nil {
				continue
			}

			if gcrawler.IsBlacklisted(urlString, urlParsed) {
				continue
			}

			robotsPrefixes, err := getOrFetchRobotsPrefixes(ctx, urlParsed)
			if errors.Is(err, context.Canceled) {
				break loop
			}
			if err != nil {
				if err == ErrRobotsBackoff {
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
			case <-ctx.Done():
				break loop
			}
		}

		// since we just exhausted all urls, we'll wait a bit to allow for more
		// urls to be added to the database.
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			break loop
		}
	}

	log.Println("[crawl][seeder] Exited.")
}

func cleaner(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx, cancelFunc := context.WithCancel(context.Background())
	canceled := make(chan bool)
	go func() {
		<-done
		log.Println("[crawl][cleaner] Shutting down...")
		cancelFunc()
		canceled <- true
	}()

loop:
	for {
		start := time.Now()
		result, err := Db.ExecContext(ctx, `
delete from contents c
where not exists (
    select 1 from urls where content_id=c.id)`)
		if ctx.Err() == context.Canceled {
			break
		}
		utils.PanicOnErr(err)
		end := time.Now()
		elapsed := end.Sub(start).Round(time.Millisecond)

		affected, err := result.RowsAffected()
		utils.PanicOnErr(err)
		if affected > 0 {
			log.Printf("[crawl][cleaner] Removed %d dangling objects from contents table in %s.\n", affected, elapsed)
		} else {
			log.Printf("[crawl][cleaner] No dangling found objects in contents table (query took %s)\n", elapsed)
		}

		select {
		case <-time.After(15 * time.Minute):
		case <-canceled:
			break loop
		}
	}

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

	// open (and check) database for all workers to use
	var err error
	Db, err = sql.Open("postgres", Config.GetDbConnStr())
	utils.PanicOnErr(err)
	err = Db.Ping()
	utils.PanicOnErr(err)

	nprocs := 500

	// create an array of channel, which will each serve as the input to each
	// processor.
	inputUrls := make([]chan string, nprocs)
	visitorDone := make([]chan bool, nprocs)
	for i := 0; i < nprocs; i++ {
		inputUrls[i] = make(chan string, 1000)
		visitorDone[i] = make(chan bool)
	}

	visitResults := make(chan VisitResult, 10000)

	for i := 0; i < nprocs; i += 1 {
		go visitor(strconv.Itoa(i), inputUrls[i], visitResults, visitorDone[i])
	}

	urlChan := make(chan string, 100000)
	coordDone := make(chan bool, 1)
	seedDone := make(chan bool, 1)
	flushDone := make(chan bool, 1)
	cleanDone := make(chan bool, 1)
	subWg := &sync.WaitGroup{}
	go coordinator(nprocs, inputUrls, urlChan, coordDone, subWg)
	go seeder(urlChan, visitResults, seedDone, subWg)
	go flusher(visitResults, flushDone, subWg)
	go cleaner(cleanDone, subWg)
	subWg.Add(4)

	// i'd use math.MaxInt, but that causes time.After to wrap around it seems!
	logPeriod := 1000 * time.Hour
	if Config.Crawl.QueueStatusLogPeriod > 0 {
		logPeriod = time.Duration(Config.Crawl.QueueStatusLogPeriod) * time.Second
	}
loop:
	for {
		if Config.Crawl.QueueStatusLogPeriod > 0 {
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
		}

		select {
		case <-done:
			break loop
		case <-time.After(logPeriod):
		}
	}

	log.Println("[crawl] Shutting down workers...")
	seedDone <- true
	coordDone <- true
	flushDone <- true
	cleanDone <- true
	subWg.Wait()

	log.Println("[crawl] Closing channels...")
	for i, c := range inputUrls {
		close(c)
		visitorDone[i] <- true
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
