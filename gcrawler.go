package main

import (
	"bufio"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/gemini"
	unorm "github.com/sekimura/go-normalize-url"
)

type VisitResult struct {
	links []string
}

func ReadBody(r io.ReadCloser) (string, error) {
	buf := new(strings.Builder)
	_, err := io.Copy(buf, r)
	if err != nil {
		fmt.Println("Error reading body: ", err)
		return "", err
	}

	return buf.String(), nil
}

func ReadGemini(ctx context.Context, client *gemini.Client, u *url.URL) (body string, code int, meta string, err error) {
	resp, certs, auth, ok, err := client.RequestURL(ctx, u)
	if err != nil {
		fmt.Printf("Request error: ok=%t auth=%t certs=%d err=%s\n", ok, auth, len(certs), err)
		return
	}

	if ok {
		body, err = ReadBody(resp.Body)
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
		code, err = strconv.Atoi(string(resp.Header.Code))
		if err != nil {
			err = fmt.Errorf("Invalid response code: %s", resp.Header.Code)
			return
		}

		if code/10 == 1 { // INPUT response
			err = fmt.Errorf("Requested input with code: %d", code)
			return
		}

		if code/10 == 2 { // SUCCESS response
			if !strings.HasPrefix(resp.Header.Meta, "text/gemini") {
				err = fmt.Errorf("Not gemtext doc: %s", resp.Header.Meta)
				return
			}

			body, err = ReadBody(resp.Body)
			if err != nil {
				return
			}
			return
		}

		if code/10 == 3 { // REDIRECT response
			// return a body with one link (the redirect target)
			body = "=> " + resp.Header.Meta

			meta = resp.Header.Meta
			return
		}

		err = fmt.Errorf("Unacceptable response code: %d", code)
		return
	}

	err = fmt.Errorf("Request error")
	return
}

func GetLinks(doc string, base *url.URL) []string {
	lines := strings.Split(doc, "\n")
	links := make([]string, 0)
	for _, line := range lines {
		if !strings.HasPrefix(line, "=>") {
			continue
		}

		line = line[2:]
		line = strings.TrimLeft(line, " ")
		parts := strings.SplitAfterN(line, " ", 2)
		if len(parts) == 0 {
			continue
		}

		link := strings.TrimRight(parts[0], " ")
		linkUrl, err := url.Parse(link)
		if err != nil {
			continue
		}

		linkUrl = base.ResolveReference(linkUrl)
		links = append(links, linkUrl.String())
	}

	return links
}

func process(idx int, processorInput chan string, processorOutput chan VisitResult) {
	ctx := context.Background()
	client := gemini.NewClient()

	for urlStr := range processorInput {
		fmt.Printf("[%d] Processing: %s\n", idx, urlStr)
		u, _ := url.Parse(urlStr)

		body, code, meta, err := ReadGemini(ctx, client, u)
		if err != nil {
			fmt.Println("Error: url=", urlStr, " ", err)
			continue
		}

		_, _ = code, meta

		links := GetLinks(body, u)
		filteredLinks := make([]string, 0)
		for _, link := range links {
			if strings.HasPrefix(link, "gemini://") {
				filteredLinks = append(filteredLinks, link)
			}
		}

		processorOutput <- VisitResult{filteredLinks}

		time.Sleep(2 * time.Second)
	}

	fmt.Println("Exited processor")
}

func hashString(input string) uint64 {
	h := fnv.New64()
	h.Write([]byte(input))
	return h.Sum64()
}

func normalizeUrl(u string) (outputUrl string, err error) {
	outputUrl, err = unorm.Normalize(u)
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
	}

	for _, prefix := range blacklistedPrefixes {
		if strings.HasPrefix(link, prefix) {
			return true
		}
	}

	return false
}

type QueueItem struct {
	link string
	url  *url.URL
}

func coordinator(nprocs int, processorInput []chan string, processorOutputs chan VisitResult) {
	f, err := os.Create("links.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	host2ip := map[string]string{}

	seen := map[string]bool{}
	for visitResult := range processorOutputs {
		for _, link := range visitResult.links {
			link, err = normalizeUrl(link)
			if err != nil {
				continue
			}

			if _, ok := seen[link]; ok {
				continue
			}

			seen[link] = true

			u, err := url.Parse(link)
			if err != nil {
				continue
			}

			if u.Scheme != "gemini" {
				continue
			}

			if isBlacklisted(link, u) {
				continue
			}

			_, err = f.WriteString(link + "\n")
			if err != nil {
				panic(err)
			}

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
					continue
				}
				ip = ips[0].String()
				host2ip[host] = ip
			}

			n := int(hashString(ip) % uint64(nprocs))

			select {
			case processorInput[n] <- link:
			default:
				// channel buffer is full
				// TODO keep the link and try later
				fmt.Printf("Buffer %d is full. Dropping link: %s\n", n, link)
			}
		}
	}

	fmt.Println("Exited coordinator")
}

func seed(inputFile string, channel chan VisitResult) {
	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	n := 0
	scanner := bufio.NewScanner(f)
	links := make([]string, 0)
	for scanner.Scan() {
		links = append(links, scanner.Text())
		n++
	}
	channel <- VisitResult{links}

	fmt.Printf("Finished seeding with %d URLs.\n", n)
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
	processorInput := make([]chan string, nprocs)
	for i := 0; i < nprocs; i++ {
		processorInput[i] = make(chan string, 1000)
	}

	processorOutput := make(chan VisitResult, 10000)

	for i := 0; i < nprocs; i += 1 {
		go process(i, processorInput[i], processorOutput)
	}

	go coordinator(nprocs, processorInput, processorOutput)

	go seed("input.txt", processorOutput)

	for {
		nLinks := 0
		sizeGroups := map[int]int{}
		for _, channel := range processorInput {
			size := len(channel)
			nLinks += size

			if _, ok := sizeGroups[size]; ok {
				sizeGroups[size] += 1
			} else {
				sizeGroups[size] = 1
			}
		}
		fmt.Println("Links in queue: ", nLinks, " outputQueue: ", len(processorOutput))
		logSizeGroups(sizeGroups)

		time.Sleep(1 * time.Second)
	}
}
