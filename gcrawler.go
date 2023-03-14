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
	"strconv"
	"strings"
	"time"

	"github.com/a-h/gemini"
	unorm "github.com/sekimura/go-normalize-url"
)

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

func process(processorInput chan string, processorOutput chan string) {
	ctx := context.Background()
	client := gemini.NewClient()

	for urlStr := range processorInput {
		fmt.Println("Processing: ", urlStr)
		u, _ := url.Parse(urlStr)

		body, code, meta, err := ReadGemini(ctx, client, u)
		if err != nil {
			fmt.Println("Error: url=", urlStr, " ", err)
			return
		}

		_, _ = code, meta

		links := GetLinks(body, u)
		for _, link := range links {
			if strings.HasPrefix(link, "gemini://") {
				processorOutput <- link
			}
		}

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
	}

	for _, prefix := range blacklistedPrefixes {
		if strings.HasPrefix(link, prefix) {
			return true
		}
	}

	return false
}

func coordinator(nprocs int, processorInput []chan string, processorOutputs chan string) {
	f, err := os.Create("links.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	seen := map[string]bool{}
	for link := range processorOutputs {
		if _, ok := seen[link]; ok {
			continue
		}

		link, err = normalizeUrl(link)
		if err != nil {
			continue
		}

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

		host := u.Hostname()
		ips, err := net.LookupIP(host)
		if err != nil {
			fmt.Printf("Error resolving host %s: %s\n", host, err)
			continue
		}
		if len(ips) == 0 {
			continue
		}
		ip := ips[0]

		fmt.Println("Adding: ", link)
		seen[link] = true

		n := hashString(ip.String()) % uint64(nprocs)
		processorInput[n] <- link

		_, err = f.WriteString(link + "\n")
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Exited coordinator")
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

	processorOutput := make(chan string, 100000)

	for i := 0; i < nprocs; i += 1 {
		go process(processorInput[i], processorOutput)
	}

	go coordinator(nprocs, processorInput, processorOutput)

	// seed the crawler from the input file
	inputFile, err := os.Open("input.txt")
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		processorOutput <- scanner.Text()
	}

	for {
		nLinks := 0
		for _, channel := range processorInput {
			nLinks += len(channel)
		}
		fmt.Println("Links in queue: ", nLinks)
		time.Sleep(1 * time.Second)
	}
}
