package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/gemini"
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
		body, err = ReadBody(resp.Body)
		if err != nil {
			return
		}

		code, _ = strconv.Atoi(string(resp.Header.Code))
		meta = resp.Header.Meta
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

func process(ctx context.Context, linksToProcess chan string, linksToPossiblyProcess chan string) {
	client := gemini.NewClient()

	for urlStr := range linksToProcess {
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
				linksToPossiblyProcess <- link
			}
		}
	}

	fmt.Println("Exited processor")
}

func coordinator(linksToProcess chan string, linksToPossiblyProcess chan string) {
	f, err := os.Create("links.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	seen := map[string]bool{}
	for link := range linksToPossiblyProcess {
		if _, ok := seen[link]; ok {
			continue
		}

		fmt.Println("Adding: ", link)
		seen[link] = true
		linksToProcess <- link

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

	ctx := context.Background()

	linksToProcess := make(chan string, 100000)
	linksToPossiblyProcess := make(chan string, 100000)
	for i := 0; i < 500; i += 1 {
		go process(ctx, linksToProcess, linksToPossiblyProcess)
	}

	go coordinator(linksToProcess, linksToPossiblyProcess)

	// seed the crawler from the input file
	inputFile, err := os.Open("input.txt")
	if err != nil {
		panic(err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		linksToPossiblyProcess <- scanner.Text()
	}

	for {
		fmt.Println("Links in queue: ", len(linksToProcess))
		time.Sleep(1 * time.Second)
	}
}
