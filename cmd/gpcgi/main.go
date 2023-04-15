package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"text/template"
	"time"

	"github.com/elektito/gemplex/pkg/config"
	"github.com/elektito/gemplex/pkg/gsearch"
	"github.com/elektito/gemplex/pkg/utils"
)

type Params struct {
	SearchDaemonSocket string
	ServerName         string
}

var (
	ErrPageNotFound = errors.New("Not found")
	ErrBadUrl       = errors.New("Bad URL")
)

func usage() {
	fmt.Printf(`Usage: %s [-config config-file] [-serve]

If you pass -serve, the program will run a test gemini server,
instead of running as a CGI script. This can be useful for
testing purposes.`, os.Args[0])
}

func main() {
	configFile := flag.String("config", "", "config file")
	serve := flag.Bool("serve", false, "start testing gemini sesrver, instead of running as a cgi script.")
	flag.Usage = usage
	flag.Parse()

	cfg := config.LoadConfig(*configFile)

	if *serve {
		// run as a gemini server. useful for debugging and testing.
		testServe(cfg)
		return
	}

	params := Params{
		SearchDaemonSocket: cfg.Search.UnixSocketPath,
		ServerName:         os.Getenv("SERVER_NAME"),
	}
	cgi(os.Stdin, os.Stdout, params)
}

func cgi(r io.Reader, w io.Writer, params Params) {
	scanner := bufio.NewScanner(r)
	ok := scanner.Scan()
	if !ok {
		log.Println("Could not read request line:", scanner.Err())
		return
	}

	urlStr := scanner.Text()
	u, err := url.Parse(urlStr)
	if err != nil {
		geminiHeader(w, 59, "Bad URL")
		return
	}

	if u.Hostname() != params.ServerName {
		geminiHeader(w, 53, "Unknown hostname in URL")
		return
	}

	if u.RawQuery == "" {
		geminiHeader(w, 10, "Search query")
		return
	}

	req, err := parseSearchRequest(u)
	if err == ErrPageNotFound {
		geminiHeader(w, 51, "Not Found")
		return
	} else if err == ErrBadUrl {
		geminiHeader(w, 59, "Bad URL")
		return
	} else if err != nil {
		cgiErr(w, "Internal error")
		return
	}

	conn, err := net.Dial("unix", params.SearchDaemonSocket)
	if err != nil {
		log.Println("Cannot connect to search backend:", err)
		cgiErr(w, "Cannot connect to search backend")
		return
	}

	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		log.Println("Error encoding search request:", err)
		cgiErr(w, "Internal error")
		return
	}

	var resp gsearch.SearchResponse
	err = json.NewDecoder(conn).Decode(&resp)
	if err != nil {
		cgiErr(w, "Internal error")
		return
	}

	if resp.Err != "" {
		log.Println("Error from search daemon:", resp.Err)
		cgiErr(w, "Internal error")
		return
	}

	geminiHeader(w, 20, "text/gemini")
	w.Write(renderResults(resp, req))
}

func renderResults(resp gsearch.SearchResponse, req gsearch.SearchRequest) []byte {
	type Page struct {
		Query        string
		QueryEscaped string
		Duration     time.Duration
		Title        string
		Results      []gsearch.SearchResult
		TotalResults uint64
		Verbose      bool
		Page         int
		PageCount    uint64
		BaseUrl      string
	}

	t := `
{{- define "SingleResult" }}
=> {{ .Url }} {{ if .Title }} {{- .Title }} {{- else }} [Untitled] {{- end }} {{ if .Hostname }} ({{ .Hostname }}) {{ end }}
{{- if verbose }}
* hrank: {{ .HostRank }}
* urank: {{ .UrlRank }}
* relevance: {{ .Relevance }}
{{- end }}
{{ .Snippet -}}
{{ end }}

{{- define "Results" }}
  {{ range . }}
    {{ template "SingleResult" . -}}
  {{ end}}
{{ end }}

{{- define "Page" -}}
# {{ .Title }}

=> {{ .BaseUrl }}/search search

Searching for: {{ .Query }}
Found {{ .TotalResults }} result(s) in {{ .Duration }}.

{{- template "Results" .Results }}
{{- if gt .Page 1 }}
=> {{ .BaseUrl }}/search/{{ dec .Page }}?{{ .QueryEscaped }} Prev Page ({{ dec .Page }} of {{ .PageCount }} pages)
{{- end }}
{{- if lt .Page .PageCount }}
=> {{ .BaseUrl }}/search/{{ inc .Page }}?{{ .QueryEscaped }} Next Page ({{ inc .Page }} of {{ .PageCount }} pages)
{{ end }}
=> / Home
{{ end -}}

{{- template "Page" . }}
`

	funcMap := template.FuncMap{
		"inc":     func(n int) int { return n + 1 },
		"dec":     func(n int) int { return n - 1 },
		"verbose": func() bool { return req.Verbose },
	}

	baseUrl := ""
	if req.Verbose {
		baseUrl = "/v"
	}

	npages := resp.TotalResults / gsearch.PageSize
	if resp.TotalResults%gsearch.PageSize != 0 {
		npages += 1
	}

	// fill in the Hostname field, since this is not ordinarily set by the
	// Search function (because we can always parse the url for reading the
	// hostname, but the templates are too dumb for that!)
	for _, r := range resp.Results {
		u, err := url.Parse(r.Url)
		if err == nil {
			r.Hostname = u.Hostname()
		}
	}

	tmpl := template.Must(template.New("root").Funcs(funcMap).Parse(t))
	data := Page{
		Query:        req.Query,
		QueryEscaped: url.QueryEscape(req.Query),
		Duration:     resp.Duration.Round(time.Millisecond / 10),
		Title:        "Gemplex Gemini Search",
		Results:      resp.Results,
		TotalResults: resp.TotalResults,
		Page:         req.Page,
		PageCount:    npages,
		BaseUrl:      baseUrl,
		Verbose:      req.Verbose,
	}
	var w bytes.Buffer
	err := tmpl.Execute(&w, data)
	utils.PanicOnErr(err)

	return w.Bytes()
}

func geminiHeader(w io.Writer, statusCode int, meta string) {
	msg := fmt.Sprintf("%d %s\r\n", statusCode, meta)
	w.Write([]byte(msg))
}

func cgiErr(w io.Writer, msg string) {
	msg = fmt.Sprintf("CGI Error: %s", msg)
	geminiHeader(w, 42, msg)
}

func parseSearchRequest(u *url.URL) (req gsearch.SearchRequest, err error) {
	// url format: [/v]/search[/page]
	re := regexp.MustCompile(`(?P<verbose>/v)?/search(?:/(?P<page>\d+))?`)
	m := re.FindStringSubmatch(u.Path)
	if m == nil {
		err = ErrPageNotFound
		return
	}

	// default value
	req.Page = 1

	for i, name := range re.SubexpNames() {
		switch name {
		case "verbose":
			if m[i] != "" {
				req.Verbose = true
			}
		case "page":
			pageStr := m[i]
			if pageStr != "" {
				req.Page, err = strconv.Atoi(pageStr)
				if err != nil {
					err = ErrBadUrl
					return
				}
			}
		}
	}

	req.Query, err = url.QueryUnescape(u.RawQuery)
	if err != nil {
		err = ErrBadUrl
		return
	}

	return
}
