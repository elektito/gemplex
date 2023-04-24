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
	"strings"
	"text/template"
	"time"

	"git.sr.ht/~elektito/gemplex/pkg/config"
	"git.sr.ht/~elektito/gemplex/pkg/gsearch"
	"git.sr.ht/~elektito/gemplex/pkg/utils"
	"github.com/dustin/go-humanize"
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

	switch {
	case strings.HasPrefix(u.Path, "/search"):
		fallthrough
	case strings.HasPrefix(u.Path, "/v/search"):
		handleSearch(u, r, w, params)
	case strings.HasPrefix(u.Path, "/image/random"):
		handleRandomImage(u, r, w, params)
	case strings.HasPrefix(u.Path, "/image/perm/"):
		handleImagePermalink(u, r, w, params)
	case strings.HasPrefix(u.Path, "/image/search"):
		handleImageSearch(u, r, w, params)
	default:
		geminiHeader(w, 51, "Not found")
	}
}

func handleRandomImage(u *url.URL, r io.Reader, w io.Writer, params Params) {
	var req struct {
		Type string `json:"t"`
	}

	var resp struct {
		Url       string    `json:"url"`
		Alt       string    `json:"alt"`
		Image     string    `json:"image"`
		FetchTime time.Time `json:"fetch_time"`
		ImageId   string    `json:"image_id"`
	}

	conn, err := net.Dial("unix", params.SearchDaemonSocket)
	if err != nil {
		log.Println("Cannot connect to search backend:", err)
		cgiErr(w, "Cannot connect to search backend")
		return
	}

	req.Type = "randimg"
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		log.Println("Error encoding search request:", err)
		cgiErr(w, "Internal error")
		return
	}

	err = json.NewDecoder(conn).Decode(&resp)
	if err != nil {
		cgiErr(w, "Internal error")
		return
	}

	t := `# 🖼️ Gemplex - Random Gemini Image

XXX {{ .Alt }}
{{ .Image }}
XXX

{{ if .Alt }}Alt: {{ .Alt }}{{ else }}No alt text.{{ end }}

Fetched at {{ .FetchTime }} from:
=> {{ .Url }} Source

=> /image/perm/{{ .ImageId }} ♾️ Permalink
=> / 🏠 Gemplex Home
`
	t = strings.Replace(t, "XXX", "```", 2)
	tmpl := template.Must(template.New("root").Parse(t))

	var out bytes.Buffer
	err = tmpl.Execute(&out, resp)
	utils.PanicOnErr(err)

	geminiHeader(w, 20, "text/gemini")
	w.Write(out.Bytes())
}

func handleImagePermalink(u *url.URL, r io.Reader, w io.Writer, params Params) {
	var req struct {
		Type string `json:"t"`
		Id   string `json:"id"`
	}

	var resp struct {
		Url       string    `json:"url"`
		Alt       string    `json:"alt"`
		Image     string    `json:"image"`
		FetchTime time.Time `json:"fetch_time"`
		ImageId   string    `json:"image_id"`
	}

	conn, err := net.Dial("unix", params.SearchDaemonSocket)
	if err != nil {
		log.Println("Cannot connect to search backend:", err)
		cgiErr(w, "Cannot connect to search backend")
		return
	}

	req.Type = "getimg"
	req.Id = u.Path[len("/image/perm/"):]
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		log.Println("Error encoding search request:", err)
		cgiErr(w, "Internal error")
		return
	}

	err = json.NewDecoder(conn).Decode(&resp)
	if err != nil {
		log.Println("Internal error:", err)
		cgiErr(w, "Internal error")
		return
	}

	if resp.ImageId == "" {
		geminiHeader(w, 51, "Not found")
		return
	}

	t := `# 🖼️ Gemplex - Random Gemini Image

XXX {{ .Alt }}
{{ .Image }}
XXX

{{ if .Alt }}Alt: {{ .Alt }}{{ else }}No alt text.{{ end }}

Fetched at {{ .FetchTime }} from:
=> {{ .Url }} Source

=> /image/random 🔀 Random Image
=> / 🏠 Gemplex Home
`
	t = strings.Replace(t, "XXX", "```", 2)
	tmpl := template.Must(template.New("root").Parse(t))

	var out bytes.Buffer
	err = tmpl.Execute(&out, resp)
	utils.PanicOnErr(err)

	geminiHeader(w, 20, "text/gemini")
	w.Write(out.Bytes())
}

func handleSearch(u *url.URL, r io.Reader, w io.Writer, params Params) {
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
		log.Println("Internal error:", err)
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

	var resp gsearch.PageSearchResponse
	err = json.NewDecoder(conn).Decode(&resp)
	if err != nil {
		log.Println("Internal error:", err)
		cgiErr(w, "Internal error")
		return
	}

	if resp.Err != "" {
		log.Println("Error from search daemon:", resp.Err)
		cgiErr(w, "Internal error")
		return
	}

	geminiHeader(w, 20, "text/gemini")
	w.Write(renderSearchResults(resp, req))
}

func renderSearchResults(resp gsearch.PageSearchResponse, req gsearch.PageSearchRequest) []byte {
	type Page struct {
		Query        string
		QueryEscaped string
		Duration     time.Duration
		Title        string
		Results      []gsearch.PageSearchResult
		TotalResults uint64
		Verbose      bool
		Page         int
		PageCount    uint64
		BaseUrl      string
	}

	t := `
{{- define "SingleResult" }}
=> {{ .Url }} {{ if .Title }} {{- .Title }} {{- else }} [Untitled] {{- end }}
* {{ .Hostname }} - {{ .ContentType }} - {{ human .ContentSize }}
{{- if verbose }}
* hrank: {{ .HostRank }}
* urank: {{ .UrlRank }}
* relevance: {{ .Relevance }}
{{- end }}
> {{ .Snippet -}}
{{ end }}

{{- define "Results" }}
  {{- range . }}
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
		"human":   func(n uint64) string { return humanize.Bytes(n) },
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
	for i, r := range resp.Results {
		u, err := url.Parse(r.Url)
		if err == nil {
			resp.Results[i].Hostname = u.Hostname()
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

func handleImageSearch(u *url.URL, r io.Reader, w io.Writer, params Params) {
	if u.RawQuery == "" {
		geminiHeader(w, 10, "Image search query")
		return
	}

	req, err := parseImageSearchRequest(u)
	if err == ErrPageNotFound {
		geminiHeader(w, 51, "Not Found")
		return
	} else if err == ErrBadUrl {
		geminiHeader(w, 59, "Bad URL")
		return
	} else if err != nil {
		log.Println("Internal error:", err)
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

	var resp gsearch.ImageSearchResponse
	err = json.NewDecoder(conn).Decode(&resp)
	if err != nil {
		log.Println("Internal error:", err)
		cgiErr(w, "Internal error")
		return
	}

	if resp.Err != "" {
		log.Println("Error from search daemon:", resp.Err)
		cgiErr(w, "Internal error")
		return
	}

	geminiHeader(w, 20, "text/gemini")
	w.Write(renderImageSearchResults(resp, req))
}

func renderImageSearchResults(resp gsearch.ImageSearchResponse, req gsearch.ImageSearchRequest) []byte {
	type Page struct {
		Query        string
		QueryEscaped string
		Duration     time.Duration
		Title        string
		Results      []gsearch.ImageSearchResult
		TotalResults uint64
		Verbose      bool
		Page         int
		PageCount    uint64
		BaseUrl      string
	}

	t := `
{{- define "SingleResult" }}
=> {{ permalink .ImageHash }} {{ .AltText }}
* Fetched: {{ .FetchTime }} - Source: {{ urlhost .SourceUrl }}
XXX {{ .AltText }}
{{ .Image }}
XXX
{{ end }}

{{- define "Results" }}
  {{- range . }}
    {{ template "SingleResult" . -}}
  {{ end}}
{{ end }}

{{- define "Page" -}}
# {{ .Title }}

=> {{ .BaseUrl }}/image/search search

Searching for: {{ .Query }}
Found {{ .TotalResults }} result(s) in {{ .Duration }}.

{{- template "Results" .Results }}
{{- if gt .Page 1 }}
=> {{ .BaseUrl }}/image/search/{{ dec .Page }}?{{ .QueryEscaped }} Prev Page ({{ dec .Page }} of {{ .PageCount }} pages)
{{- end }}
{{- if lt .Page .PageCount }}
=> {{ .BaseUrl }}/image/search/{{ inc .Page }}?{{ .QueryEscaped }} Next Page ({{ inc .Page }} of {{ .PageCount }} pages)
{{ end }}
=> / Home
{{ end -}}

{{- template "Page" . }}
`
	t = strings.Replace(t, "XXX", "```", 2)

	funcMap := template.FuncMap{
		"inc":   func(n int) int { return n + 1 },
		"dec":   func(n int) int { return n - 1 },
		"human": func(n uint64) string { return humanize.Bytes(n) },
		"urlhost": func(ustr string) string {
			u, err := url.Parse(ustr)
			if err != nil {
				return "unknown"
			}
			return u.Host
		},
		"permalink": func(ih string) string { return "/image/perm/" + ih },
	}

	baseUrl := ""
	npages := resp.TotalResults / gsearch.PageSize
	if resp.TotalResults%gsearch.PageSize != 0 {
		npages += 1
	}

	tmpl := template.Must(template.New("root").Funcs(funcMap).Parse(t))
	data := Page{
		Query:        req.Query,
		QueryEscaped: url.QueryEscape(req.Query),
		Duration:     resp.Duration.Round(time.Millisecond / 10),
		Title:        "Gemplex Gemini Image Search",
		Results:      resp.Results,
		TotalResults: resp.TotalResults,
		Page:         req.Page,
		PageCount:    npages,
		BaseUrl:      baseUrl,
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

func parseSearchRequest(u *url.URL) (req gsearch.PageSearchRequest, err error) {
	// url format: [/v]/search[/page]
	re := regexp.MustCompile(`(?P<verbose>/v)?/search(?:/(?P<page>\d+))?`)
	m := re.FindStringSubmatch(u.Path)
	if m == nil {
		err = ErrPageNotFound
		return
	}

	// default value
	req.Type = "search"
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

func parseImageSearchRequest(u *url.URL) (req gsearch.ImageSearchRequest, err error) {
	// url format: [/v]/search[/page]
	re := regexp.MustCompile(`/search(?:/(?P<page>\d+))?`)
	m := re.FindStringSubmatch(u.Path)
	if m == nil {
		err = ErrPageNotFound
		return
	}

	// default value
	req.Type = "searchimg"
	req.Page = 1

	for i, name := range re.SubexpNames() {
		switch name {
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
