package main

import (
	"bytes"
	"strings"
	"text/template"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/elektito/gcrawler/pkg/gsearch"
	"github.com/elektito/gcrawler/pkg/utils"
	"github.com/pitr/gig"
)

type SearchResult struct {
	Id        int64
	Url       string
	Title     string
	Snippet   string
	UrlRank   float64
	HostRank  float64
	Relevance float64
}

type Page struct {
	Query        string
	Duration     time.Duration
	Title        string
	Results      []SearchResult
	TotalResults uint64
}

var idx bleve.Index

func main() {
	var err error
	idx, err = gsearch.OpenIndex("idx.bleve")
	utils.PanicOnErr(err)

	g := gig.Default()
	g.Handle("/", gemRoot)
	err = g.Run("cert.pem", "key.pem")
	utils.PanicOnErr(err)
}

func search(q string, highlightStyle string) (results []SearchResult, dur time.Duration, nresults uint64) {
	start := time.Now()

	rr, err := gsearch.Search(q, idx, highlightStyle)
	utils.PanicOnErr(err)
	for _, r := range rr.Hits {
		snippet := strings.Join(r.Fragments["Content"], "")

		// this make sure snippets don't expand on many lines, and also
		// cruicially, formatted lines are not rendered in clients that do that.
		snippet = " " + strings.Replace(snippet, "\n", " / ", -1)

		result := SearchResult{
			Url:       r.ID,
			Title:     r.Fields["Title"].(string),
			Snippet:   snippet,
			UrlRank:   r.Fields["PageRank"].(float64),
			HostRank:  r.Fields["HostRank"].(float64),
			Relevance: r.Score,
		}
		results = append(results, result)
	}

	end := time.Now()
	dur = end.Sub(start)
	nresults = rr.Total

	return
}

func gemRoot(c gig.Context) error {
	q, err := c.QueryString()
	utils.PanicOnErr(err)

	if q == "" {
		return c.NoContent(10, "Give me something!")
	}

	results, dur, nresults := search(q, "gem")
	utils.PanicOnErr(err)

	text := renderSearchResults(results, dur, nresults, q)

	return c.Gemini(text)
}

func renderSearchResults(results []SearchResult, dur time.Duration, nresults uint64, query string) string {
	t := `
{{- define "SingleResult" }}
=> {{ .Url }} {{ .Title }}
hrank: {{ .HostRank }}
urank: {{ .UrlRank }}
relevance: {{ .Relevance }}

{{ .Snippet -}}
{{ end }}

{{- define "Results" }}
  {{ range . }}
    {{ template "SingleResult" . -}}
  {{ end}}
{{ end }}

{{- define "Page" -}}
# {{ .Title }}

=> / search

Searching for: {{ .Query }}
Found {{ .TotalResults }} result(s) in {{ .Duration }}.

{{- template "Results" .Results -}}

{{ end }}

{{- template "Page" . }}
`
	tmpl := template.Must(template.New("root").Parse(t))
	data := Page{
		Query:        query,
		Duration:     dur,
		Title:        "Elektito's Gem-Search",
		Results:      results,
		TotalResults: nresults,
	}
	var w bytes.Buffer
	err := tmpl.Execute(&w, data)
	utils.PanicOnErr(err)

	return w.String()
}
