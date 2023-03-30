package main

import (
	"bytes"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/gsearch"
	"github.com/elektito/gcrawler/pkg/utils"
	"github.com/pitr/gig"
)

type SearchResult struct {
	Id        int64
	Url       string
	Hostname  string
	Title     string
	Snippet   string
	UrlRank   float64
	HostRank  float64
	Relevance float64
	Verbose   bool
}

type Page struct {
	Query        string
	Duration     time.Duration
	Title        string
	Results      []SearchResult
	TotalResults uint64
	Verbose      bool
}

var curIdx bleve.Index
var idx bleve.IndexAlias
var idxReadyChan chan bool

func main() {
	go periodicIndex()

	idxReadyChan = make(chan bool)

	log.Println("Waiting for index to be ready...")
	<-idxReadyChan
	log.Println("Index is ready.")

	g := gig.Default()
	g.Handle("/search", handleNonVerboseSearch)
	g.Handle("/v/search", handleVerboseSearch)
	err := g.Run(
		config.GetBindAddrAndPort(),
		config.Config.Capsule.CertFile,
		config.Config.Capsule.KeyFile)
	utils.PanicOnErr(err)
}

func loadInitialIndex() (chosenIdx bleve.Index, err error) {
	pingFile := path.Join(config.Config.Index.Path, "ping.idx")
	pongFile := path.Join(config.Config.Index.Path, "pong.idx")

	_, err = os.Stat(pingFile)
	pingExists := (err == nil)

	_, err = os.Stat(pongFile)
	pongExists := (err == nil)

	err = nil

	if pingExists && pongExists {
		log.Println("Both ping and pong exist; checking...")
		pingIdx, pingErr := gsearch.OpenIndex(pingFile, "ping")
		pongIdx, pongErr := gsearch.OpenIndex(pongFile, "pong")
		if pingErr == nil && pongErr != nil {
			log.Println("Going with ping because there was an error opening pong.")
			chosenIdx = pingIdx
			return
		} else if pongErr == nil && pingErr != nil {
			log.Println("Going with pong because there was an error opening ping.")
			chosenIdx = pongIdx
			return
		} else if pingErr != nil && pongErr != nil {
			err = fmt.Errorf("Could not open either index file:\nping: %v\npong: %v", pingErr, pongErr)
			return
		}

		pingCount, pingErr := pingIdx.DocCount()
		pongCount, pongErr := pongIdx.DocCount()
		if pingErr == nil && pongErr != nil {
			log.Println("Going with ping because there was an error reading pong.")
			chosenIdx = pingIdx
			return
		} else if pongErr == nil && pingErr != nil {
			log.Println("Going with pong because there was an error reading ping.")
			chosenIdx = pongIdx
			return
		} else if pingErr != nil && pongErr != nil {
			err = fmt.Errorf("Could not read either index file:\nping: %v\npong: %v", pingErr, pongErr)
			return
		}

		if pingCount > pongCount {
			log.Printf(
				"Choosing ping index since it has more documents (%d) than pong (%d).\n",
				pingCount, pongCount)
			chosenIdx = pingIdx
		} else {
			log.Printf(
				"Choosing pong index since it has more documents (%d) than ping (%d).\n",
				pongCount, pingCount)
			chosenIdx = pongIdx
		}
	} else if pingExists {
		log.Println("Opening ping index...")
		chosenIdx, err = gsearch.OpenIndex(pingFile, "ping")
	} else if pongExists {
		log.Println("Opening pong index...")
		chosenIdx, err = gsearch.OpenIndex(pongFile, "pong")
	} else {
		log.Println("No index available. Creating ping index...")
		chosenIdx, err = gsearch.NewIndex(pingFile, "ping")
		if err != nil {
			return
		}

		err = gsearch.IndexDb(curIdx)
	}

	return
}

func periodicIndex() {
	pingFile := path.Join(config.Config.Index.Path, "ping.idx")
	pongFile := path.Join(config.Config.Index.Path, "pong.idx")

	curIdx, err := loadInitialIndex()
	utils.PanicOnErr(err)

	idx = bleve.NewIndexAlias(curIdx)
	idxReadyChan <- true

	var newIdxFile string
	var newIdxName string
	for {
		time.Sleep(1 * time.Hour)

		if curIdx.Name() == "ping" {
			newIdxFile = pingFile
			newIdxName = "ping"
		} else {
			newIdxFile = pongFile
			newIdxName = "pong"
		}

		err = os.RemoveAll(newIdxFile)
		utils.PanicOnErr(err)

		log.Println("Creating new index:", newIdxFile)
		newIdx, err := gsearch.NewIndex(newIdxFile, newIdxName)
		utils.PanicOnErr(err)

		err = gsearch.IndexDb(newIdx)
		utils.PanicOnErr(err)

		idx.Swap([]bleve.Index{newIdx}, []bleve.Index{curIdx})
		log.Println("Swapped in new index:", newIdxFile)

		curIdx = newIdx
	}
}

func search(q string, highlightStyle string) (results []SearchResult, dur time.Duration, nresults uint64) {
	start := time.Now()

	rr, err := gsearch.Search(q, idx, highlightStyle)
	utils.PanicOnErr(err)
	for _, r := range rr.Hits {
		snippet := strings.Join(r.Fragments["Content"], "")

		// this make sure snippets don't expand on many lines, and also
		// cruicially, formatted lines are not rendered in clients that do that.
		snippet = " " + strings.Replace(snippet, "\n", "…", -1)

		var hostname string
		u, err := url.Parse(r.ID)
		if err == nil {
			hostname = u.Hostname()
		}

		result := SearchResult{
			Url:       r.ID,
			Hostname:  hostname,
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

func handleVerboseSearch(c gig.Context) error {
	return handleSearch(c, true)
}

func handleNonVerboseSearch(c gig.Context) error {
	return handleSearch(c, false)
}

func handleSearch(c gig.Context, verbose bool) error {
	q, err := c.QueryString()
	utils.PanicOnErr(err)

	if q == "" {
		return c.NoContent(10, "Give me something!")
	}

	results, dur, nresults := search(q, "gem")
	utils.PanicOnErr(err)

	for i := 0; i < len(results); i++ {
		results[i].Verbose = verbose
	}

	text := renderSearchResults(results, dur, nresults, q)

	return c.GeminiBlob([]byte(text))
}

func renderSearchResults(results []SearchResult, dur time.Duration, nresults uint64, query string) string {
	t := `
{{- define "SingleResult" }}
=> {{ .Url }} {{ if .Title }} {{- .Title }} {{- else }} [Untitled] {{- end }} {{ if .Hostname }} ({{ .Hostname }}) {{ end }}
{{- if .Verbose }}
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

=> /search search

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
