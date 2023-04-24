package gsearch

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	_ "github.com/blevesearch/bleve/v2/search/highlight/highlighter/ansi"
	"github.com/lib/pq"

	"git.sr.ht/~elektito/gemplex/pkg/config"
	"git.sr.ht/~elektito/gemplex/pkg/gcrawler"
	"git.sr.ht/~elektito/gemplex/pkg/utils"
)

const PageSize = 15

type Doc struct {
	Title       string
	Content     string
	Lang        string
	Links       string
	PageRank    float64
	HostRank    float64
	Kind        string
	ContentType string
	ContentSize uint64
}

type RankedSort struct {
	desc          bool
	pageRankBytes []byte
	hostRankBytes []byte
}

type SearchRequest struct {
	// for search requests, this should be "search"
	Type string `json:"t"`

	Query          string `json:"q"`
	Page           int    `json:"page,omitempty"`
	HighlightStyle string `json:"-"`
	Verbose        bool   `json:"-"`
}

type SearchResult struct {
	Url         string  `json:"url"`
	Title       string  `json:"title"`
	Snippet     string  `json:"snippet"`
	UrlRank     float64 `json:"prank"`
	HostRank    float64 `json:"hrank"`
	Relevance   float64 `json:"score"`
	ContentType string  `json:"content_type"`
	ContentSize uint64  `json:"content_size"`

	// used by templates; this is _not_ set by the Search function.
	Hostname string `json:"-"`
}

type SearchResponse struct {
	TotalResults uint64         `json:"n"`
	Results      []SearchResult `json:"results"`
	Duration     time.Duration  `json:"duration"`

	// used by the search daemon and cgi
	Err string `json:"err,omitempty"`
}

func (so *RankedSort) UpdateVisitor(field string, term []byte) {
	switch field {
	case "PageRank":
		if len(term) > len(so.pageRankBytes) {
			so.pageRankBytes = make([]byte, len(term))
			copy(so.pageRankBytes, term)
		}
	case "HostRank":
		if len(term) > len(so.hostRankBytes) {
			so.hostRankBytes = make([]byte, len(term))
			copy(so.hostRankBytes, term)
		}
	}
}

func (so *RankedSort) Value(a *search.DocumentMatch) string {
	prp, err := numeric.PrefixCoded(so.pageRankBytes).Int64()
	utils.PanicOnErr(err)
	pr := math.Float64frombits(uint64(prp))

	hrp, err := numeric.PrefixCoded(so.hostRankBytes).Int64()
	utils.PanicOnErr(err)
	hr := math.Float64frombits(uint64(hrp))

	so.pageRankBytes = so.pageRankBytes[:0]
	so.hostRankBytes = so.hostRankBytes[:0]

	_, _ = pr, hr
	score := numeric.Float64ToInt64((a.Score + 1) * (pr + 1))

	return string(numeric.MustNewPrefixCodedInt64(score, 0))
}

func (so *RankedSort) Descending() bool {
	return so.desc
}

func (so *RankedSort) RequiresDocID() bool {
	return false
}

func (so *RankedSort) RequiresScoring() bool {
	return false
}

func (so *RankedSort) RequiresFields() []string {
	return []string{"PageRank", "HostRank"}
}

func (so *RankedSort) Reverse() {
	so.desc = !so.desc
}

func (so *RankedSort) Copy() search.SearchSort {
	prb := make([]byte, len(so.pageRankBytes))
	hrb := make([]byte, len(so.hostRankBytes))
	copy(so.pageRankBytes, prb)
	copy(so.hostRankBytes, hrb)
	return &RankedSort{
		desc:          so.desc,
		pageRankBytes: prb,
		hostRankBytes: hrb,
	}
}

func NewIndex(path string, name string) (idx bleve.Index, err error) {
	idxMapping := bleve.NewIndexMapping()

	docMapping := bleve.NewDocumentMapping()

	titleFieldMapping := bleve.NewTextFieldMapping()
	docMapping.AddFieldMappingsAt("Title", titleFieldMapping)

	contentFieldMapping := bleve.NewTextFieldMapping()
	docMapping.AddFieldMappingsAt("Content", contentFieldMapping)

	langFieldMapping := bleve.NewKeywordFieldMapping()
	langFieldMapping.IncludeInAll = false
	langFieldMapping.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("Lang", langFieldMapping)

	linksFieldMapping := bleve.NewTextFieldMapping()
	docMapping.AddFieldMappingsAt("Links", linksFieldMapping)

	pageRankFieldMapping := bleve.NewNumericFieldMapping()
	pageRankFieldMapping.Index = false
	pageRankFieldMapping.IncludeInAll = false
	pageRankFieldMapping.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("PageRank", pageRankFieldMapping)

	hostRankFieldMapping := bleve.NewNumericFieldMapping()
	hostRankFieldMapping.Index = false
	pageRankFieldMapping.IncludeInAll = false
	pageRankFieldMapping.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("HostRank", hostRankFieldMapping)

	kindFieldMapping := bleve.NewTextFieldMapping()
	kindFieldMapping.Index = true
	kindFieldMapping.IncludeInAll = false
	kindFieldMapping.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("Kind", kindFieldMapping)

	contentTypeFieldMapping := bleve.NewKeywordFieldMapping()
	contentTypeFieldMapping.Index = true
	contentTypeFieldMapping.IncludeInAll = false
	contentTypeFieldMapping.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("ContentType", contentTypeFieldMapping)

	contentSizeFieldMapping := bleve.NewNumericFieldMapping()
	contentSizeFieldMapping.Index = true
	contentSizeFieldMapping.IncludeInAll = false
	contentSizeFieldMapping.IncludeTermVectors = false
	docMapping.AddFieldMappingsAt("ContentSize", contentSizeFieldMapping)

	idxMapping.AddDocumentMapping("Page", docMapping)

	idx, err = bleve.New(path, idxMapping)
	if err != nil {
		return
	}

	idx.SetName(name)
	return
}

func OpenIndex(path string, name string) (idx bleve.Index, err error) {
	idx, err = bleve.Open(path)
	if err != nil {
		return
	}

	idx.SetName(name)
	return
}

func IndexDb(index bleve.Index, cfg *config.Config, done chan bool) (err error) {
	db, err := sql.Open("postgres", cfg.GetDbConnStr())
	if err != nil {
		return
	}
	defer db.Close()

	q := `
with x as
    (select dst_url_id uid, array_agg(text) links
     from links
     group by dst_url_id)
select u.url, c.title, c.content_text, length(c.content), c.content_type, c.lang, c.kind, x.links, u.rank, h.rank
from x
join urls u on u.id = uid
join contents c on c.id = u.content_id
join hosts h on h.hostname = u.hostname
where u.rank is not null and h.rank is not null
`

	rows, err := db.Query(q)
	if err != nil {
		return
	}
	defer rows.Close()

	n := 1
	batch := index.NewBatch()
loop:
	for rows.Next() {
		var doc Doc
		var links pq.StringArray
		var urlStr string
		var lang sql.NullString
		var kind sql.NullString
		err = rows.Scan(&urlStr, &doc.Title, &doc.Content, &doc.ContentSize, &doc.ContentType, &lang, &kind, &links, &doc.PageRank, &doc.HostRank)
		if err != nil {
			return
		}

		// in case there are pages we've fetched before adding blacklist rules
		var urlParsed *url.URL
		urlParsed, err = url.Parse(urlStr)
		if err != nil {
			log.Printf("WARNING: URL stored in db cannot be parsed: url=%s error=%s\n", urlStr, err)
		} else if gcrawler.IsBlacklisted(urlStr, urlParsed) {
			continue
		}

		doc.Lang = ""
		if lang.Valid {
			doc.Lang = lang.String
		}

		doc.Kind = ""
		if kind.Valid {
			doc.Kind = kind.String
		}

		doc.Links = strings.Join(links, "\n")

		doc.Title = strings.ToValidUTF8(doc.Title, "")

		batch.Index(urlStr, doc)
		if batch.Size() >= cfg.Index.BatchSize {
			err = index.Batch(batch)
			if err != nil {
				return
			}
			batch.Reset()
			log.Printf("Indexing progress: %d pages indexed so far.\n", n)
		}

		select {
		case <-done:
			break loop
		default:
		}

		n++
	}

	if batch.Size() > 0 {
		err = index.Batch(batch)
		if err != nil {
			return
		}
	}

	log.Printf("Finished indexing: %d pages indexed.\n", n)
	return
}

func Search(req SearchRequest, idx bleve.Index) (resp SearchResponse, err error) {
	// sanity check, in case someone sends a zero-based page index
	if req.Page < 1 {
		err = fmt.Errorf("Invalid page number (needs to be greater than or equal to 1)")
		return
	}

	shouldContent := bleve.NewMatchQuery(req.Query)
	shouldContent.SetField("Content")

	shouldTitle := bleve.NewMatchQuery(req.Query)
	shouldTitle.SetField("Title")
	shouldTitle.SetBoost(2.0)

	mustNotEmail := bleve.NewTermQuery("email")
	mustNotEmail.SetField("Kind")

	mustNotRfc := bleve.NewTermQuery("rfc")
	mustNotRfc.SetField("Kind")

	mustNotIrc := bleve.NewTermQuery("irc")
	mustNotIrc.SetField("Kind")

	q := bleve.NewBooleanQuery()
	q.AddShould(shouldContent)
	q.AddShould(shouldTitle)
	q.AddMustNot(mustNotEmail)
	q.AddMustNot(mustNotRfc)
	q.AddMustNot(mustNotIrc)

	highlightStyle := req.HighlightStyle
	if highlightStyle == "" {
		highlightStyle = "gem"
	}

	s := bleve.NewSearchRequest(q)
	s.Highlight = bleve.NewHighlightWithStyle(highlightStyle)
	s.Fields = []string{"Title", "Content", "PageRank", "HostRank", "ContentType", "ContentSize"}

	langFacet := bleve.NewFacetRequest("Lang", 3)
	s.AddFacet("lang", langFacet)

	rs := &RankedSort{
		desc:          true,
		pageRankBytes: make([]byte, 0),
		hostRankBytes: make([]byte, 0),
	}
	so := []search.SearchSort{rs}
	s.SortByCustom(so)

	s.Size = PageSize
	s.From = (req.Page - 1) * s.Size

	results, err := idx.Search(s)
	if err != nil {
		return
	}

	resp.TotalResults = results.Total
	resp.Duration = results.Took

	for _, r := range results.Hits {
		snippet := strings.Join(r.Fragments["Content"], "…")

		// this make sure snippets don't expand on many lines, and also
		// cruicially, formatted lines are not rendered in clients that do that.
		snippet = " " + strings.Replace(snippet, "\n", " ", -1)

		result := SearchResult{
			Url:         r.ID,
			Title:       r.Fields["Title"].(string),
			Snippet:     snippet,
			UrlRank:     r.Fields["PageRank"].(float64),
			HostRank:    r.Fields["HostRank"].(float64),
			Relevance:   r.Score,
			ContentType: r.Fields["ContentType"].(string),
			ContentSize: uint64(r.Fields["ContentSize"].(float64)),
		}
		resp.Results = append(resp.Results, result)
	}

	return
}

var _ search.SearchSort = (*RankedSort)(nil)
