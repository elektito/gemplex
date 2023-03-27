package gsearch

import (
	"math"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	_ "github.com/blevesearch/bleve/v2/search/highlight/highlighter/ansi"

	"github.com/elektito/gcrawler/pkg/utils"
)

type Doc struct {
	Title    string
	Content  string
	Links    string
	PageRank float64
	HostRank float64
}

type RankedSort struct {
	desc          bool
	pageRankBytes []byte
	hostRankBytes []byte
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

func NewIndex(path string) (idx bleve.Index, err error) {
	idxMapping := bleve.NewIndexMapping()

	docMapping := bleve.NewDocumentMapping()

	titleFieldMapping := bleve.NewTextFieldMapping()
	docMapping.AddFieldMappingsAt("Title", titleFieldMapping)

	contentFieldMapping := bleve.NewTextFieldMapping()
	docMapping.AddFieldMappingsAt("Content", contentFieldMapping)

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

	idxMapping.AddDocumentMapping("Page", docMapping)

	idx, err = bleve.New(path, idxMapping)
	if err != nil {
		return
	}

	return
}

func OpenIndex(path string) (idx bleve.Index, err error) {
	idx, err = bleve.Open(path)
	return
}

func Search(query string, idx bleve.Index, highlightStyle string) (results *bleve.SearchResult, err error) {
	q := bleve.NewMatchQuery(query)
	s := bleve.NewSearchRequest(q)
	s.Highlight = bleve.NewHighlightWithStyle(highlightStyle)
	s.Fields = []string{"Title", "Content", "PageRank", "HostRank"}
	ss := &RankedSort{
		desc:          true,
		pageRankBytes: make([]byte, 0),
		hostRankBytes: make([]byte, 0),
	}
	so := []search.SearchSort{ss}
	s.SortByCustom(so)
	results, err = idx.Search(s)
	return
}

var _ search.SearchSort = (*RankedSort)(nil)
