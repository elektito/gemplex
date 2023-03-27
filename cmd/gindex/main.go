package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/gsearch"
	"github.com/elektito/gcrawler/pkg/utils"
	"github.com/lib/pq"
)

func main() {
	index, err := gsearch.NewIndex("idx.bleve")
	utils.PanicOnErr(err)

	dbConnStr := config.GetDbConnStr()
	db, err := sql.Open("postgres", dbConnStr)
	utils.PanicOnErr(err)
	defer db.Close()

	q := `
with x as
    (select dst_url_id uid, array_agg(text) links
     from links
     group by dst_url_id)
select u.url, c.title, c.content, x.links, u.rank, h.rank
from x
join urls u on u.id = uid
join contents c on c.id = u.content_id
join hosts h on h.hostname = u.hostname
`

	rows, err := db.Query(q)
	utils.PanicOnErr(err)
	defer rows.Close()

	n := 1
	batch := index.NewBatch()
	for rows.Next() {
		fmt.Println(n)

		var doc gsearch.Doc
		var links pq.StringArray
		var url string
		rows.Scan(&url, &doc.Title, &doc.Content, &links, &doc.PageRank, &doc.HostRank)
		doc.Links = strings.Join(links, "\n")

		batch.Index(url, doc)
		if batch.Size() >= 10000 {
			index.Batch(batch)
			batch.Reset()
		}

		n++
	}

	if batch.Size() > 0 {
		index.Batch(batch)
	}
}
