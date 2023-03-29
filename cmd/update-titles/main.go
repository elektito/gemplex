package main

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/gparse"
	"github.com/elektito/gcrawler/pkg/utils"
	_ "github.com/lib/pq"
)

func main() {
	// this program re-parses all the contents in the database, checks if the
	// title has changes, and if so, saves the new titles to the database again.
	// This is useful, if our parsing algorithms change and we want to apply it
	// to existing pages.

	db, err := sql.Open("postgres", config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

	rows, err := db.Query(`
select c.id, content, title, content_type, u.url
from contents c
join urls u on u.content_id=c.id
`)
	utils.PanicOnErr(err)
	defer rows.Close()

	changes := map[int64]string{}
	for rows.Next() {
		var id int64
		var blob []byte
		var oldTitle string
		var us string
		var contentType string
		rows.Scan(&id, &blob, &oldTitle, &contentType, &us)

		u, _ := url.Parse(us)
		rr, err := gparse.ParsePage(blob, u, contentType)
		if err != nil {
			continue
		}

		if rr.Title != oldTitle {
			fmt.Printf("'%s' => '%s'  %s  %d\n", oldTitle, rr.Title, u.String(), id)
			changes[id] = rr.Title
		}
	}

	fmt.Printf("---- applying %d changes ----\n", len(changes))
	i := 0
	for id, newTitle := range changes {
		_, err := db.Exec(`update contents set title = $1 where id = $2`, newTitle, id)
		utils.PanicOnErr(err)

		i++
		if i%100 == 0 {
			fmt.Printf("Progress: %d/%d\n", i, len(changes))
		}
	}

	fmt.Printf("---- done: %d changes ----\n", len(changes))
}
