package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/utils"
	_ "github.com/lib/pq"
)

type Link struct {
	Text string
	Url  string
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <url>\n", os.Args[0])
		os.Exit(1)
	}

	inputUrl := os.Args[1]

	db, err := sql.Open("postgres", config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

	row := db.QueryRow(`
select u.id, u.rank, h.rank, c.id, c.title, c.content_type, c.content_type_args, length(c.content), length(c.content_text)
from urls u
join hosts h on h.hostname = u.hostname
join contents c on u.content_id = c.id
where url = $1
`, inputUrl)

	var uid int64
	var urank float64
	var hrank float64
	var cid sql.NullInt64
	var title string
	var ct string
	var cta string
	var clen int
	var tlen int
	err = row.Scan(&uid, &urank, &hrank, &cid, &title, &ct, &cta, &clen, &tlen)
	if err == sql.ErrNoRows {
		fmt.Println("Not found.")
		os.Exit(1)
	} else if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	fmt.Println("URL:", inputUrl)
	fmt.Printf("uid: %d  urank: %f\n", uid, urank)

	if cid.Valid {
		fmt.Printf("cid: %d  title: %s\n", cid.Int64, title)
		fmt.Printf("content-type: %s", ct)
		if cta != "" {
			fmt.Printf("  args: %s", cta)
		}
		fmt.Print("\n")
		fmt.Printf("content-length: %d  text-length: %d\n", clen, tlen)
	} else {
		fmt.Println("No content.")
	}

	u, err := url.Parse(inputUrl)
	utils.PanicOnErr(err)

	// links

	rows, err := db.Query(`
select u.url, links.text
from links
join urls u on u.id = dst_url_id
where src_url_id = $1
`, uid)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	internalLinks := make([]Link, 0)
	externalLinks := make([]Link, 0)
	for rows.Next() {
		var durl string
		var linkText string
		err = rows.Scan(&durl, &linkText)
		utils.PanicOnErr(err)

		du, err := url.Parse(durl)
		utils.PanicOnErr(err)

		if du.Hostname() == u.Hostname() {
			internalLinks = append(internalLinks, Link{Text: linkText, Url: durl})
		} else {
			externalLinks = append(externalLinks, Link{Text: linkText, Url: durl})
		}
	}

	fmt.Println()
	if len(internalLinks) == 0 {
		fmt.Println("No internal links.")
	} else {
		fmt.Printf("%d internal links:\n", len(internalLinks))
		for _, link := range internalLinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}

	fmt.Println()
	if len(externalLinks) == 0 {
		fmt.Println("No external links.")
	} else {
		fmt.Printf("%d external links:\n", len(externalLinks))
		for _, link := range externalLinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}

	// backlinks

	rows, err = db.Query(`
select u.url, links.text
from links
join urls u on u.id = src_url_id
where dst_url_id = $1
`, uid)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	internalBacklinks := make([]Link, 0)
	externalBacklinks := make([]Link, 0)
	for rows.Next() {
		var durl string
		var linkText string
		err = rows.Scan(&durl, &linkText)
		utils.PanicOnErr(err)

		du, err := url.Parse(durl)
		utils.PanicOnErr(err)

		if du.Hostname() == u.Hostname() {
			internalBacklinks = append(internalBacklinks, Link{Text: linkText, Url: durl})
		} else {
			externalBacklinks = append(externalBacklinks, Link{Text: linkText, Url: durl})
		}
	}

	fmt.Println()
	if len(internalBacklinks) == 0 {
		fmt.Println("No internal backlinks.")
	} else {
		fmt.Printf("%d internal backlinks:\n", len(internalBacklinks))
		for _, link := range internalBacklinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}

	fmt.Println()
	if len(externalBacklinks) == 0 {
		fmt.Println("No external backlinks.")
	} else {
		fmt.Printf("%d external backlinks:\n", len(externalBacklinks))
		for _, link := range externalBacklinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}
}
