package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/db"
	"github.com/elektito/gcrawler/pkg/gparse"
	"github.com/elektito/gcrawler/pkg/gsearch"
	"github.com/elektito/gcrawler/pkg/pagerank"
	"github.com/elektito/gcrawler/pkg/utils"
	"github.com/lib/pq"
)

type Command struct {
	Info       string
	ShortUsage string
	Handler    func([]string)
}

var commands map[string]Command

func init() {
	commands = map[string]Command{
		"index": {
			Info:       "Index the contents of the database",
			ShortUsage: "<index-dir>",
			Handler:    handleIndexCommand,
		},
		"pagerank": {
			Info:       "Update pageranks in the database.",
			ShortUsage: "",
			Handler:    handlePageRankCommand,
		},
		"reparse": {
			Info:       "Re-parse all pages in db, re-calculate columns we get from parsing, and write the results back to db.",
			ShortUsage: "",
			Handler:    handleReparseCommand,
		},
		"url": {
			Info:       "Display information about the given url",
			ShortUsage: "[-substr] <url>",
			Handler:    handleUrlInfoCommand,
		},
	}
}

func handleIndexCommand(args []string) {
	if len(args) != 1 {
		usage()
		os.Exit(1)
	}

	indexDir := args[0]

	var indexName string
	_, filename := path.Split(indexDir)
	if strings.HasSuffix(filename, ".idx") {
		indexName = filename[:len(filename)-4]
	} else {
		indexName = filename
	}

	index, err := gsearch.NewIndex(indexDir, indexName)
	utils.PanicOnErr(err)

	err = gsearch.IndexDb(index)
	utils.PanicOnErr(err)
}

func handlePageRankCommand(args []string) {
	pagerank.PerformPageRankOnDb()
}

func handleUrlInfoCommand(args []string) {
	fs := flag.NewFlagSet("url", flag.ExitOnError)

	substr := fs.Bool("substr", false, "Search for the given substring in urls; first will be picked.")

	fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
		os.Exit(1)
	}

	inputUrl := fs.Arg(0)
	info, err := db.QueryUrl(inputUrl, *substr)
	if err == sql.ErrNoRows {
		fmt.Println("Not found.")
		os.Exit(1)
	} else if err != nil {
		panic(err)
	}

	fmt.Println("URL:", info.Url)
	fmt.Printf("uid: %d  urank: %f  hrank: %f\n", info.UrlId, info.UrlRank, info.HostRank)

	if info.ContentId >= 0 {
		fmt.Printf("cid: %d  title: %s\n", info.ContentId, info.ContentTitle)
		fmt.Printf("content-type: %s", info.ContentType)
		if info.ContentTypeArgs != "" {
			fmt.Printf("  args: %s", info.ContentTypeArgs)
		}
		fmt.Print("\n")
		fmt.Printf("lang: %s  kind:  %s\n", info.ContentLang, info.ContentKind)
		fmt.Printf("content-length: %d  text-length: %d\n", len(info.Contents), len(info.ContentsText))
	} else {
		fmt.Println("No content.")
	}

	fmt.Println()
	if len(info.InternalLinks) == 0 {
		fmt.Println("No internal links.")
	} else {
		fmt.Printf("%d internal links:\n", len(info.InternalLinks))
		for _, link := range info.InternalLinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}

	fmt.Println()
	if len(info.ExternalLinks) == 0 {
		fmt.Println("No external links.")
	} else {
		fmt.Printf("%d external links:\n", len(info.ExternalLinks))
		for _, link := range info.ExternalLinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}

	fmt.Println()
	if len(info.InternalBacklinks) == 0 {
		fmt.Println("No internal backlinks.")
	} else {
		fmt.Printf("%d internal backlinks:\n", len(info.InternalBacklinks))
		for _, link := range info.InternalBacklinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}

	fmt.Println()
	if len(info.ExternalBacklinks) == 0 {
		fmt.Println("No external backlinks.")
	} else {
		fmt.Printf("%d external backlinks:\n", len(info.ExternalBacklinks))
		for _, link := range info.ExternalBacklinks {
			if link.Text == "" {
				fmt.Printf(" - %s\n", link.Url)
			} else {
				fmt.Printf(" - \"%s\"\n   %s\n", link.Text, link.Url)
			}
		}
	}
}

func handleReparseCommand(args []string) {
	// this sub-command re-parses all the contents in the database, checks if the
	// title has changes, and if so, saves the new titles to the database again.
	// This is useful, if our parsing algorithms change and we want to apply it
	// to existing pages.

	db, err := sql.Open("postgres", config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

	rows, err := db.Query(`
select c.id, content, title, content_type, lang, kind, u.url
from contents c
join urls u on u.content_id=c.id
`)
	utils.PanicOnErr(err)
	defer rows.Close()

	changedTitles := map[int64]string{}
	changedKinds := map[int64]string{}
	changedLangs := map[int64]string{}
	i := 0
	for rows.Next() {
		var id int64
		var blob []byte
		var oldTitle string
		var oldKind string
		var oldLang string
		var oldKindNull sql.NullString
		var oldLangNull sql.NullString
		var us string
		var contentType string
		err = rows.Scan(&id, &blob, &oldTitle, &contentType, &oldLangNull, &oldKindNull, &us)
		utils.PanicOnErr(err)

		if oldLangNull.Valid {
			oldLang = oldLangNull.String
		} else {
			oldLang = ""
		}

		if oldKindNull.Valid {
			oldKind = oldKindNull.String
		} else {
			oldKind = ""
		}

		u, _ := url.Parse(us)
		rr, err := gparse.ParsePage(blob, u, contentType)
		if err != nil {
			continue
		}

		if rr.Title != oldTitle {
			fmt.Printf("Title change: '%s' => '%s'  url=%s  cid=%d\n", oldTitle, rr.Title, u.String(), id)
			changedTitles[id] = rr.Title
		}

		if rr.Kind != oldKind {
			fmt.Printf("Kind change: '%s' => '%s'  url=%s  cid=%d\n", oldKind, rr.Kind, u.String(), id)
			changedKinds[id] = rr.Kind
		}

		if rr.Lang != oldLang {
			fmt.Printf("Lang change: '%s' => '%s'  url=%s  cid=%d\n", oldLang, rr.Lang, u.String(), id)
			changedLangs[id] = rr.Lang
		}

		i++
		if i%1000 == 0 {
			fmt.Println("Progress:", i)
		}
	}

	fmt.Printf("---- applying %d changed titles ----\n", len(changedTitles))
	ids := make([]int64, 0)
	values := make([]string, 0)
	for id, value := range changedTitles {
		ids = append(ids, id)
		values = append(values, value)
	}
	q := `
update contents
set title = x.title
from
    (select unnest($1::bigint[]) id, unnest($2::text[]) title) x
where contents.id = x.id
`
	_, err = db.Exec(q, pq.Array(ids), pq.Array(values))
	utils.PanicOnErr(err)

	fmt.Printf("---- applying %d changed kinds ----\n", len(changedKinds))
	ids = make([]int64, 0)
	values = make([]string, 0)
	for id, value := range changedKinds {
		ids = append(ids, id)
		values = append(values, value)
	}
	q = `
update contents
set kind = x.kind
from
    (select unnest($1::bigint[]) id, unnest($2::text[]) kind) x
where contents.id = x.id
`
	_, err = db.Exec(q, pq.Array(ids), pq.Array(values))
	utils.PanicOnErr(err)

	fmt.Printf("---- applying %d changed langs ----\n", len(changedLangs))
	ids = make([]int64, 0)
	values = make([]string, 0)
	for id, value := range changedLangs {
		ids = append(ids, id)
		values = append(values, value)
	}
	q = `
update contents
set lang = x.lang
from
    (select unnest($1::bigint[]) id, unnest($2::text[]) lang) x
where contents.id = x.id
`
	_, err = db.Exec(q, pq.Array(ids), pq.Array(values))
	utils.PanicOnErr(err)

	fmt.Printf("Done.")
}

func usage() {
	fmt.Printf("Usage: %s <command> <command-args>\n", os.Args[0])
	fmt.Println("Available commands:")
	for name, cmd := range commands {
		fmt.Printf(" - %s %s\n", name, cmd.ShortUsage)
		fmt.Printf("   %s\n", cmd.Info)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		fmt.Println("Unknown command:", cmd)
		usage()
		os.Exit(1)
	}

	cmd.Handler(os.Args[2:])
}
