package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/db"
	"github.com/elektito/gcrawler/pkg/gparse"
	"github.com/elektito/gcrawler/pkg/utils"
	_ "github.com/lib/pq"
)

type Command struct {
	Info       string
	ShortUsage string
	Handler    func([]string)
}

var commands map[string]Command

func init() {
	commands = map[string]Command{
		"update-titles": {
			Info:       "Re-parse all pages in db, re-calculate the title, and write it back to db.",
			ShortUsage: "",
			Handler:    handleUpdateTitlesCommand,
		},
		"url": {
			Info:       "Display information about the given url",
			ShortUsage: "[-substr] <url>",
			Handler:    handleUrlInfoCommand,
		},
	}
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

func handleUpdateTitlesCommand(args []string) {
	// this sub-command re-parses all the contents in the database, checks if the
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
	i := 0
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

		i++
		if i%1000 == 0 {
			fmt.Println("Progress:", i)
		}
	}

	fmt.Printf("---- applying %d changes ----\n", len(changes))
	i = 0
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
