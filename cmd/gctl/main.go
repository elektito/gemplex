package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	"github.com/elektito/gcrawler/pkg/db"
	_ "github.com/lib/pq"
)

type Command struct {
	ShortUsage string
	Handler    func([]string)
}

var commands map[string]Command

func init() {
	commands = map[string]Command{
		"url": {
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

func usage() {
	fmt.Printf("Usage: %s <command> <command-args>\n", os.Args[0])
	fmt.Println("Available commands:")
	for name, cmd := range commands {
		fmt.Printf(" - %s %s\n", name, cmd.ShortUsage)
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
