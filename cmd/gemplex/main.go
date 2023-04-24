package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"git.sr.ht/~elektito/gemplex/pkg/config"
	"git.sr.ht/~elektito/gemplex/pkg/gcrawler"
	"git.sr.ht/~elektito/gemplex/pkg/utils"
)

var Config *config.Config
var CrawlerStateFile *string
var Db *sql.DB

func main() {
	configFile := flag.String("config", "", "config file")
	CrawlerStateFile = flag.String(
		"dump-crawler-state",
		"",
		"Dump crawler state on shutdown to the given filename (by default state will not be dumped).",
	)
	flag.Usage = usage
	flag.Parse()

	Config = config.LoadConfig(*configFile)

	// open (and check) database for all workers to use
	var err error
	Db, err = sql.Open("postgres", Config.GetDbConnStr())
	utils.PanicOnErr(err)
	err = Db.Ping()
	utils.PanicOnErr(err)

	updateBlacklist()

	var cmds []string
	allCmds := []string{"crawl", "rank", "index", "search"}

	if len(flag.Args()) == 0 {
		cmds = allCmds
	} else if flag.Arg(0) == "all" {
		cmds = allCmds
	} else {
		cmds = flag.Args()
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	seen := map[string]bool{}
	funcs := []func(chan bool, *sync.WaitGroup){}
	for _, cmd := range cmds {
		if _, ok := seen[cmd]; ok {
			fmt.Println("Duplicate command:", cmd)
			os.Exit(1)
		}

		switch cmd {
		case "crawl":
			funcs = append(funcs, crawl)
		case "rank":
			funcs = append(funcs, rank)
		case "index":
			funcs = append(funcs, index)
		case "search":
			funcs = append(funcs, search)
		default:
			fmt.Println("Unrecognized command:", cmd)
			os.Exit(1)
		}
	}

	// setup signal handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	var done []chan bool

	done = make([]chan bool, len(funcs))
	for i := range done {
		done[i] = make(chan bool)
	}

	wg.Add(len(funcs))

	for i, f := range funcs {
		go f(done[i], &wg)
	}

	<-sigs

	// stop receiving signals, so user can stop the program by sending another
	// signal (in case the finalization process is taking too long).
	signal.Stop(sigs)

	log.Println("[gemplex] Received signal.")

	for _, c := range done {
		go func(c chan bool) { c <- true }(c)
	}

	log.Println("[gemplex] Waiting for daemons to stop...")
	wg.Wait()

	log.Println("[gemplex] Done.")
}

func usage() {
	fmt.Printf(`Gemplex Search Engine

usage: %s [flags] { all | <commands> }

The following flags are available:

-config <filename>

    config_file is the name of the toml configuration file to load. If not
    specified, one of the following files (if present) is used, in order of
    preference: %s

-dump-crawler-state <filename>

    Dump crawler state to a file with the given name. Could be useful for
    debugging. By default, state will not be dumped.

<commands> can be one or more of these commands, separated by spaces. If "all"
is used, all daemons are launched.

 - crawl: Start the crawler daemon. The crawler routinely crawls the geminispace
   and stores the results in the database.

 - rank: Start the periodic pagerank calculator damon.

 - index: Start the periodic ping-pong indexer daemon. It builds, alternatingly,
   an index named "ping" or "pong".

 - search: Start the search daemon, which opens the latest index (either ping or
   pong), and listens for search requests over a unix domain socket.

`, os.Args[0], strings.Join(config.DefaultConfigFiles, ", "))
}

func updateBlacklist() {
	for _, domain := range Config.Blacklist.Domains {
		gcrawler.AddDomainToBlacklist(domain)
	}

	for _, prefix := range Config.Blacklist.Prefixes {
		gcrawler.AddPrefixToBlacklist(prefix)
	}
}
