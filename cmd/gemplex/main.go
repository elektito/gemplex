package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func usage() {
	fmt.Printf(`Gemplex Search Engine

usage: %s all | <commands>

<commands> can be one or more of these commands, separated by spaces. If "all"
is used, all daemons are launched.

 - crawl: Start the crawler daemon. The crawler routinely crawls the geminispace
   and stores the results in the database.

 - rank: Start the periodic pagerank calculator damon.

 - index: Start the periodic ping-pong indexer daemon. It builds, alternatingly,
   an index named "ping" or "pong".

 - search: Start the search daemon, which opens the latest index (either ping or
   pong), and listens for search requests over a unix domain socket.

`, os.Args[0])
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var cmds []string
	allCmds := []string{"crawl", "rank", "index", "search"}

	if len(os.Args) == 1 {
		cmds = allCmds
	} else if os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "-?" {
		usage()
		os.Exit(0)
	} else if os.Args[1] == "all" {
		cmds = allCmds
	} else {
		cmds = os.Args[1:]
	}

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
