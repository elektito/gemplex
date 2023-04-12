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

	"github.com/elektito/gemplex/pkg/config"
)

func main() {
	var cmds []string
	allCmds := []string{"crawl", "rank", "index", "search"}

	if len(config.Config.Args) == 0 {
		cmds = allCmds
	} else if config.Config.Args[0] == "all" {
		cmds = allCmds
	} else {
		cmds = config.Config.Args
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
