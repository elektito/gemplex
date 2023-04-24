package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"git.sr.ht/~elektito/gemplex/pkg/gsearch"
	"git.sr.ht/~elektito/gemplex/pkg/utils"
	"github.com/blevesearch/bleve/v2"
)

// used to make sure loadInitialIndex, which is called by both search and index
// daemons, is run only once.
var loadIndexOnce sync.Once

// this is an index alias which is used for searching (if the search daemon is
// running). the actual index in use will be swapped transprently be the index
// daemon periodically.
var idx bleve.IndexAlias

// the current index added to the index alias. this is only used by the index
// daemon.
var curIdx bleve.Index

func index(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	ctx, cancelFunc := context.WithCancel(context.Background())
	loadIndexOnce.Do(func() { loadInitialIndex(ctx) })

	loopDone := make(chan bool)
	go func() {
		<-done
		cancelFunc()
		loopDone <- true
	}()

loop:
	for {
		indexDb(ctx)

		select {
		case <-time.After(1 * time.Hour):
		case <-loopDone:
			break loop
		}
	}

	log.Println("[index] Done.")
}

func loadInitialIndex(ctx context.Context) {
	pingFile := path.Join(Config.Index.Path, "ping.idx")
	pongFile := path.Join(Config.Index.Path, "pong.idx")

	idx = bleve.NewIndexAlias()

	_, err := os.Stat(pingFile)
	pingExists := (err == nil)

	_, err = os.Stat(pongFile)
	pongExists := (err == nil)

	err = nil

	if pingExists && pongExists {
		log.Println("[index] Both ping and pong exist; checking...")
		pingIdx, pingErr := gsearch.OpenIndex(pingFile, "ping")
		pongIdx, pongErr := gsearch.OpenIndex(pongFile, "pong")
		if pingErr == nil && pongErr != nil {
			log.Println("[index] Going with ping because there was an error opening pong.")
			curIdx = pingIdx
			idx.Add(pingIdx)
			return
		} else if pongErr == nil && pingErr != nil {
			log.Println("[index] Going with pong because there was an error opening ping.")
			curIdx = pongIdx
			idx.Add(pongIdx)
			return
		} else if pingErr != nil && pongErr != nil {
			err = fmt.Errorf("Could not open either index file:\nping: %v\npong: %v", pingErr, pongErr)
			panic(err)
		}

		pingCount, pingErr := pingIdx.DocCount()
		pongCount, pongErr := pongIdx.DocCount()
		if pingErr == nil && pongErr != nil {
			log.Println("[index] Going with ping because there was an error reading pong.")
			curIdx = pingIdx
			idx.Add(pingIdx)
			return
		} else if pongErr == nil && pingErr != nil {
			log.Println("[index] Going with pong because there was an error reading ping.")
			curIdx = pongIdx
			idx.Add(pongIdx)
			return
		} else if pingErr != nil && pongErr != nil {
			err = fmt.Errorf("[index] Could not read either index file:\nping: %v\npong: %v", pingErr, pongErr)
			panic(err)
		}

		if pingCount > pongCount {
			log.Printf(
				"[index] Choosing ping index since it has more documents (%d) than pong (%d).\n",
				pingCount, pongCount)
			curIdx = pingIdx
			idx.Add(pingIdx)
		} else {
			log.Printf(
				"[index] Choosing pong index since it has more documents (%d) than ping (%d).\n",
				pongCount, pingCount)
			curIdx = pongIdx
			idx.Add(pongIdx)
		}
	} else if pingExists {
		curIdx, err = gsearch.OpenIndex(pingFile, "ping")
		utils.PanicOnErr(err)
		idx.Add(curIdx)
		log.Println("[index] Opened ping index.")
	} else if pongExists {
		curIdx, err = gsearch.OpenIndex(pongFile, "pong")
		utils.PanicOnErr(err)
		idx.Add(curIdx)
		log.Println("[index] Opened pong index.")
	} else {
		log.Println("[index] No index available. Creating ping index...")

		curIdx, err = gsearch.NewIndex(pingFile, "ping")
		utils.PanicOnErr(err)

		err = gsearch.IndexDb(ctx, curIdx, Config)
		if ctx.Err() == context.Canceled {
			return
		}
		utils.PanicOnErr(err)

		idx.Add(curIdx)
	}
}

func indexDb(ctx context.Context) {
	pingFile := path.Join(Config.Index.Path, "ping.idx")
	pongFile := path.Join(Config.Index.Path, "pong.idx")

	var newIdxFile string
	var newIdxName string

	if curIdx.Name() == "ping" {
		newIdxFile = pongFile
		newIdxName = "pong"
	} else {
		newIdxFile = pingFile
		newIdxName = "ping"
	}

	err := os.RemoveAll(newIdxFile)
	utils.PanicOnErr(err)

	log.Println("Creating new index:", newIdxFile)
	newIdx, err := gsearch.NewIndex(newIdxFile, newIdxName)
	utils.PanicOnErr(err)

	err = gsearch.IndexDb(ctx, newIdx, Config)
	if ctx.Err() == context.Canceled {
		return
	}
	utils.PanicOnErr(err)

	idx.Swap([]bleve.Index{newIdx}, []bleve.Index{curIdx})
	log.Println("Swapped in new index:", newIdxFile)

	curIdx = newIdx
}
