package main

import (
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/elektito/gemplex/pkg/pagerank"
	"github.com/elektito/gemplex/pkg/utils"
)

func rank(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	db, err := sql.Open("postgres", Config.GetDbConnStr())
	utils.PanicOnErr(err)
	defer db.Close()

loop:
	for {
		pagerank.PerformPageRankOnDb(db)

		select {
		case <-time.After(1 * time.Hour):
		case <-done:
			break loop
		}
	}

	log.Println("[rank] Done.")
}
