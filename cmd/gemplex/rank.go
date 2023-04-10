package main

import (
	"log"
	"sync"
	"time"

	"github.com/elektito/gcrawler/pkg/pagerank"
)

func rank(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

loop:
	for {
		pagerank.PerformPageRankOnDb()

		select {
		case <-time.After(1 * time.Hour):
		case <-done:
			break loop
		}
	}

	log.Println("[rank] Done.")
}
