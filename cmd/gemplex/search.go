package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/gsearch"
	"github.com/elektito/gcrawler/pkg/utils"
)

type Request struct {
	Query string `json:"q"`
	Page  int    `json:"p"`
}

type SearchResult struct {
	Url       string
	Title     string
	Snippet   string
	UrlRank   float64
	HostRank  float64
	Relevance float64
}

type Response struct {
	TotalResults uint64         `json:"n"`
	Results      []SearchResult `json:"r"`
	Duration     time.Duration  `json:"d"`
}

func search(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	loadIndexOnce.Do(func() { loadInitialIndex(done) })

	cleanupUnixSocket()
	listener, err := net.Listen("unix", config.Config.Search.UnixSocketPath)
	utils.PanicOnErr(err)

	closing := false
	go func() {
		<-done
		closing = true
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if closing {
			break
		}
		utils.PanicOnErr(err)

		go handleConn(conn)
	}

	log.Println("[search] Done.")
}

func cleanupUnixSocket() {
	err := os.Remove(config.Config.Search.UnixSocketPath)
	if err != nil && !os.IsNotExist(err) {
		log.Println("[search] Error cleaning up unix socket:", err)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	ok := scanner.Scan()
	if !ok {
		log.Println("Scanner error:", scanner.Err())
		return
	}

	var req Request
	err := json.Unmarshal(scanner.Bytes(), &req)
	if err != nil {
		conn.Write(errorResponse("bad request"))
		return
	}

	if req.Query == "" {
		conn.Write(errorResponse("no query"))
		return
	}

	rr, err := gsearch.Search(req.Query, idx, "gem", req.Page)
	if err != nil {
		conn.Write(errorResponse(err.Error()))
		return
	}

	results := make([]SearchResult, 0)
	for _, r := range rr.Hits {
		snippet := strings.Join(r.Fragments["Content"], "")

		// this make sure snippets don't expand on many lines, and also
		// cruicially, formatted lines are not rendered in clients that do that.
		snippet = " " + strings.Replace(snippet, "\n", "…", -1)

		result := SearchResult{
			Url:       r.ID,
			Title:     r.Fields["Title"].(string),
			Snippet:   snippet,
			UrlRank:   r.Fields["PageRank"].(float64),
			HostRank:  r.Fields["HostRank"].(float64),
			Relevance: r.Score,
		}
		results = append(results, result)
	}

	resp := Response{
		TotalResults: rr.Total,
		Results:      results,
	}
	err = json.NewEncoder(conn).Encode(resp)
	if err != nil {
		conn.Write(errorResponse(fmt.Sprintf("Error marshalling results: %s", err)))
	}
}

func errorResponse(msg string) (resp []byte) {
	type errorJson struct {
		Err string `json:"err"`
	}
	v := errorJson{
		Err: msg,
	}
	resp, err := json.Marshal(v)
	utils.PanicOnErr(err)
	resp = append(resp, '\r', '\n')
	return
}
