package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/elektito/gemplex/pkg/config"
	"github.com/elektito/gemplex/pkg/gsearch"
	"github.com/elektito/gemplex/pkg/utils"
)

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

	log.Println("Request:", scanner.Text())

	var req gsearch.SearchRequest
	req.Page = 1
	err := json.Unmarshal(scanner.Bytes(), &req)
	if err != nil {
		conn.Write(errorResponse("bad request"))
		return
	}

	if req.Query == "" {
		conn.Write(errorResponse("no query"))
		return
	}

	resp, err := gsearch.Search(req, idx)
	if err != nil {
		conn.Write(errorResponse(err.Error()))
		return
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
