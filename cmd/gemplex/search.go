package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"git.sr.ht/~elektito/gemplex/pkg/gsearch"
	"git.sr.ht/~elektito/gemplex/pkg/utils"
)

type TypedRequest struct {
	Type string `json:"t"`
}

func search(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	loadIndexOnce.Do(func() { loadInitialIndex(done) })

	cleanupUnixSocket()
	listener, err := net.Listen("unix", Config.Search.UnixSocketPath)
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
	err := os.Remove(Config.Search.UnixSocketPath)
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

	reqLine := scanner.Bytes()
	log.Println("Request:", scanner.Text())

	var req TypedRequest
	req.Type = "search"
	err := json.Unmarshal(reqLine, &req)
	if err != nil {
		conn.Write([]byte("bad request"))
		return
	}

	var resp []byte
	switch req.Type {
	case "search":
		resp = handleSearchRequest(reqLine)
	case "randimg":
		resp = handleRandImgRequest(reqLine)
	case "getimg":
		resp = handleGetImgRequest(reqLine)
	default:
		resp = errorResponse("unknown request type")
		return
	}

	resp = append(resp, byte('\n'))
	conn.Write(resp)
}

func handleSearchRequest(reqLine []byte) []byte {
	var req gsearch.SearchRequest
	req.Page = 1
	err := json.Unmarshal(reqLine, &req)
	if err != nil {
		return errorResponse("bad request")
	}

	if req.Query == "" {
		return errorResponse("no query")
	}

	resp, err := gsearch.Search(req, idx)
	if err != nil {
		return errorResponse(err.Error())
	}

	jsonResp, err := json.Marshal(resp)
	if err != nil {
		return errorResponse(fmt.Sprintf("Error marshalling results: %s", err))
	}

	return jsonResp
}

func handleRandImgRequest(reqLine []byte) []byte {
	var resp struct {
		Url       string    `json:"url"`
		Alt       string    `json:"alt"`
		Image     string    `json:"image"`
		FetchTime time.Time `json:"fetch_time"`
		ImageId   string    `json:"image_id"`
	}

	row := Db.QueryRow(`
select * from
	(select url, alt, image_hash, image, fetch_time from images tablesample bernoulli(1)) s
order by random() limit 1;
`)
	err := row.Scan(&resp.Url, &resp.Alt, &resp.ImageId, &resp.Image, &resp.FetchTime)
	if err != nil {
		return errorResponse(fmt.Sprintf("Database error: %s", err))
	}

	jsonResp, err := json.Marshal(resp)
	if err != nil {
		return errorResponse(fmt.Sprintf("Error marshalling results: %s", err))
	}

	return jsonResp
}

func handleGetImgRequest(reqLine []byte) []byte {
	var req struct {
		Id string `json:"id"`
	}

	var resp struct {
		Url       string    `json:"url"`
		Alt       string    `json:"alt"`
		Image     string    `json:"image"`
		FetchTime time.Time `json:"fetch_time"`
		ImageId   string    `json:"image_id"`
	}

	err := json.Unmarshal(reqLine, &req)
	if err != nil {
		return errorResponse("bad request")
	}

	row := Db.QueryRow(`select url, alt, image_hash, image, fetch_time from images where image_hash = $1`, req.Id)
	err = row.Scan(&resp.Url, &resp.Alt, &resp.ImageId, &resp.Image, &resp.FetchTime)
	if err != nil {
		return errorResponse(fmt.Sprintf("Database error: %s", err))
	}

	jsonResp, err := json.Marshal(resp)
	if err != nil {
		return errorResponse(fmt.Sprintf("Error marshalling results: %s", err))
	}

	return jsonResp
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
