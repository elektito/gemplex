package main

import (
	"log"
	"net"
	"os"
	"sync"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/utils"
)

func search(done chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	defer cleanupUnixSocket()

	loadIndexOnce.Do(func() { loadInitialIndex(done) })

	listener, err := net.Listen("unix", config.Config.Search.UnixSocketPath)
	utils.PanicOnErr(err)

	closing := false
	go func() {
		closing = true
		<-done
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
	if err != nil {
		log.Println("[search] Error cleaning up unix socket:", err)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
}
