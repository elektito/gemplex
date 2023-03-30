package main

import (
	"github.com/elektito/gcrawler/pkg/gsearch"
	"github.com/elektito/gcrawler/pkg/utils"
)

func main() {
	index, err := gsearch.NewIndex("idx.bleve", "idx")
	utils.PanicOnErr(err)

	err = gsearch.IndexDb(index)
	utils.PanicOnErr(err)
}
