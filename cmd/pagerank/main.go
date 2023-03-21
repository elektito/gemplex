package main

import (
	"database/sql"
	"fmt"
	"math"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/utils"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

const (
	beta    = float64(0.85)
	epsilon = float64(0.0001)
)

type Link struct {
	src int64
	dst int64
}

func main() {
	fmt.Println("PageRank Calculator")

	db, err := sql.Open("postgres", config.GetDbConnStr())
	utils.PanicOnErr(err)

	links := make([]Link, 0)

	// map node ids to their out-degree (that is the number of nodes they link
	// to)
	outDegree := map[int64]float64{}

	// set of all nodes
	nodes := map[int64]bool{}

	fmt.Println("Reading links...")
	rows, err := db.Query("select src_url_id, dst_url_id from links")
	utils.PanicOnErr(err)
	for i := 0; rows.Next(); i++ {
		var link Link
		err = rows.Scan(&link.src, &link.dst)
		utils.PanicOnErr(err)

		links = append(links, link)

		outDegree[link.src] += 1

		nodes[link.src] = true
		nodes[link.dst] = true
	}

	// map url id to rank
	ranks := map[int64]float64{}
	newRanks := map[int64]float64{}

	// uniformly distribute 1.0 unit of rank to all nodes
	for id := range nodes {
		ranks[id] = float64(1.0) / float64(len(nodes))
	}

	diff := math.MaxFloat64
	for i := 1; diff > epsilon; i++ {
		fmt.Println("Start Iteration:", i)

		for _, link := range links {
			if link.src == link.dst { // ignore self-links
				continue
			}
			newRanks[link.dst] += beta * (ranks[link.src] / outDegree[link.src])
		}

		// We distributed 1.0 unit worth of ranks between all nodes, but some
		// nodes don't have any links and their rank would "leak". We now
		// calculate the amount of leak and distribute it uniformly between all
		// nodes. It's as if nodes with no links have a link to all other nodes.
		total := float64(0)
		for id := range nodes {
			total += newRanks[id]
		}
		leak := (1.0 - total) / float64(len(nodes))

		diff = float64(0)
		for id := range ranks {
			newRanks[id] += leak
			diff += math.Abs(ranks[id] - newRanks[id])
		}

		ranks, newRanks = newRanks, ranks
		for id := range newRanks {
			newRanks[id] = 0.0
		}

		fmt.Println("Finish Iteration:", i, " Diff:", diff)
	}

	// normalize ranks based, making the node with the highest rank a 1.0, and
	// everything else proportional to that.
	fmt.Println("Normalizing ranks...")
	max := 0.0
	for _, r := range ranks {
		if r > max {
			max = r
		}
	}

	for id := range ranks {
		ranks[id] /= max
	}

	fmt.Println("Writing ranks to database...")
	urlIds := make([]int64, len(ranks))
	urlRanks := make([]float64, len(ranks))
	i := 0
	for id, rank := range ranks {
		urlIds[i] = id
		urlRanks[i] = rank
		i++
	}
	q := `update urls
          set rank = x.rank
          from
             (select unnest($1::bigint[]) id, unnest($2::real[]) rank) x
          where urls.id = x.id`
	_, err = db.Exec(q, pq.Array(urlIds), pq.Array(urlRanks))
	utils.PanicOnErr(err)

	fmt.Println("Done.")
}
