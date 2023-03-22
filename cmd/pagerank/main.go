package main

import (
	"database/sql"
	"fmt"
	"math"

	"github.com/elektito/gcrawler/pkg/config"
	"github.com/elektito/gcrawler/pkg/utils"
	"github.com/lib/pq"
)

const (
	beta    = float64(0.85)
	epsilon = float64(0.0001)
)

type Link struct {
	src int64
	dst int64
}

// Calculate pagerank given a set of links. The input "links" map, maps a node
// id to another node id. As an example if links[1] == 2, then node 1 links to
// node 2.
//
// Return value is a map that maps all node ids to a rank value in [0.0, 1.0]
// range. The ranks are normalized so that the highest ranking node always has
// the rank 1.0.
func pagerank(links []Link) (ranks map[int64]float64) {
	if len(links) == 0 {
		return map[int64]float64{}
	}

	// map node ids to their out-degree (that is the number of nodes they link
	// to)
	outDegree := map[int64]float64{}

	// set of all nodes
	nodes := map[int64]bool{}

	for _, link := range links {
		outDegree[link.src] += 1

		nodes[link.src] = true
		nodes[link.dst] = true
	}

	// map url id to rank
	ranks = map[int64]float64{}
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

	return
}

func getHostRanks(urlLinks []Link, url2host map[int64]string) (hostRanks map[string]float64) {
	hostRanks = map[string]float64{}

	// we need to assign a node id to each hostname in order to be able to call
	// pagerank
	host2id := map[string]int64{}
	id2host := map[int64]string{}
	i := int64(0)
	for _, host := range url2host {
		if _, ok := host2id[host]; ok {
			// already assigned
			continue
		}
		host2id[host] = i
		id2host[i] = host
		i++
	}

	// now create a map of host links (a host linking to another host)
	hostLinks := make([]Link, 0)
	for _, link := range urlLinks {
		srcHost := url2host[link.src]
		dstHost := url2host[link.dst]
		srcHostId := host2id[srcHost]
		dstHostId := host2id[dstHost]
		hostLinks = append(hostLinks, Link{srcHostId, dstHostId})
	}

	// map the ranks back to hostnames
	ranks := pagerank(hostLinks)
	for id, rank := range ranks {
		hostname := id2host[id]
		hostRanks[hostname] = rank
	}

	return
}

func main() {
	fmt.Println("PageRank Calculator")

	db, err := sql.Open("postgres", config.GetDbConnStr())
	utils.PanicOnErr(err)

	links := make([]Link, 0)

	fmt.Println("Reading links...")
	rows, err := db.Query("select src_url_id, dst_url_id from links")
	utils.PanicOnErr(err)
	for rows.Next() {
		var link Link
		err = rows.Scan(&link.src, &link.dst)
		utils.PanicOnErr(err)

		links = append(links, link)
	}

	urlRanks := pagerank(links)

	// Now we'll normalize url ranks based on the domain ranks. To do that, we
	// first need a mapping between url ids and hostnames.
	fmt.Println("Reading hostnames...")
	rows, err = db.Query("select id, hostname from urls")
	url2host := map[int64]string{}
	for rows.Next() {
		var id int64
		var host string
		err = rows.Scan(&id, &host)
		utils.PanicOnErr(err)
		url2host[id] = host
	}

	fmt.Println("Calculating hostname ranks...")
	hostRanks := getHostRanks(links, url2host)

	fmt.Println("Normalizing url ranks based on hostname ranks...")
	maxUrlRank := float64(0)
	for id := range urlRanks {
		hostname := url2host[id]
		urlRanks[id] *= hostRanks[hostname]

		if urlRanks[id] > maxUrlRank {
			maxUrlRank = urlRanks[id]
		}
	}

	// after normalizing based on host ranks, the top url is no longer ranked
	// 1.0. So we normalize them again.
	fmt.Println("Normalizing the final results...")
	for id := range urlRanks {
		urlRanks[id] /= maxUrlRank
	}

	fmt.Println("Writing url ranks to database...")
	ids := make([]int64, len(urlRanks))
	rs := make([]float64, len(urlRanks))
	i := 0
	for id, rank := range urlRanks {
		ids[i] = id
		rs[i] = rank
		i++
	}
	q := `update urls
          set rank = x.rank
          from
             (select unnest($1::bigint[]) id, unnest($2::real[]) rank) x
          where urls.id = x.id`
	_, err = db.Exec(q, pq.Array(ids), pq.Array(rs))
	utils.PanicOnErr(err)

	fmt.Println("Writing host ranks to database...")
	hostnames := make([]string, len(hostRanks))
	rs = make([]float64, len(hostRanks))
	i = 0
	for hostname, rank := range hostRanks {
		hostnames[i] = hostname
		rs[i] = rank
		i++
	}
	q = `with hostranks as
             (select unnest($1::text[]) hostname, unnest($2::real[]) rank)
         insert into hosts (hostname, rank)
         select * from hostranks
         on conflict (hostname) do update
         set rank = excluded.rank`
	_, err = db.Exec(q, pq.Array(hostnames), pq.Array(rs))
	utils.PanicOnErr(err)

	fmt.Println("Done.")
}
