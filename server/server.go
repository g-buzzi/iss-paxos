package main

import (
	"flag"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/abd"
	"github.com/ailidani/paxi/blockchain"
	"github.com/ailidani/paxi/chain"
	"github.com/ailidani/paxi/dynamo"
	"github.com/ailidani/paxi/epaxos"
	"github.com/ailidani/paxi/hpaxos"
	"github.com/ailidani/paxi/iss2"
	"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/m2paxos"
	"github.com/ailidani/paxi/paxos"
	"github.com/ailidani/paxi/paxos_group"
	"github.com/ailidani/paxi/sdpaxos"
	"github.com/ailidani/paxi/vpaxos"
	"github.com/ailidani/paxi/wankeeper"
	"github.com/ailidani/paxi/wpaxos"
)

var algorithm = flag.String("algorithm", "paxos", "Distributed algorithm")
var id = flag.String("id", "", "ID in format of Zone.Node.")
var simulation = flag.Bool("sim", false, "simulation mode")

var master = flag.String("master", "", "Master address.")

var numBuckets = flag.Int("buckets", 2, "Number of buckets for ISS.")
var numSegments = flag.Int("segments", 2, "Number of segments for ISS.")
var segmentSize = flag.Int("segSize", 8, "Size of segments for ISS.")
var batchTimeout = flag.Int("batch", 2, "Time in ms to wait for requests.")
var heartbeat = flag.Int("heartbeat", 100, "Time in ms to wait for activity before trying to become leader.")

func replica(id paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false, id)
	}

	log.Infof("node %v starting...", id)

	switch *algorithm {

	case "paxos":
		paxos.NewReplica(id).Run()

	case "epaxos":
		epaxos.NewReplica(id).Run()

	case "sdpaxos":
		sdpaxos.NewReplica(id).Run()

	case "wpaxos":
		wpaxos.NewReplica(id).Run()

	case "abd":
		abd.NewReplica(id).Run()

	case "chain":
		chain.NewReplica(id).Run()

	case "vpaxos":
		vpaxos.NewReplica(id).Run()

	case "wankeeper":
		wankeeper.NewReplica(id).Run()

	case "kpaxos":
		kpaxos.NewReplica(id).Run()

	case "paxos_groups":
		paxos_group.NewReplica(id).Run()

	case "dynamo":
		dynamo.NewReplica(id).Run()

	case "blockchain":
		blockchain.NewMiner(id).Run()

	case "m2paxos":
		m2paxos.NewReplica(id).Run()

	case "hpaxos":
		hpaxos.NewReplica(id).Run()

	case "iss":
		iss2.NewReplica(id, *numBuckets, *numSegments, *segmentSize, *batchTimeout, *heartbeat).Run()
	default:
		panic("Unknown algorithm")
	}
}

func main() {
	paxi.Init()

	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		paxi.Simulation()
		for id := range paxi.GetConfig().Addrs {
			n := id
			go replica(n)
		}
		wg.Wait()
	} else {
		replica(paxi.ID(*id))
	}
}
