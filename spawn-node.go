package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	ds "github.com/ipfs/go-datastore"
	ipns "github.com/ipfs/go-ipns"
	libp2p "github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	host "github.com/libp2p/go-libp2p-core/host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dhtopts "github.com/libp2p/go-libp2p-kad-dht/opts"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	record "github.com/libp2p/go-libp2p-record"
)

func bootstrapperAddrs() pstore.PeerInfo {
	addr := dht.DefaultBootstrapPeers[rand.Intn(len(dht.DefaultBootstrapPeers))]
	ai, err := pstore.InfoFromP2pAddr(addr)
	if err != nil {
		panic(err)
	}

	return *ai
}

var bootstrapDone int64

// MakeAndStartNode ...
func MakeAndStartNode(ds ds.Batching, addr string, relay bool, bucketSize int, limiter chan struct{}) (host.Host, *dht.IpfsDHT, error) {
	cmgr := connmgr.NewConnManager(1500, 2000, time.Minute)

	priv, _, _ := crypto.GenerateKeyPair(crypto.Ed25519, 0)

	opts := []libp2p.Option{libp2p.ListenAddrStrings(addr), libp2p.ConnectionManager(cmgr), libp2p.Identity(priv)}
	if relay {
		opts = append(opts, libp2p.EnableRelay(circuit.OptHop))
	}

	node, err := libp2p.New(context.Background(), opts...)
	if err != nil {
		panic(err)
	}

	dhtNode, err := dht.New(context.Background(), node, dhtopts.BucketSize(bucketSize), dhtopts.Datastore(ds), dhtopts.Validator(record.NamespacedValidator{
		"pk":   record.PublicKeyValidator{},
		"ipns": ipns.Validator{KeyBook: node.Peerstore()},
	}))
	if err != nil {
		panic(err)
	}

	// bootstrap in the background
	// it's safe to start doing this _before_ establishing any connections
	// as we'll trigger a boostrap round as soon as we get a connection anyways.
	dhtNode.Bootstrap(context.Background())

	go func() {
		// ❓ what is this limiter for?
		if limiter != nil {
			limiter <- struct{}{}
		}

		// ❓ tries to connect to bootstrappers 2x, why?
		for i := 0; i < 2; i++ {
			if err := node.Connect(context.Background(), bootstrapperAddrs()); err != nil {
				fmt.Println("bootstrap connect failed: ", err)
				i--
			}
		}

		if limiter != nil {
			<-limiter
		}
		atomic.AddInt64(&bootstrapDone, 1)

	}()
	return node, dhtNode, nil
}