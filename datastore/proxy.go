package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	hook "github.com/alanshaw/ipfs-hookds"
	hres "github.com/alanshaw/ipfs-hookds/query/results"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	leveldb "github.com/ipfs/go-ds-leveldb"
	ipfsApi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/go-unixfs"
	caopts "github.com/ipfs/interface-go-ipfs-core/options"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/manuelwedler/hydra-booster/metrics"
	"github.com/manuelwedler/hydra-booster/utils"
	"github.com/multiformats/go-base32"
	"github.com/whyrusleeping/timecache"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// root namespace of provider keys
var providersRoot = datastore.NewKey(providers.ProvidersKeyPrefix)

// AddProviderFunc adds a provider for a given CID to the datastore
type AddProviderFunc func(ctx context.Context, c cid.Cid, id peer.ID)

// GetRoutingFunc is a function that returns an appropriate routing module given a CID
type GetRoutingFunc func(cid.Cid) (routing.Routing, AddProviderFunc, error)

// Options are options for the Hydra datastore
type Options struct {
	// total number of find provider queries we should queue
	FindProvidersQueueSize int
	// number of providers to find when a provider record does not exist in the store
	FindProvidersCount int
	// number of find provider requests we will concurrently process
	FindProvidersConcurrency int
	// maximum time a find providers call is allowed to take
	FindProvidersTimeout time.Duration
	// period after a find failure that another find request for the same key will be discarded
	FindProvidersFailureBackoff time.Duration
}

// option defaults
const (
	findProvidersQueueSize   = 1000
	findProvidersCount       = 1
	findProvidersConcurrency = 1
	findProvidersTimeout     = time.Second * 10
)

type findResult struct {
	key      datastore.Key
	status   string
	duration time.Duration
	err      error
}

// NewPrefetchProxy returns a new proxy to a datastore that adds hooks to perform hydra things
func NewPrefetchProxy(ctx context.Context, ds datastore.Batching, getRouting GetRoutingFunc, opts Options) datastore.Batching {
	if opts.FindProvidersConcurrency == 0 {
		opts.FindProvidersConcurrency = findProvidersConcurrency
	}
	if opts.FindProvidersCount == 0 {
		opts.FindProvidersCount = findProvidersCount
	}
	if opts.FindProvidersQueueSize == 0 {
		opts.FindProvidersQueueSize = findProvidersQueueSize
	}
	if opts.FindProvidersTimeout == 0 {
		opts.FindProvidersTimeout = findProvidersTimeout
	}
	if opts.FindProvidersFailureBackoff == 0 {
		opts.FindProvidersFailureBackoff = time.Minute
	}
	return hook.NewBatching(ds, hook.WithAfterQuery(newOnAfterQueryHook(ctx, getRouting, opts)))
}

func NewIpnsProxy(ctx context.Context, ds datastore.Batching, exportDs leveldb.Datastore, headId peer.ID) (datastore.Batching, error) {
	afterPut, err := newAfterPutIpnsHook(ctx, exportDs, headId)
	if err != nil {
		return nil, err
	}
	return hook.NewBatching(ds, hook.WithAfterPut(afterPut)), nil
}

func newOnAfterQueryHook(ctx context.Context, getRouting GetRoutingFunc, opts Options) hook.AfterQueryFunc {
	findC := make(chan datastore.Key, opts.FindProvidersQueueSize)
	foundC := make(chan findResult)

	pending := make(map[string]bool)
	var pendingLock sync.Mutex

	failed := timecache.NewTimeCache(opts.FindProvidersFailureBackoff)
	var failedLock sync.Mutex

	for i := 0; i < opts.FindProvidersConcurrency; i++ {
		go findProviders(ctx, findC, foundC, getRouting, opts)
	}

	go func() {
		for {
			select {
			case r := <-foundC:
				pendingLock.Lock()
				delete(pending, r.key.String())
				stats.Record(ctx, metrics.FindProvsQueueSize.M(-1))
				pendingLock.Unlock()

				if r.err != nil {
					fmt.Println(r.err)
					continue
				}

				recordFindProvsComplete(ctx, r.status, metrics.FindProvsDuration.M(float64(r.duration)))

				// if we failed to find this key, add to the failure cache so we don't try again for a bit
				if r.status == "failed" {
					failedLock.Lock()
					if !failed.Has(r.key.String()) {
						failed.Add(r.key.String())
					}
					failedLock.Unlock()
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return func(q query.Query, res query.Results, err error) (query.Results, error) {
		if err != nil {
			return res, err
		}

		k := datastore.NewKey(q.Prefix)

		// not interested if this is not a query for providers of a particular cid
		// we're looking for /providers/cid, not /providers (currently used in GC)
		if !providersRoot.IsAncestorOf(k) || len(k.Namespaces()) < 2 {
			return res, err
		}

		failedLock.Lock()
		if failed.Has(k.String()) {
			failedLock.Unlock()
			recordFindProvsComplete(ctx, "discarded")
			return res, err
		}
		failedLock.Unlock()

		var count int
		onAfterNextSync := func(r query.Result, ok bool) (query.Result, bool) {
			if ok {
				count++
				return r, ok
			}
			if count > 0 { // query has ended and there were locally found records
				recordFindProvsComplete(ctx, "local")
				return r, ok
			}

			pendingLock.Lock()
			isPending := pending[k.String()]
			if !isPending {
				// send to the find provs queue, if channel is full then discard...
				select {
				case findC <- k:
					pending[k.String()] = true
					stats.Record(ctx, metrics.FindProvsQueueSize.M(1))
				case <-ctx.Done():
				default:
					recordFindProvsComplete(ctx, "discarded")
				}
			}
			pendingLock.Unlock()

			return r, ok
		}

		return hres.NewResults(res, hres.WithAfterNextSync(onAfterNextSync)), nil
	}
}

func findProviders(ctx context.Context, findC chan datastore.Key, foundC chan findResult, getRouting GetRoutingFunc, opts Options) {
	done := func(r findResult) {
		select {
		case foundC <- r:
		case <-ctx.Done():
		}
	}

	for {
		select {
		case k := <-findC:
			cid, err := providerKeyToCID(k)
			if err != nil {
				done(findResult{key: k, err: fmt.Errorf("failed to create CID from provider record key %v: %w", k, err)})
				continue
			}

			routing, addProvider, err := getRouting(cid)
			if err != nil {
				done(findResult{key: k, err: fmt.Errorf("failed to get routing for CID: %w", err)})
				continue
			}

			status := "failed"
			start := time.Now()
			fctx, cancel := context.WithTimeout(ctx, opts.FindProvidersTimeout)

			for ai := range routing.FindProvidersAsync(fctx, cid, opts.FindProvidersCount) {
				addProvider(ctx, cid, ai.ID)
				status = "succeeded"
			}

			cancel()
			done(findResult{key: k, status: status, duration: time.Since(start) / 1e+9})
		case <-ctx.Done():
			return
		}
	}
}

var errInvalidKeyNamespaces = fmt.Errorf("not enough namespaces in provider record key")

func providerKeyToCID(k datastore.Key) (cid.Cid, error) {
	nss := k.Namespaces()
	if len(nss) < 2 {
		return cid.Undef, errInvalidKeyNamespaces
	}

	b, err := base32.RawStdEncoding.DecodeString(nss[1])
	if err != nil {
		return cid.Undef, err
	}

	_, c, err := cid.CidFromBytes(b)
	if err != nil {
		return cid.Undef, err
	}

	return c, nil
}

func recordFindProvsComplete(ctx context.Context, status string, extraMeasures ...stats.Measurement) {
	stats.RecordWithTags(
		ctx,
		[]tag.Mutator{tag.Upsert(metrics.KeyStatus, status)},
		append([]stats.Measurement{metrics.FindProvs.M(1)}, extraMeasures...)...,
	)
}

func newAfterPutIpnsHook(ctx context.Context, exportDs leveldb.Datastore, headId peer.ID) (hook.AfterPutFunc, error) {
	ipfs, err := ipfsApi.NewLocalApi()
	if err != nil {
		return nil, err
	}

	dag := ipfs.Dag()
	name := ipfs.Name()

	return func(k datastore.Key, v []byte, err error) error {
		if err != nil {
			return err
		}

		export, err := utils.NewIpnsExportEntry(k.String(), v)

		if err != nil {
			fmt.Printf("Error during ipns export: %s\n", err)
			return nil
		}

		t := time.Now()
		export.ReceiveTime = t.Format(time.RFC3339)

		if strings.HasPrefix(export.Value, "/ipfs") {
			_, cidString, err := record.SplitKey(export.Value)
			if err == nil {
				cid, err := cid.Decode(cidString)
				if err == nil {
					dagCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute))
					defer cancel()
					node, err := dag.Get(dagCtx, cid)
					if err == nil {
						fsNode, err := unixfs.FSNodeFromBytes(node.RawData())
						if err == nil {
							export.ContentType = fsNode.Type()
						} else {
							export.UnrecognizedType = true
						}
					} else {
						export.ContentUnreachable = true
					}
				}
			}
		}

		t = time.Now()
		_, err = name.Resolve(ctx, export.Name.String(), caopts.Name.ResolveOption(nsopts.DhtTimeout(nsopts.DefaultResolveOpts().DhtTimeout)))
		if err == nil {
			export.NameResolveTime = time.Now().Sub(t).Milliseconds()
		}

		export.ReceivingHead = headId

		version := 0
		var exportKey datastore.Key

		for {
			exportKey = datastore.NewKey(export.Name.String() + "/" + fmt.Sprint(version))
			hasKey, err := exportDs.Has(exportKey)
			if err != nil {
				fmt.Printf("Error during ipns export: %s\n", err)
				return nil
			}
			if !hasKey {
				break
			}
			version++
		}

		data, err := json.Marshal(export)
		if err != nil {
			fmt.Printf("Error during ipns export: %s\n", err)
			return nil
		}
		// Store it under key with and without version suffix
		exportDs.Put(exportKey, data)
		exportDs.Put(datastore.NewKey(export.Name.String()), data)

		return nil
	}, nil
}
