package natsx

import (
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	"github.com/nats-io/nats.go"
)

var (
	producerCache sync.Map // key → *NatsPool
	producerLocks sync.Map
)

func P(path string) (any, error) { return PPS(path) }
func PPS(path string) (any, error) {
	cfg, key, err := ParseFile(path)
	if err != nil { return nil, fmt.Errorf("natsx.PPS: %w", err) }
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	pool, err := getOrCreateProducerPool(cacheKey, cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func POS(cfg mqx.Config) (any, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	pool, err := getOrCreateProducerPool(cacheKey, &cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func PPC(path string) (any, error) {
	cfg, key, err := ParseFile(path)
	if err != nil { return nil, fmt.Errorf("natsx.PPC: %w", err) }
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	pool, err := getOrCreateProducerPool(cacheKey, cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func POC(cfg mqx.Config) (any, error) {
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + utils.ConfigFingerprint(&cfg)
	pool, err := getOrCreateProducerPool(cacheKey, &cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func MustP(path string) any { p, e := P(path); if e!=nil{panic(e)}; return p }
func MustPPS(path string) any { return MustP(path) }
func MustPOS(cfg mqx.Config) any { p, e := POS(cfg); if e!=nil{panic(e)}; return p }
func MustPPC(path string) any { p, e := PPC(path); if e!=nil{panic(e)}; return p }
func MustPOC(cfg mqx.Config) any { p, e := POC(cfg); if e!=nil{panic(e)}; return p }

func getOrCreateProducerPool(cacheKey string, cfg *mqx.Config) (*NatsPool, error) {
	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*NatsPool), nil
	}

	if cfg.Driver != "" && cfg.Driver != "nats" {
		return nil, fmt.Errorf("natsx: driver mismatch, expected nats but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*NatsPool), nil
	}

	size := 1
	if cfg.InstancePoolSize > 1 { size = cfg.InstancePoolSize }

	pool, err := newNatsPool(size, cfg, cacheKey)
	if err != nil {
		return nil, fmt.Errorf("create nats pool: %w", err)
	}

	// 自动声明持久化 Stream 拓扑
	if cfg.NATS != nil && cfg.NATS.JetStream && cfg.NATS.StreamName != "" && len(cfg.NATS.StreamSubjects) > 0 {
		js := pool.instances[0].(nats.JetStreamContext)
		streamCfg := &nats.StreamConfig{
			Name:     cfg.NATS.StreamName,
			Subjects: cfg.NATS.StreamSubjects,
		}
		if cfg.NATS.StreamStorage == "memory" { streamCfg.Storage = nats.MemoryStorage } else { streamCfg.Storage = nats.FileStorage }
		if cfg.NATS.StreamReplicas > 0 { streamCfg.Replicas = cfg.NATS.StreamReplicas }

		_, err = js.AddStream(streamCfg)
		if err != nil && err != nats.ErrStreamNameAlreadyInUse {
			log.Printf("[natsx] warning: create jetstream %s failed: %v", cfg.NATS.StreamName, err)
		}
	}

	producerCache.Store(cacheKey, pool)
	log.Printf("[natsx] producer pool ready [key=%s, size=%d]", cacheKey, size)
	return pool, nil
}
