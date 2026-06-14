package natsx

import (
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	consumerCache sync.Map // key → *nats.Conn 或 nats.JetStreamContext
	consumerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 消费者 (Single / Cluster) -> 返回 any
// =====================================================================

func C(path string) (any, error) { return CPS(path) }

func CPS(path string) (any, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("natsx.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (any, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustC(path string) any { return MustCPS(path) }

func MustCPS(path string) any {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("natsx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) any {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("natsx MustCOS failure: %w", err))
	}
	return c
}

func CPC(path string) (any, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("natsx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (any, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustCPC(path string) any {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("natsx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) any {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("natsx MustCOC failure: %w", err))
	}
	return c
}

func getOrCreateConsumer(cacheKey string, cfg *mqx.Config) (any, error) {
	if val, ok := consumerCache.Load(cacheKey); ok {
		return val, nil
	}

	if cfg.Driver != "" && cfg.Driver != "nats" {
		return nil, fmt.Errorf("natsx: driver mismatch, expected nats but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := consumerCache.Load(cacheKey); ok {
		return val, nil
	}

	// 消费者的底层也是复用 NATS 连接
	nc, clientKey, err := getOrCreateConn(cfg)
	if err != nil {
		return nil, err
	}

	var result any

	if cfg.NATS != nil && cfg.NATS.JetStream {
		js, err := nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("init jetstream context for consumer: %w", err)
		}
		result = js
		log.Printf("[natsx] consumer ready (JetStream Enabled) [client_key=%s]", clientKey)
	} else {
		result = nc
		log.Printf("[natsx] consumer ready (Core NATS) [client_key=%s]", clientKey)
	}

	consumerCache.Store(cacheKey, result)
	return result, nil
}
