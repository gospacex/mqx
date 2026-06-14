package pulsarx

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	producerCache sync.Map // key → pulsar.Producer
	producerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 生产者 (Single / Cluster) -> 返回 pulsar.Producer 接口
// =====================================================================

func P(path string) (pulsar.Producer, error) { return PPS(path) }

func PPS(path string) (pulsar.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("pulsarx.PPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateProducer(cacheKey, cfg)
}

func POS(cfg mqx.Config) (pulsar.Producer, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateProducer(cacheKey, &cfg)
}

func MustP(path string) pulsar.Producer { return MustPPS(path) }

func MustPPS(path string) pulsar.Producer {
	p, err := PPS(path)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustPPS failure: %w", err))
	}
	return p
}

func MustPOS(cfg mqx.Config) pulsar.Producer {
	p, err := POS(cfg)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustPOS failure: %w", err))
	}
	return p
}

func PPC(path string) (pulsar.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("pulsarx.PPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateProducer(cacheKey, cfg)
}

func POC(cfg mqx.Config) (pulsar.Producer, error) {
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateProducer(cacheKey, &cfg)
}

func MustPPC(path string) pulsar.Producer {
	p, err := PPC(path)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustPPC failure: %w", err))
	}
	return p
}

func MustPOC(cfg mqx.Config) pulsar.Producer {
	p, err := POC(cfg)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustPOC failure: %w", err))
	}
	return p
}

func getOrCreateProducer(cacheKey string, cfg *mqx.Config) (pulsar.Producer, error) {
	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(pulsar.Producer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "pulsar" {
		return nil, fmt.Errorf("pulsarx: driver mismatch, expected pulsar but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(pulsar.Producer), nil
	}

	client, clientKey, err := getOrCreateClient(cfg)
	if err != nil {
		return nil, err
	}

	opts := pulsar.ProducerOptions{
		Topic: cfg.Producer.Topic,
	}

	if cfg.Producer.Timeout > 0 {
		opts.SendTimeout = cfg.Producer.Timeout
	}
	if cfg.Producer.BatchSize > 0 {
		opts.BatchingMaxMessages = uint(cfg.Producer.BatchSize)
	}
	if cfg.Producer.LingerMs > 0 {
		opts.BatchingMaxPublishDelay = time.Duration(cfg.Producer.LingerMs) * time.Millisecond
	}

	if cfg.Pulsar != nil {
		if cfg.Pulsar.MaxPendingMessages > 0 {
			opts.MaxPendingMessages = cfg.Pulsar.MaxPendingMessages
		}
		if cfg.Pulsar.EnableBatching {
			opts.DisableBatching = false
		} else {
			opts.DisableBatching = true // 默认 SDK 是开启的，这里根据用户配置灵活反转
		}
	}

	producer, err := client.CreateProducer(opts)
	if err != nil {
		return nil, fmt.Errorf("create pulsar producer: %w", err)
	}

	producerCache.Store(cacheKey, producer)
	log.Printf("[pulsarx] producer ready [client_key=%s, topic=%s]", clientKey, cfg.Producer.Topic)

	return producer, nil
}
