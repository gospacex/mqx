package pulsarx

import (
	"fmt"
	"log"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	consumerCache sync.Map // key → pulsar.Consumer
	consumerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 消费者 (Single / Cluster) -> 返回 pulsar.Consumer 接口
// =====================================================================

func C(path string) (pulsar.Consumer, error) { return CPS(path) }

func CPS(path string) (pulsar.Consumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("pulsarx.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (pulsar.Consumer, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustC(path string) pulsar.Consumer { return MustCPS(path) }

func MustCPS(path string) pulsar.Consumer {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) pulsar.Consumer {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustCOS failure: %w", err))
	}
	return c
}

func CPC(path string) (pulsar.Consumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("pulsarx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (pulsar.Consumer, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustCPC(path string) pulsar.Consumer {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) pulsar.Consumer {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("pulsarx MustCOC failure: %w", err))
	}
	return c
}

func getOrCreateConsumer(cacheKey string, cfg *mqx.Config) (pulsar.Consumer, error) {
	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(pulsar.Consumer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "pulsar" {
		return nil, fmt.Errorf("pulsarx: driver mismatch, expected pulsar but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(pulsar.Consumer), nil
	}

	client, clientKey, err := getOrCreateClient(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Consumer.Group == "" {
		return nil, fmt.Errorf("consumer group (subscription name) is required for pulsar")
	}

	opts := pulsar.ConsumerOptions{
		Topics:           cfg.Consumer.Topics,
		SubscriptionName: cfg.Consumer.Group,
	}

	if cfg.Pulsar != nil {
		if cfg.Pulsar.ReceiverQueueSize > 0 {
			opts.ReceiverQueueSize = cfg.Pulsar.ReceiverQueueSize
		}
		if cfg.Pulsar.NackRedeliveryDelay > 0 {
			opts.NackRedeliveryDelay = cfg.Pulsar.NackRedeliveryDelay
		}
		
		switch cfg.Pulsar.SubscriptionType {
		case "Shared":
			opts.Type = pulsar.Shared
		case "Failover":
			opts.Type = pulsar.Failover
		case "KeyShared":
			opts.Type = pulsar.KeyShared
		default:
			opts.Type = pulsar.Exclusive
		}

		// DLQ 配置
		if cfg.DLQ.Enabled && cfg.Pulsar.DeadLetterTopic != "" {
			maxRetry := cfg.Pulsar.DeadLetterMaxRetry
			if maxRetry == 0 {
				maxRetry = uint32(cfg.DLQ.MaxRetries)
			}
			opts.RetryEnable = true
			opts.DLQ = &pulsar.DLQPolicy{
				MaxDeliveries:   maxRetry,
				DeadLetterTopic: cfg.Pulsar.DeadLetterTopic,
			}
		}
	}

	consumer, err := client.Subscribe(opts)
	if err != nil {
		return nil, fmt.Errorf("create pulsar consumer: %w", err)
	}

	consumerCache.Store(cacheKey, consumer)
	log.Printf("[pulsarx] consumer ready [client_key=%s, topics=%v, sub=%s]", clientKey, cfg.Consumer.Topics, cfg.Consumer.Group)

	return consumer, nil
}
