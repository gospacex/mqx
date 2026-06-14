package kafkax

import (
	"fmt"
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	consumerCache sync.Map // key → *kafka.Consumer
	consumerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 单机消费者 (Single Consumer)
// =====================================================================

func C(path string) (*kafka.Consumer, error) { return CPS(path) }

func CPS(path string) (*kafka.Consumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafkax.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (*kafka.Consumer, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustC(path string) *kafka.Consumer { return MustCPS(path) }

func MustCPS(path string) *kafka.Consumer {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("kafkax MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) *kafka.Consumer {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("kafkax MustCOS failure: %w", err))
	}
	return c
}

// =====================================================================
// 集群消费者 (Cluster Consumer)
// =====================================================================

func CPC(path string) (*kafka.Consumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafkax.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (*kafka.Consumer, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustCPC(path string) *kafka.Consumer {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("kafkax MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) *kafka.Consumer {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("kafkax MustCOC failure: %w", err))
	}
	return c
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// =====================================================================

func getOrCreateConsumer(cacheKey string, cfg *mqx.Config) (*kafka.Consumer, error) {
	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(*kafka.Consumer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "kafka" {
		return nil, fmt.Errorf("kafkax: driver mismatch, expected kafka but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(*kafka.Consumer), nil
	}

	cm, err := buildConsumerConfigMap(cfg)
	if err != nil {
		return nil, fmt.Errorf("build consumer config: %w", err)
	}

	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	if len(cfg.Consumer.Topics) > 0 {
		if err := consumer.SubscribeTopics(cfg.Consumer.Topics, nil); err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("subscribe topics: %w", err)
		}
	}

	consumerCache.Store(cacheKey, consumer)
	log.Printf("[kafkax] consumer ready [key=%s, topics=%v]", cacheKey, cfg.Consumer.Topics)

	return consumer, nil
}
