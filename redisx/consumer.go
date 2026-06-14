package redisx

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	"github.com/redis/go-redis/v9"
)

var (
	singleConsumerCache   sync.Map // key → *redis.Client
	clusterConsumerCache  sync.Map // key → *redis.ClusterClient
	consumerLocks         sync.Map // key → *sync.Mutex
)

// =====================================================================
// 单机消费者 (Single Consumer)
// =====================================================================

func C(path string) (*redis.Client, error) { return CPS(path) }

func CPS(path string) (*redis.Client, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("redisx.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateSingleConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (*redis.Client, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateSingleConsumer(cacheKey, &cfg)
}

func MustC(path string) *redis.Client { return MustCPS(path) }

func MustCPS(path string) *redis.Client {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("redisx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) *redis.Client {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("redisx MustCOS failure: %w", err))
	}
	return c
}

// =====================================================================
// 集群消费者 (Cluster Consumer)
// =====================================================================

func CPC(path string) (*redis.ClusterClient, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("redisx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateClusterConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (*redis.ClusterClient, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateClusterConsumer(cacheKey, &cfg)
}

func MustCPC(path string) *redis.ClusterClient {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("redisx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) *redis.ClusterClient {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("redisx MustCOC failure: %w", err))
	}
	return c
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// 针对 Consumer，我们可以通过发送 XGROUP CREATE 命令提前创建消费者组
// =====================================================================

func getOrCreateSingleConsumer(cacheKey string, cfg *mqx.Config) (*redis.Client, error) {
	if val, ok := singleConsumerCache.Load(cacheKey); ok {
		return val.(*redis.Client), nil
	}

	if cfg.Driver != "" && cfg.Driver != "redis" {
		return nil, fmt.Errorf("redisx: driver mismatch, expected redis but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := singleConsumerCache.Load(cacheKey); ok {
		return val.(*redis.Client), nil
	}

	// 这里的初始化参数与 Producer 相同
	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	opts := &redis.Options{
		Addr:      cfg.Addrs[0],
		Username:  cfg.Auth.Username, // Redis 6+ ACL
		Password:  cfg.Auth.Password,
		TLSConfig: tlsConfig,
	}

	if cfg.Redis != nil {
		opts.DB = cfg.Redis.DB
		opts.PoolSize = cfg.Redis.PoolSize
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis failed: %w", err)
	}

	// 提前声明 Stream 消费者组 (自动创建 Stream)
	if cfg.Consumer.Group != "" && len(cfg.Consumer.Topics) > 0 {
		for _, topic := range cfg.Consumer.Topics {
			err := client.XGroupCreateMkStream(ctx, topic, cfg.Consumer.Group, "0").Err()
			if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
				log.Printf("[redisx] warning: failed to create consumer group for topic %s: %v", topic, err)
			}
		}
	}

	singleConsumerCache.Store(cacheKey, client)
	log.Printf("[redisx] single consumer ready [key=%s]", cacheKey)

	return client, nil
}

func getOrCreateClusterConsumer(cacheKey string, cfg *mqx.Config) (*redis.ClusterClient, error) {
	if val, ok := clusterConsumerCache.Load(cacheKey); ok {
		return val.(*redis.ClusterClient), nil
	}

	if cfg.Driver != "" && cfg.Driver != "redis" {
		return nil, fmt.Errorf("redisx: driver mismatch, expected redis but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := clusterConsumerCache.Load(cacheKey); ok {
		return val.(*redis.ClusterClient), nil
	}

	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	opts := &redis.ClusterOptions{
		Addrs:     cfg.Addrs,
		Username:  cfg.Auth.Username, // Redis 6+ ACL
		Password:  cfg.Auth.Password,
		TLSConfig: tlsConfig,
	}
	if cfg.Redis != nil {
		opts.PoolSize = cfg.Redis.PoolSize
	}

	client := redis.NewClusterClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis cluster failed: %w", err)
	}

	if cfg.Consumer.Group != "" && len(cfg.Consumer.Topics) > 0 {
		for _, topic := range cfg.Consumer.Topics {
			err := client.XGroupCreateMkStream(ctx, topic, cfg.Consumer.Group, "0").Err()
			if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
				log.Printf("[redisx] warning: failed to create consumer group for topic %s: %v", topic, err)
			}
		}
	}

	clusterConsumerCache.Store(cacheKey, client)
	log.Printf("[redisx] cluster consumer ready [key=%s]", cacheKey)

	return client, nil
}
