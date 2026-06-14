package redisx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/utils"
	"github.com/redis/go-redis/v9"
)

var (
	singleProducerCache   sync.Map
	clusterProducerCache sync.Map
	producerLocks        sync.Map
)

// =====================================================================
// 单机生产者 (Single Producer)
// =====================================================================

func P(path string) (*redis.Client, error) { return PPS(path) }

func PPS(path string) (*redis.Client, error) {
	cfg, key, err := ParseFile(path)
	if err != nil { return nil, fmt.Errorf("redisx.PPS: %w", err) }
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateSingleProducer(cacheKey, cfg)
}

func POS(cfg mqx.Config) (*redis.Client, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateSingleProducer(cacheKey, &cfg)
}

// =====================================================================
// 集群生产者 (Cluster Producer)
// =====================================================================

func PPC(path string) (*redis.ClusterClient, error) {
	cfg, key, err := ParseFile(path)
	if err != nil { return nil, fmt.Errorf("redisx.PPC: %w", err) }
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateClusterProducer(cacheKey, cfg)
}

func POC(cfg mqx.Config) (*redis.ClusterClient, error) {
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateClusterProducer(cacheKey, &cfg)
}

// ... Must
func MustP(path string) *redis.Client { p, e := P(path); if e != nil { panic(e) }; return p }
func MustPPS(path string) *redis.Client { return MustP(path) }
func MustPOS(cfg mqx.Config) *redis.Client { p, e := POS(cfg); if e != nil { panic(e) }; return p }
func MustPPC(path string) *redis.ClusterClient { p, e := PPC(path); if e != nil { panic(e) }; return p }
func MustPOC(cfg mqx.Config) *redis.ClusterClient { p, e := POC(cfg); if e != nil { panic(e) }; return p }

// =====================================================================
// 核心逻辑 (Double-Checked Locking & Metrics)
// =====================================================================

// interface 供后台统计
type poolStater interface {
	PoolStats() *redis.PoolStats
	Ping(ctx context.Context) *redis.StatusCmd
}

func monitorRedisPool(client poolStater, driver, instanceID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := client.Ping(ctx).Err()
		cancel()
		if err != nil && err.Error() == "redis: client is closed" {
			return
		}

		stats := client.PoolStats()
		if stats == nil {
			continue
		}

		observability.NativeRedisPoolStats.WithLabelValues(driver, instanceID, "total_conns").Set(float64(stats.TotalConns))
		observability.NativeRedisPoolStats.WithLabelValues(driver, instanceID, "idle_conns").Set(float64(stats.IdleConns))
		observability.NativeRedisPoolStats.WithLabelValues(driver, instanceID, "stale_conns").Set(float64(stats.StaleConns))
		observability.NativeRedisPoolStats.WithLabelValues(driver, instanceID, "hits").Set(float64(stats.Hits))
		observability.NativeRedisPoolStats.WithLabelValues(driver, instanceID, "misses").Set(float64(stats.Misses))
		observability.NativeRedisPoolStats.WithLabelValues(driver, instanceID, "timeouts").Set(float64(stats.Timeouts))
	}
}

func getOrCreateSingleProducer(cacheKey string, cfg *mqx.Config) (*redis.Client, error) {
	if val, ok := singleProducerCache.Load(cacheKey); ok {
		return val.(*redis.Client), nil
	}

	if cfg.Driver != "" && cfg.Driver != "redis" {
		return nil, fmt.Errorf("redisx: driver mismatch, expected redis but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := singleProducerCache.Load(cacheKey); ok {
		return val.(*redis.Client), nil
	}

	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}
	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, err
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
		opts.MinIdleConns = cfg.Redis.MinIdleConns
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis failed: %w", err)
	}

	if cfg.Metrics.Enabled {
		go monitorRedisPool(client, "redis", cacheKey)
	}

	singleProducerCache.Store(cacheKey, client)
	return client, nil
}

func getOrCreateClusterProducer(cacheKey string, cfg *mqx.Config) (*redis.ClusterClient, error) {
	if val, ok := clusterProducerCache.Load(cacheKey); ok {
		return val.(*redis.ClusterClient), nil
	}
	if cfg.Driver != "" && cfg.Driver != "redis" {
		return nil, fmt.Errorf("redisx: driver mismatch, expected redis but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	if val, ok := clusterProducerCache.Load(cacheKey); ok {
		return val.(*redis.ClusterClient), nil
	}

	tlsConfig, _ := cfg.TLS.BuildTLS()
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
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	if cfg.Metrics.Enabled {
		go monitorRedisPool(client, "redis-cluster", cacheKey)
	}
	clusterProducerCache.Store(cacheKey, client)
	return client, nil
}