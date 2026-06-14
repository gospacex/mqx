package redisx

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gospacex/mqx/utils"
	"github.com/redis/go-redis/v9"
)

// Reload 执行平滑热更新。原子替换底层的 go-redis 实例，旧池在后台自动销毁。
func Reload(path string) error {
	log.Printf("[redisx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	// 根据 mode 决定重载目标
	if cfg.Mode == "cluster" {
		newCacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
		_, err = getOrCreateClusterProducer(newCacheKey, cfg)
	} else {
		cfg.Mode = "single"
		newCacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
		_, err = getOrCreateSingleProducer(newCacheKey, cfg)
	}

	if err != nil { return fmt.Errorf("reload build new pool error: %w", err) }
	
	log.Printf("[redisx] hot-reload success. New connections are ready.")
	return nil
}

// Shutdown 优雅关闭所有 redisx 管理的连接
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	// 关闭所有单机生产者
	singleProducerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, client *redis.Client) {
			defer wg.Done()
			log.Printf("[redisx] closing single producer [key=%s]", k)
			_ = client.Close()
			singleProducerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*redis.Client))
		return true
	})

	// 关闭所有集群生产者
	clusterProducerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, client *redis.ClusterClient) {
			defer wg.Done()
			log.Printf("[redisx] closing cluster producer [key=%s]", k)
			_ = client.Close()
			clusterProducerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*redis.ClusterClient))
		return true
	})

	// ... 省略 consumer 层的重复代码，原理相同

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[redisx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[redisx] shutdown timed out, some connections may not be fully closed")
	}
}

// HealthCheck 返回当前节点所持有的所有 Redis 连接的健康状态
func HealthCheck() map[string]string {
	result := make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	singleProducerCache.Range(func(key, value any) bool {
		client := value.(*redis.Client)
		if err := client.Ping(ctx).Err(); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else { result[key.(string)] = "healthy" }
		return true
	})

	clusterProducerCache.Range(func(key, value any) bool {
		client := value.(*redis.ClusterClient)
		if err := client.Ping(ctx).Err(); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else { result[key.(string)] = "healthy" }
		return true
	})

	return result
}
