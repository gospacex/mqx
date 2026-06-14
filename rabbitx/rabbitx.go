package rabbitx

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Reload 执行平滑热更新。原子替换底层的连接池，旧池在后台自动销毁。
func Reload(path string) error {
	log.Printf("[rabbitx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil {
		return fmt.Errorf("reload parse error: %w", err)
	}

	// 模拟单机/集群遍历。在生产中可提供带有 Mode 的专有 Reload 方法
	cfg.Mode = "single"
	newCacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)

	// 构建新池 (这里自动更新了 activeConfigCache 并且建立新连接)
	_, err = getOrCreateProducerPool(newCacheKey, cfg)
	if err != nil {
		return fmt.Errorf("reload build new pool error: %w", err)
	}
	
	log.Printf("[rabbitx] hot-reload success. New connections are ready.")
	return nil
}

func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	// 关闭所有 Producer Pool
	producerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, pool *ConnectionPool) {
			defer wg.Done()
			log.Printf("[rabbitx] closing producer pool [key=%s]", k)
			pool.Close()
			producerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*ConnectionPool))
		return true
	})

	// 关闭所有 Consumer
	consumerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, conn *amqp.Connection) {
			defer wg.Done()
			log.Printf("[rabbitx] closing consumer connection [key=%s]", k)
			if err := conn.Close(); err != nil {
				log.Printf("[rabbitx] consumer close error [key=%s]: %v", k, err)
			}
			consumerCache.Delete(k)
			consumerLocks.Delete(k)
		}(key.(string), value.(*amqp.Connection))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[rabbitx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[rabbitx] shutdown timed out, some connections may not be fully closed")
	}
}

func HealthCheck() map[string]string {
	result := make(map[string]string)
	producerCache.Range(func(key, value any) bool {
		pool := value.(*ConnectionPool)
		if pool.instances[0].IsClosed() {
			result[key.(string)] = "unhealthy: primary connection closed"
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})
	return result
}
