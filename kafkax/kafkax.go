package kafkax

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

// Reload 执行平滑热更新。重新读取配置文件并原子替换底层的连接池。
// 老的连接池会被扔入后台，等待 flushTimeout 后安全销毁，确保在途消息不丢失。

func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	// 关闭所有 Producer Pool
	producerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, pool *ProducerPool) {
			defer wg.Done()
			log.Printf("[kafkax] flushing and closing producer pool [key=%s]", k)
			
			// 池化后，每个底层实例都要排空
			flushTimeoutMs := int(flushTimeout(ctx).Milliseconds())
			pool.Close(flushTimeoutMs)

			producerCache.Delete(k)
			producerLocks.Delete(k)
			log.Printf("[kafkax] producer pool closed [key=%s]", k)
		}(key.(string), value.(*ProducerPool))
		return true
	})

	// 关闭所有 Consumer
	consumerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, c *kafka.Consumer) {
			defer wg.Done()
			log.Printf("[kafkax] closing consumer [key=%s]", k)
			if err := c.Close(); err != nil {
				log.Printf("[kafkax] consumer close error [key=%s]: %v", k, err)
			}
			consumerCache.Delete(k)
			consumerLocks.Delete(k)
			log.Printf("[kafkax] consumer closed [key=%s]", k)
		}(key.(string), value.(*kafka.Consumer))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[kafkax] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[kafkax] shutdown timed out, some connections may not be fully closed")
	}
}

func flushTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok { return 15 * time.Second }
	remaining := time.Until(deadline)
	if remaining < time.Second { return time.Second }
	return time.Duration(float64(remaining) * 0.8)
}

func HealthCheck() map[string]string {
	result := make(map[string]string)
	
	// 在引入 Pool 之后，我们可以探活池子里的一号机
	producerCache.Range(func(key, value any) bool {
		pool := value.(*ProducerPool)
		_, err := pool.instances[0].GetMetadata(nil, false, 5000)
		if err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})

	// ... 省略 consumer 探测
	return result
}

func Reload(path string) error {
	log.Printf("[kafkax] hot-reloading config from %s ...", path)

	// 获取旧配置以计算旧缓存 Key
	var oldCacheKey string
	if val, ok := activeConfigCache.Load(path); ok {
		oldCfg := val.(*mqx.Config)
		oldCfg.Mode = "single" // 仅示例
		_, oldKey := splitPath(path)
		if oldKey == "" { oldKey = "default" }
		oldCacheKey = "producer:single:" + oldKey + ":" + utils.ConfigFingerprint(oldCfg)
	}

	cfg, key, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	cfg.Mode = "single"
	newCacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	newPool, err := getOrCreateProducerPool(newCacheKey, cfg)
	if err != nil { return fmt.Errorf("reload build new pool error: %w", err) }

	// 延迟关闭旧连接池 (防止在途消息丢失与内存泄漏)
	if oldCacheKey != "" && oldCacheKey != newCacheKey {
		if val, ok := producerCache.Load(oldCacheKey); ok {
			oldPool := val.(*ProducerPool)
			go func() {
				time.Sleep(5 * time.Second)
				log.Printf("[kafkax] hot-reload: executing delayed close on old pool [key=%s]", oldCacheKey)
				oldPool.Close(5000)
				producerCache.Delete(oldCacheKey)
				producerLocks.Delete(oldCacheKey)
			}()
		}
	}
	
	_ = newPool
	log.Printf("[kafkax] hot-reload success. New connections are ready.")
	return nil
}
