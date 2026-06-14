package rabbitx

import (
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	producerCache sync.Map // key → *ConnectionPool
	producerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 生产者快捷函数 (通过 pool.Get 返回原生指针)
// =====================================================================

func P(path string) (*amqp.Connection, error) { return PPS(path) }

func PPS(path string) (*amqp.Connection, error) {
	cfg, key, err := ParseFile(path)
	if err != nil { return nil, fmt.Errorf("rabbitx.PPS: %w", err) }
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	pool, err := getOrCreateProducerPool(cacheKey, cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func POS(cfg mqx.Config) (*amqp.Connection, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	pool, err := getOrCreateProducerPool(cacheKey, &cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func PPC(path string) (*amqp.Connection, error) {
	cfg, key, err := ParseFile(path)
	if err != nil { return nil, fmt.Errorf("rabbitx.PPC: %w", err) }
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	pool, err := getOrCreateProducerPool(cacheKey, cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func POC(cfg mqx.Config) (*amqp.Connection, error) {
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + utils.ConfigFingerprint(&cfg)
	pool, err := getOrCreateProducerPool(cacheKey, &cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

// ... Must 系列
func MustP(path string) *amqp.Connection { p, e := P(path); if e!=nil{panic(e)}; return p }
func MustPPS(path string) *amqp.Connection { return MustP(path) }
func MustPOS(cfg mqx.Config) *amqp.Connection { p, e := POS(cfg); if e!=nil{panic(e)}; return p }
func MustPPC(path string) *amqp.Connection { p, e := PPC(path); if e!=nil{panic(e)}; return p }
func MustPOC(cfg mqx.Config) *amqp.Connection { p, e := POC(cfg); if e!=nil{panic(e)}; return p }

// =====================================================================
// 核心逻辑 (Pool)
// =====================================================================

func getOrCreateProducerPool(cacheKey string, cfg *mqx.Config) (*ConnectionPool, error) {
	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*ConnectionPool), nil
	}

	if cfg.Driver != "" && cfg.Driver != "rabbitmq" {
		return nil, fmt.Errorf("rabbitx: driver mismatch, expected rabbitmq but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*ConnectionPool), nil
	}

	size := 1
	if cfg.InstancePoolSize > 1 { size = cfg.InstancePoolSize }

	pool, err := newConnectionPool(size, cfg, cacheKey)
	if err != nil {
		return nil, fmt.Errorf("create rabbitmq pool: %w", err)
	}

	// 挂载监控: 只监控 0 号连接即可代表网络连通性
	go func() {
		errChan := pool.instances[0].NotifyClose(make(chan *amqp.Error))
		if e := <-errChan; e != nil {
			log.Printf("[rabbitx] primary connection closed unexpectedly [key=%s]: %v", cacheKey, e)
			// 注意：如果是单例被外部强杀，删掉缓存，让下次获取能重建。
			if val, ok := producerCache.Load(cacheKey); ok && val == pool { producerCache.Delete(cacheKey) }
		}
	}()

	producerCache.Store(cacheKey, pool)
	log.Printf("[rabbitx] producer pool ready [key=%s, size=%d]", cacheKey, size)

	return pool, nil
}
