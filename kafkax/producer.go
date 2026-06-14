package kafkax

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/utils"
)

var (
	producerCache sync.Map // key → *ProducerPool (【重磅升级】这里存的是连接池对象，而不是单个实例)
	producerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 单机/集群 生产者快捷函数 (通过 pool.Get 返回原生指针)
// =====================================================================

func P(path string) (*kafka.Producer, error) { return PPS(path) }

func PPS(path string) (*kafka.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafkax.PPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	pool, err := getOrCreateProducerPool(cacheKey, cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func POS(cfg mqx.Config) (*kafka.Producer, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	pool, err := getOrCreateProducerPool(cacheKey, &cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

func PPC(path string) (*kafka.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafkax.PPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	pool, err := getOrCreateProducerPool(cacheKey, cfg)
	if err != nil { return nil, err }
	return pool.Get(), nil
}

// 强硬 API (Must 系列)
func MustP(path string) *kafka.Producer {
	p, err := P(path)
	if err != nil { panic(err) }
	return p
}
func MustPPS(path string) *kafka.Producer { return MustP(path) }
func MustPPC(path string) *kafka.Producer {
	p, err := PPC(path)
	if err != nil { panic(err) }
	return p
}

// =====================================================================
// 核心：基于池的多路复用初始化
// =====================================================================

func getOrCreateProducerPool(cacheKey string, cfg *mqx.Config) (*ProducerPool, error) {
	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*ProducerPool), nil
	}

	if cfg.Driver != "" && cfg.Driver != "kafka" {
		return nil, fmt.Errorf("kafkax: driver mismatch, expected kafka but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*ProducerPool), nil
	}

	cm, err := buildProducerConfigMap(cfg)
	if err != nil {
		return nil, fmt.Errorf("build config map: %w", err)
	}

	// 初始化连接池
	size := 1
	if cfg.InstancePoolSize > 1 {
		size = cfg.InstancePoolSize
	}

	pool, err := newProducerPool(size, cfg, cm, cacheKey)
	if err != nil {
		return nil, fmt.Errorf("create producer pool: %w", err)
	}

	producerCache.Store(cacheKey, pool)
	log.Printf("[kafkax] producer pool ready [key=%s, size=%d]", cacheKey, size)

	return pool, nil
}

// =====================================================================
// 核心：深度 Native Stats 指标暴漏
// =====================================================================

func handleProducerEvents(p *kafka.Producer, cfg *mqx.Config, instanceID string) {
	metricsEnabled := cfg.Metrics.Enabled

	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			topic := "unknown"
			if ev.TopicPartition.Topic != nil { topic = *ev.TopicPartition.Topic }

			if ev.TopicPartition.Error != nil {
				if metricsEnabled { observability.ProduceErrorsTotal.WithLabelValues("kafka", topic, instanceID).Inc() }
				log.Printf("[kafkax] msg delivery failed (topic=%s): %v", topic, ev.TopicPartition.Error)
			} else {
				if metricsEnabled { observability.ProduceSuccessTotal.WithLabelValues("kafka", topic, instanceID).Inc() }
			}

		case *kafka.Stats:
			// 【重磅功能】解析底层的 C 核心 JSON 指标！
			if metricsEnabled {
				var stats map[string]interface{}
				if err := json.Unmarshal([]byte(ev.String()), &stats); err == nil {
					// 取出队列长度 (msg_cnt) 和 在途请求数 (txmsgs)
					if msgCnt, ok := stats["msg_cnt"].(float64); ok {
						observability.NativeQueueLength.WithLabelValues("kafka", instanceID).Set(msgCnt)
					}
					// 这让 SRE 能够第一时间看到底层 Socket 层面的积压状况
				}
			}

		case kafka.Error:
			log.Printf("[kafkax] producer core error: %v", ev)
		}
	}
}
