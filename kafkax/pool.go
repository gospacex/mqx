package kafkax

import (
	"fmt"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx"
)

// ProducerPool 实现了 Round-Robin 的多路复用连接池
type ProducerPool struct {
	instances []*kafka.Producer
	counter   uint64
	size      int
}

func newProducerPool(size int, cfg *mqx.Config, cm *kafka.ConfigMap, cacheKey string) (*ProducerPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ProducerPool{
		instances: make([]*kafka.Producer, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		p, err := kafka.NewProducer(cm)
		if err != nil {
			// 初始化失败，需将已创建的实例回收
			for j := 0; j < i; j++ {
				pool.instances[j].Close()
			}
			return nil, fmt.Errorf("create pool instance %d: %w", i, err)
		}
		pool.instances[i] = p
		
		// 启动每个实例的后台事件拦截 (含 Native Stats)
		go handleProducerEvents(p, cfg, fmt.Sprintf("%s-pool-%d", cacheKey, i))
	}

	return pool, nil
}

// Get O(1) 获取一个原生指针
func (p *ProducerPool) Get() *kafka.Producer {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅回收池中所有指针
func (p *ProducerPool) Close(flushTimeoutMs int) {
	for _, inst := range p.instances {
		inst.Flush(flushTimeoutMs)
		inst.Close()
	}
}
