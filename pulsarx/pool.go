package pulsarx

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx"
)

// ProducerPool Pulsar 生产者连接池（Round-Robin）
type ProducerPool struct {
	instances []pulsar.Producer
	counter   uint64
	size      int
}

func newProducerPool(size int, cacheKey string, cfg *mqx.Config, client pulsar.Client) (*ProducerPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ProducerPool{
		instances: make([]pulsar.Producer, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		opts := pulsar.ProducerOptions{
			Topic: cfg.Producer.Topic,
		}

		if cfg.Producer.Timeout > 0 {
			opts.SendTimeout = cfg.Producer.Timeout
		}
		if cfg.Producer.BatchSize > 0 {
			opts.BatchingMaxMessages = uint(cfg.Producer.BatchSize)
		}
		if cfg.Producer.LingerMs > 0 {
			opts.BatchingMaxPublishDelay = cfg.Producer.LingerMs * 1000000 // ns
		}

		if cfg.Pulsar != nil {
			if cfg.Pulsar.MaxPendingMessages > 0 {
				opts.MaxPendingMessages = cfg.Pulsar.MaxPendingMessages
			}
			if cfg.Pulsar.EnableBatching {
				opts.DisableBatching = false
			} else {
				opts.DisableBatching = true
			}
		}

		p, err := client.CreateProducer(opts)
		if err != nil {
			// 初始化失败，回收已创建的实例
			for j := 0; j < i; j++ {
				pool.instances[j].Close()
			}
			return nil, mqx.PoolError("pulsar", fmt.Sprintf("create pool instance %d", i), err)
		}
		pool.instances[i] = p
	}

	return pool, nil
}

// Get O(1) 获取一个生产者实例
func (p *ProducerPool) Get() pulsar.Producer {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅关闭池中所有生产者
func (p *ProducerPool) Close() {
	for _, inst := range p.instances {
		inst.Close()
	}
}

// ConsumerPool Pulsar 消费者连接池（Round-Robin）
type ConsumerPool struct {
	instances []pulsar.Consumer
	counter   uint64
	size      int
}

func newConsumerPool(size int, cacheKey string, cfg *mqx.Config, client pulsar.Client) (*ConsumerPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ConsumerPool{
		instances: make([]pulsar.Consumer, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		if cfg.Consumer.Group == "" {
			return nil, mqx.ConfigError("consumer group (subscription name) is required for pulsar", nil)
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

		c, err := client.Subscribe(opts)
		if err != nil {
			// 初始化失败，回收已创建的实例
			for j := 0; j < i; j++ {
				pool.instances[j].Close()
			}
			return nil, mqx.PoolError("pulsar", fmt.Sprintf("create pool instance %d", i), err)
		}
		pool.instances[i] = c
	}

	return pool, nil
}

// Get O(1) 获取一个消费者实例
func (c *ConsumerPool) Get() pulsar.Consumer {
	if c.size == 1 {
		return c.instances[0]
	}
	idx := atomic.AddUint64(&c.counter, 1)
	return c.instances[idx%uint64(c.size)]
}

// Close 优雅关闭池中所有消费者
func (c *ConsumerPool) Close() {
	for _, inst := range c.instances {
		inst.Close()
	}
}
