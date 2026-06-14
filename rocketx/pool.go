package rocketx

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/gospacex/mqx"
)

// ProducerPool RocketMQ 生产者连接池（Round-Robin）
type ProducerPool struct {
	instances []rocketmq.Producer
	counter   uint64
	size      int
}

func newProducerPool(size int, cacheKey string, cfg *mqx.Config) (*ProducerPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ProducerPool{
		instances: make([]rocketmq.Producer, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		opts := []producer.Option{}

		// 1. 地址配置
		nameServers := cfg.Addrs
		if cfg.RocketMQ != nil && len(cfg.RocketMQ.NameServer) > 0 {
			nameServers = cfg.RocketMQ.NameServer
		}
		opts = append(opts, producer.WithNameServer(nameServers))

		// 2. 组名与实例名
		if cfg.RocketMQ != nil {
			if cfg.RocketMQ.GroupName != "" {
				opts = append(opts, producer.WithGroupName(cfg.RocketMQ.GroupName))
			}
			if cfg.RocketMQ.InstanceName != "" {
				opts = append(opts, producer.WithInstanceName(cfg.RocketMQ.InstanceName))
			}
			if cfg.RocketMQ.Namespace != "" {
				opts = append(opts, producer.WithNamespace(cfg.RocketMQ.Namespace))
			}
			if cfg.RocketMQ.SendTimeout > 0 {
				opts = append(opts, producer.WithSendMsgTimeout(cfg.RocketMQ.SendTimeout))
			}
			if cfg.RocketMQ.RetryOnSendFail > 0 {
				opts = append(opts, producer.WithRetry(cfg.RocketMQ.RetryOnSendFail))
			}

			// 阿里云 ACL 配置
			if cfg.RocketMQ.AccessKey != "" && cfg.RocketMQ.SecretKey != "" {
				opts = append(opts, producer.WithCredentials(primitive.Credentials{
					AccessKey: cfg.RocketMQ.AccessKey,
					SecretKey: cfg.RocketMQ.SecretKey,
				}))
			}
		} else {
			// 默认一个基于 Topic 的发送组
			opts = append(opts, producer.WithGroupName("mqx_producer_"+cfg.Producer.Topic))
		}

		// 3. 通用重试兜底配置
		if cfg.Retry.MaxRetries > 0 {
			opts = append(opts, producer.WithRetry(cfg.Retry.MaxRetries))
		}

		// 初始化
		p, err := rocketmq.NewProducer(opts...)
		if err != nil {
			// 回收已创建的实例
			for j := 0; j <= i; j++ {
				pool.instances[j].Shutdown()
			}
			return nil, fmt.Errorf("create pool instance %d: %w", i, err)
		}

		// RocketMQ 必须手动启动
		err = p.Start()
		if err != nil {
			// 回收已创建的实例
			for j := 0; j <= i; j++ {
				pool.instances[j].Shutdown()
			}
			return nil, fmt.Errorf("start pool instance %d: %w", i, err)
		}

		pool.instances[i] = p
	}

	return pool, nil
}

// Get O(1) 获取一个生产者实例
func (p *ProducerPool) Get() rocketmq.Producer {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅关闭池中所有生产者
func (p *ProducerPool) Close() {
	for _, inst := range p.instances {
		inst.Shutdown()
	}
}
