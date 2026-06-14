package nsqx

import (
	"fmt"
	"sync/atomic"

	"github.com/gospacex/mqx"
	"github.com/nsqio/go-nsq"
)

// ProducerPool NSQ 生产者连接池（Round-Robin）
type ProducerPool struct {
	instances []*nsq.Producer
	counter   uint64
	size      int
}

func newProducerPool(size int, cacheKey string, cfg *mqx.Config, addr string) (*ProducerPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ProducerPool{
		instances: make([]*nsq.Producer, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		nsqCfg := nsq.NewConfig()

		// 映射高级配置
		if cfg.NSQ != nil {
			if cfg.NSQ.DialTimeout > 0 {
				nsqCfg.DialTimeout = cfg.NSQ.DialTimeout
			}
			if cfg.NSQ.ReadTimeout > 0 {
				nsqCfg.ReadTimeout = cfg.NSQ.ReadTimeout
			}
			if cfg.NSQ.WriteTimeout > 0 {
				nsqCfg.WriteTimeout = cfg.NSQ.WriteTimeout
			}
			if cfg.NSQ.HeartbeatInterval > 0 {
				nsqCfg.HeartbeatInterval = cfg.NSQ.HeartbeatInterval
			}
			if cfg.NSQ.OutputBufferSize > 0 {
				nsqCfg.OutputBufferSize = cfg.NSQ.OutputBufferSize
			}
		}

		// 鉴权
		if cfg.Auth.Password != "" {
			nsqCfg.AuthSecret = cfg.Auth.Password
		}

		// TLS
		tlsConfig, err := cfg.TLS.BuildTLS()
		if err != nil {
			// 初始化失败，回收已创建的实例
			for j := 0; j < i; j++ {
				pool.instances[j].Stop()
			}
			return nil, fmt.Errorf("build tls config: %w", err)
		}
		if tlsConfig != nil {
			nsqCfg.TlsV1 = true
			nsqCfg.TlsConfig = tlsConfig
		}

		producer, err := nsq.NewProducer(addr, nsqCfg)
		if err != nil {
			// 初始化失败，回收已创建的实例
			for j := 0; j < i; j++ {
				pool.instances[j].Stop()
			}
			return nil, fmt.Errorf("create pool instance %d: %w", i, err)
		}

		// Ping 验证
		err = producer.Ping()
		if err != nil {
			producer.Stop()
			// 回收已创建的实例
			for j := 0; j <= i; j++ {
				pool.instances[j].Stop()
			}
			return nil, fmt.Errorf("ping pool instance %d: %w", i, err)
		}

		pool.instances[i] = producer
	}

	return pool, nil
}

// Get O(1) 获取一个生产者实例
func (p *ProducerPool) Get() *nsq.Producer {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅关闭池中所有生产者
func (p *ProducerPool) Close() {
	for _, inst := range p.instances {
		inst.Stop()
	}
}
