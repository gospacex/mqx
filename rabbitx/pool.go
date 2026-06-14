package rabbitx

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/gospacex/mqx"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionPool 实现了 RabbitMQ Connection 的多路复用轮询
type ConnectionPool struct {
	instances []*amqp.Connection
	counter   uint64
	size      int
}

func newConnectionPool(size int, cfg *mqx.Config, cacheKey string) (*ConnectionPool, error) {
	if size < 1 { size = 1 }
	pool := &ConnectionPool{
		instances: make([]*amqp.Connection, size),
		size:      size,
	}

	uri, err := BuildAMQPURI(cfg)
	if err != nil {
		return nil, fmt.Errorf("build amqp uri: %w", err)
	}

	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	for i := 0; i < size; i++ {
		var conn *amqp.Connection
		if tlsConfig != nil {
			conn, err = amqp.DialTLS(uri, tlsConfig)
		} else {
			conn, err = amqp.Dial(uri)
		}

		if err != nil {
			// 回收已建连接
			for j := 0; j < i; j++ { pool.instances[j].Close() }
			return nil, fmt.Errorf("create rabbit connection %d: %w", i, err)
		}
		
		// 仅用 0 号实例负责声明拓扑，避免重复声明引发性能损耗
		if i == 0 {
			ch, err := conn.Channel()
			if err == nil {
				if setupErr := SetupTopology(ch, cfg); setupErr != nil {
					log.Printf("[rabbitx] topology setup warning: %v", setupErr)
				}
				_ = ch.Close()
			}
		}

		pool.instances[i] = conn
	}

	return pool, nil
}

// Get O(1) 原子获取一个原生连接
func (p *ConnectionPool) Get() *amqp.Connection {
	if p.size == 1 { return p.instances[0] }
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 关闭所有底层连接
func (p *ConnectionPool) Close() {
	for _, inst := range p.instances {
		if !inst.IsClosed() {
			_ = inst.Close()
		}
	}
}
