package natsx

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/observability"
	"github.com/nats-io/nats.go"
)

type NatsPool struct {
	instances []any // *nats.Conn 或 nats.JetStreamContext
	conns     []*nats.Conn // 仅为了优雅关闭和监控保留真实网络句柄
	counter   uint64
	size      int
}

func newNatsPool(size int, cfg *mqx.Config, cacheKey string) (*NatsPool, error) {
	if size < 1 { size = 1 }
	pool := &NatsPool{
		instances: make([]any, size),
		conns:     make([]*nats.Conn, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		nc, _, err := getOrCreateConn(cfg) // 底层实际建连
		if err != nil {
			for j := 0; j < i; j++ { pool.conns[j].Close() }
			return nil, fmt.Errorf("create nats pool instance %d: %w", i, err)
		}

		pool.conns[i] = nc
		if cfg.NATS != nil && cfg.NATS.JetStream {
			js, err := nc.JetStream()
			if err != nil {
				return nil, fmt.Errorf("init jetstream context: %w", err)
			}
			pool.instances[i] = js
		} else {
			pool.instances[i] = nc
		}

		// 挂载 Native Metrics
		if cfg.Metrics.Enabled {
			go monitorNatsStats(nc, fmt.Sprintf("%s-pool-%d", cacheKey, i))
		}
	}

	return pool, nil
}

func (p *NatsPool) Get() any {
	if p.size == 1 { return p.instances[0] }
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

func (p *NatsPool) DrainAndClose() {
	for _, nc := range p.conns {
		if !nc.IsClosed() {
			_ = nc.Drain() // 优雅清空残留消息
		}
	}
}

func monitorNatsStats(nc *nats.Conn, instanceID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		if nc.IsClosed() { return }
		
		stats := nc.Stats()
		observability.NativeNatsStats.WithLabelValues("nats", instanceID, "in_msgs").Set(float64(stats.InMsgs))
		observability.NativeNatsStats.WithLabelValues("nats", instanceID, "out_msgs").Set(float64(stats.OutMsgs))
		observability.NativeNatsStats.WithLabelValues("nats", instanceID, "reconnects").Set(float64(stats.Reconnects))
	}
}
