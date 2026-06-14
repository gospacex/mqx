package redisx

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gospacex/mqx"
	"github.com/redis/go-redis/v9"
)

// ClientPool Redis 客户端连接池（Round-Robin）
type ClientPool struct {
	instances []*redis.Client
	counter   uint64
	size      int
}

// ClusterClientPool Redis 集群客户端连接池（Round-Robin）
type ClusterClientPool struct {
	instances []*redis.ClusterClient
	counter   uint64
	size      int
}

func newClientPool(size int, cacheKey string, cfg *mqx.Config) (*ClientPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ClientPool{
		instances: make([]*redis.Client, size),
		size:      size,
	}

	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("no addresses provided")
	}

	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	for i := 0; i < size; i++ {
		opts := &redis.Options{
			Addr:      cfg.Addrs[0],
			Username:  cfg.Auth.Username,
			Password:  cfg.Auth.Password,
			TLSConfig: tlsConfig,
		}

		if cfg.Redis != nil {
			opts.DB = cfg.Redis.DB
			opts.PoolSize = cfg.Redis.PoolSize
			opts.MinIdleConns = cfg.Redis.MinIdleConns
		}

		client := redis.NewClient(opts)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := client.Ping(ctx).Err()
		cancel()

		if err != nil {
			_ = client.Close()
			// 回收已创建的实例
			for j := 0; j <= i; j++ {
				pool.instances[j].Close()
			}
			return nil, fmt.Errorf("ping pool instance %d: %w", i, err)
		}

		pool.instances[i] = client
	}

	return pool, nil
}

func newClusterClientPool(size int, cacheKey string, cfg *mqx.Config) (*ClusterClientPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ClusterClientPool{
		instances: make([]*redis.ClusterClient, size),
		size:      size,
	}

	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	for i := 0; i < size; i++ {
		opts := &redis.ClusterOptions{
			Addrs:     cfg.Addrs,
			Username:  cfg.Auth.Username,
			Password:  cfg.Auth.Password,
			TLSConfig: tlsConfig,
		}

		if cfg.Redis != nil {
			opts.PoolSize = cfg.Redis.PoolSize
		}

		client := redis.NewClusterClient(opts)
		err := client.Ping(context.Background()).Err()

		if err != nil {
			_ = client.Close()
			// 回收已创建的实例
			for j := 0; j <= i; j++ {
				pool.instances[j].Close()
			}
			return nil, fmt.Errorf("ping pool instance %d: %w", i, err)
		}

		pool.instances[i] = client
	}

	return pool, nil
}

// Get O(1) 获取一个单机客户端实例
func (p *ClientPool) Get() *redis.Client {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅关闭池中所有单机客户端
func (p *ClientPool) Close() {
	for _, inst := range p.instances {
		inst.Close()
	}
}

// Get O(1) 获取一个集群客户端实例
func (p *ClusterClientPool) Get() *redis.ClusterClient {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅关闭池中所有集群客户端
func (p *ClusterClientPool) Close() {
	for _, inst := range p.instances {
		inst.Close()
	}
}
