package mqttx

import (
	"fmt"
	"sync/atomic"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gospacex/mqx"
)

// ClientPool MQTT 客户端连接池（Round-Robin）
type ClientPool struct {
	instances []mqtt.Client
	counter   uint64
	size      int
}

func newClientPool(size int, cacheKey string, cfg *mqx.Config) (*ClientPool, error) {
	if size < 1 {
		size = 1
	}
	pool := &ClientPool{
		instances: make([]mqtt.Client, size),
		size:      size,
	}

	for i := 0; i < size; i++ {
		opts := mqtt.NewClientOptions()

		// 1. 地址
		for _, addr := range cfg.Addrs {
			prefix := ""
			if !hasPrefix(addr, "tcp://", "ssl://", "ws://", "wss://") {
				if cfg.TLS.Enabled {
					prefix = "ssl://"
				} else {
					prefix = "tcp://"
				}
			}
			if prefix != "" {
				opts.AddBroker(prefix + addr)
			} else {
				opts.AddBroker(addr)
			}
		}

		// 2. 鉴权
		if cfg.Auth.Username != "" {
			opts.SetUsername(cfg.Auth.Username)
			opts.SetPassword(cfg.Auth.Password)
		}

		// 3. 高级配置
		if cfg.MQTT != nil {
			if cfg.MQTT.ClientID != "" {
				opts.SetClientID(cfg.MQTT.ClientID)
			}
			opts.SetCleanSession(cfg.MQTT.CleanSession)
			opts.SetOrderMatters(cfg.MQTT.OrderMatters)
			opts.SetResumeSubs(cfg.MQTT.ResumeSubs)
			opts.SetAutoReconnect(cfg.MQTT.AutoReconnect)

			if cfg.MQTT.KeepAlive > 0 {
				opts.SetKeepAlive(cfg.MQTT.KeepAlive)
			}
			if cfg.MQTT.PingTimeout > 0 {
				opts.SetPingTimeout(cfg.MQTT.PingTimeout)
			}
			if cfg.MQTT.ConnectTimeout > 0 {
				opts.SetConnectTimeout(cfg.MQTT.ConnectTimeout)
			}
			if cfg.MQTT.MaxReconnectInterval > 0 {
				opts.SetMaxReconnectInterval(cfg.MQTT.MaxReconnectInterval)
			}

			// 遗嘱消息
			if cfg.MQTT.WillEnabled && cfg.MQTT.WillTopic != "" {
				opts.SetWill(cfg.MQTT.WillTopic, cfg.MQTT.WillPayload, cfg.MQTT.WillQoS, cfg.MQTT.WillRetained)
			}
		}

		// 4. TLS
		tlsConfig, err := cfg.TLS.BuildTLS()
		if err != nil {
			// 初始化失败，回收已创建的实例
			for j := 0; j < i; j++ {
				pool.instances[j].Disconnect(250)
			}
			return nil, fmt.Errorf("build tls config: %w", err)
		}
		if tlsConfig != nil {
			opts.SetTLSConfig(tlsConfig)
		}

		// 初始化建连
		client := mqtt.NewClient(opts)
		token := client.Connect()
		if token.Wait() && token.Error() != nil {
			// 回收已创建的实例
			for j := 0; j <= i; j++ {
				pool.instances[j].Disconnect(250)
			}
			return nil, fmt.Errorf("connect pool instance %d: %w", i, token.Error())
		}

		pool.instances[i] = client
	}

	return pool, nil
}

// Get O(1) 获取一个客户端实例
func (p *ClientPool) Get() mqtt.Client {
	if p.size == 1 {
		return p.instances[0]
	}
	idx := atomic.AddUint64(&p.counter, 1)
	return p.instances[idx%uint64(p.size)]
}

// Close 优雅关闭池中所有客户端
func (p *ClientPool) Close() {
	for _, inst := range p.instances {
		inst.Disconnect(250)
	}
}

// hasPrefix 检查字符串是否以任意给定前缀开头
func hasPrefix(s string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}
