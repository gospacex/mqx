package mqttx

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	clientCache sync.Map // key → mqtt.Client
	clientLocks sync.Map // key → *sync.Mutex
)

// getOrCreateClient MQTT 的生产和消费基于同一个 Client (双向通信)，统一在此初始化
func getOrCreateClient(cfg *mqx.Config) (mqtt.Client, string, error) {
	cacheKey := "mqtt:" + utils.ConfigFingerprint(cfg)

	if val, ok := clientCache.Load(cacheKey); ok {
		return val.(mqtt.Client), cacheKey, nil
	}

	if cfg.Driver != "" && cfg.Driver != "mqtt" {
		return nil, "", fmt.Errorf("mqttx: driver mismatch, expected mqtt but got %s", cfg.Driver)
	}

	lockVal, _ := clientLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := clientCache.Load(cacheKey); ok {
		return val.(mqtt.Client), cacheKey, nil
	}

	opts := mqtt.NewClientOptions()

	// 1. 地址
	for _, addr := range cfg.Addrs {
		if !strings.HasPrefix(addr, "tcp://") && !strings.HasPrefix(addr, "ssl://") && !strings.HasPrefix(addr, "ws://") {
			if cfg.TLS.Enabled {
				addr = "ssl://" + addr
			} else {
				addr = "tcp://" + addr
			}
		}
		opts.AddBroker(addr)
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
		
		if cfg.MQTT.KeepAlive > 0 { opts.SetKeepAlive(cfg.MQTT.KeepAlive) }
		if cfg.MQTT.PingTimeout > 0 { opts.SetPingTimeout(cfg.MQTT.PingTimeout) }
		if cfg.MQTT.ConnectTimeout > 0 { opts.SetConnectTimeout(cfg.MQTT.ConnectTimeout) }
		if cfg.MQTT.MaxReconnectInterval > 0 { opts.SetMaxReconnectInterval(cfg.MQTT.MaxReconnectInterval) }

		// 遗嘱消息 (Will)
		if cfg.MQTT.WillEnabled && cfg.MQTT.WillTopic != "" {
			opts.SetWill(cfg.MQTT.WillTopic, cfg.MQTT.WillPayload, cfg.MQTT.WillQoS, cfg.MQTT.WillRetained)
		}
	}

	// 4. TLS
	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, "", fmt.Errorf("build tls config: %w", err)
	}
	if tlsConfig != nil {
		opts.SetTLSConfig(tlsConfig)
	}

	// 初始化建连
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, "", fmt.Errorf("connect to mqtt broker: %w", token.Error())
	}

	clientCache.Store(cacheKey, client)
	log.Printf("[mqttx] client connection ready [key=%s]", cacheKey)

	return client, cacheKey, nil
}
