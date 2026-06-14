package pulsarx

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	clientCache sync.Map // key → pulsar.Client
	clientLocks sync.Map // key → *sync.Mutex
)

// getOrCreateClient 返回底层的 pulsar.Client
// 注意：Pulsar SDK 是基于 Client 创建 Producer 和 Consumer 的，因此我们先将 Client 单例化
func getOrCreateClient(cfg *mqx.Config) (pulsar.Client, string, error) {
	// Pulsar 的地址要求是一个字符串 (通常逗号分隔)
	url := strings.Join(cfg.Addrs, ",")
	if !strings.HasPrefix(url, "pulsar://") && !strings.HasPrefix(url, "pulsar+ssl://") {
		if cfg.TLS.Enabled {
			url = "pulsar+ssl://" + url
		} else {
			url = "pulsar://" + url
		}
	}

	// 指纹基于 Driver + Address + Auth
	cacheKey := "client:" + utils.ConfigFingerprint(cfg)

	if val, ok := clientCache.Load(cacheKey); ok {
		return val.(pulsar.Client), cacheKey, nil
	}

	if cfg.Driver != "" && cfg.Driver != "pulsar" {
		return nil, "", fmt.Errorf("pulsarx: driver mismatch, expected pulsar but got %s", cfg.Driver)
	}

	lockVal, _ := clientLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := clientCache.Load(cacheKey); ok {
		return val.(pulsar.Client), cacheKey, nil
	}

	opts := pulsar.ClientOptions{
		URL: url,
	}

	if cfg.Pulsar != nil {
		if cfg.Pulsar.OperationTimeout > 0 {
			opts.OperationTimeout = cfg.Pulsar.OperationTimeout
		}
		if cfg.Pulsar.ConnectionTimeout > 0 {
			opts.ConnectionTimeout = cfg.Pulsar.ConnectionTimeout
		}
	}

	// Auth 优先顺序: TLS Cert > Token > Basic Auth > (Username-only)
	// Token 与 Basic 互斥：Token 优先 (常见于 JWT 场景)。
	if cfg.Auth.Token != "" {
		opts.Authentication = pulsar.NewAuthenticationToken(cfg.Auth.Token)
	} else if cfg.Auth.Username != "" {
		// Basic Auth 走用户名/密码；空密码也允许（部分 Pulsar 部署使用 username-only 鉴权）
		auth, err := pulsar.NewAuthenticationBasic(cfg.Auth.Username, cfg.Auth.Password)
		if err != nil {
			return nil, "", fmt.Errorf("create pulsar basic auth: %w", err)
		}
		opts.Authentication = auth
	}

	// TLS
	if cfg.TLS.Enabled {
		opts.TLSTrustCertsFilePath = cfg.TLS.CAFile
		opts.TLSAllowInsecureConnection = cfg.TLS.SkipVerify
		if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
			opts.Authentication = pulsar.NewAuthenticationTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		}
	}

	client, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, "", fmt.Errorf("create pulsar client: %w", err)
	}

	clientCache.Store(cacheKey, client)
	log.Printf("[pulsarx] client connection ready [key=%s]", cacheKey)

	return client, cacheKey, nil
}
