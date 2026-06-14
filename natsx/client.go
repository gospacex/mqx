package natsx

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

var (
	connCache sync.Map // key → *nats.Conn
	connLocks sync.Map // key → *sync.Mutex
)

// getOrCreateConn 返回底层的 nats.Conn
func getOrCreateConn(cfg *mqx.Config) (*nats.Conn, string, error) {
	cacheKey := "natsconn:" + utils.ConfigFingerprint(cfg)

	if val, ok := connCache.Load(cacheKey); ok {
		return val.(*nats.Conn), cacheKey, nil
	}

	lockVal, _ := connLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := connCache.Load(cacheKey); ok {
		return val.(*nats.Conn), cacheKey, nil
	}

	// 1. 拼接地址
	url := strings.Join(cfg.Addrs, ",")
	if url == "" {
		url = nats.DefaultURL
	}

	opts := []nats.Option{}

	// 2. Auth & Security
	if cfg.Auth.Token != "" {
		opts = append(opts, nats.Token(cfg.Auth.Token))
	} else if cfg.Auth.Username != "" {
		opts = append(opts, nats.UserInfo(cfg.Auth.Username, cfg.Auth.Password))
	}

	if cfg.NATS != nil {
		if cfg.NATS.Name != "" {
			opts = append(opts, nats.Name(cfg.NATS.Name))
		}
		if cfg.NATS.CredsFile != "" {
			opts = append(opts, nats.UserCredentials(cfg.NATS.CredsFile))
		}
		if cfg.NATS.NKeyFile != "" {
			seed, err := os.ReadFile(cfg.NATS.NKeyFile)
				if err != nil {
					return nil, "", fmt.Errorf("read nkey seed file: %w", err)
				}
				kp, err := nkeys.FromSeed(seed)
				if err != nil {
					return nil, "", fmt.Errorf("parse nkey seed: %w", err)
				}
				pubKey, err := kp.PublicKey()
				if err != nil {
					return nil, "", fmt.Errorf("get nkey public key: %w", err)
				}
				opts = append(opts, nats.Nkey(pubKey, func(nonce []byte) ([]byte, error) {
					sig, err := kp.Sign(nonce)
					if err != nil {
						return nil, fmt.Errorf("nkey sign: %w", err)
					}
					return sig, nil
				}))
		}
		if cfg.NATS.MaxReconnects != 0 {
			opts = append(opts, nats.MaxReconnects(cfg.NATS.MaxReconnects))
		}
		if cfg.NATS.ReconnectWait > 0 {
			opts = append(opts, nats.ReconnectWait(cfg.NATS.ReconnectWait))
		}
		if cfg.NATS.PingInterval > 0 {
			opts = append(opts, nats.PingInterval(cfg.NATS.PingInterval))
		}
	}

	// 3. TLS
	if cfg.TLS.Enabled {
		opts = append(opts, nats.Secure())
		if cfg.TLS.CAFile != "" {
			opts = append(opts, nats.RootCAs(cfg.TLS.CAFile))
		}
		if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
			opts = append(opts, nats.ClientCert(cfg.TLS.CertFile, cfg.TLS.KeyFile))
		}
	}

	// 4. 重试映射
	if cfg.Retry.MaxRetries > 0 {
		opts = append(opts, nats.MaxReconnects(cfg.Retry.MaxRetries))
	}

	// 初始化连线
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, "", fmt.Errorf("create nats connection: %w", err)
	}

	connCache.Store(cacheKey, nc)
	log.Printf("[natsx] connection ready [key=%s, url=%s]", cacheKey, url)

	return nc, cacheKey, nil
}
