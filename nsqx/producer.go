package nsqx

import (
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	"github.com/nsqio/go-nsq"
)

var (
	producerCache sync.Map // key → *nsq.Producer
	producerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 生产者 (Single / Cluster) -> 返回 *nsq.Producer
// NSQ 生产者不管集群还是单机，都是直连 nsqd 节点的
// =====================================================================

func P(path string) (*nsq.Producer, error) { return PPS(path) }

func PPS(path string) (*nsq.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("nsqx.PPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateProducer(cacheKey, cfg)
}

func POS(cfg mqx.Config) (*nsq.Producer, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateProducer(cacheKey, &cfg)
}

func MustP(path string) *nsq.Producer { return MustPPS(path) }

func MustPPS(path string) *nsq.Producer {
	p, err := PPS(path)
	if err != nil {
		panic(fmt.Errorf("nsqx MustPPS failure: %w", err))
	}
	return p
}

func MustPOS(cfg mqx.Config) *nsq.Producer {
	p, err := POS(cfg)
	if err != nil {
		panic(fmt.Errorf("nsqx MustPOS failure: %w", err))
	}
	return p
}

// NSQ 集群生产者实际上也是连接到一个或多个直连节点
func PPC(path string) (*nsq.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("nsqx.PPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateProducer(cacheKey, cfg)
}

func POC(cfg mqx.Config) (*nsq.Producer, error) {
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateProducer(cacheKey, &cfg)
}

func MustPPC(path string) *nsq.Producer {
	p, err := PPC(path)
	if err != nil {
		panic(fmt.Errorf("nsqx MustPPC failure: %w", err))
	}
	return p
}

func MustPOC(cfg mqx.Config) *nsq.Producer {
	p, err := POC(cfg)
	if err != nil {
		panic(fmt.Errorf("nsqx MustPOC failure: %w", err))
	}
	return p
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// =====================================================================

func getOrCreateProducer(cacheKey string, cfg *mqx.Config) (*nsq.Producer, error) {
	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*nsq.Producer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "nsq" {
		return nil, fmt.Errorf("nsqx: driver mismatch, expected nsq but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(*nsq.Producer), nil
	}

	nsqCfg := nsq.NewConfig()

	// 映射高级配置
	if cfg.NSQ != nil {
		if cfg.NSQ.DialTimeout > 0 { nsqCfg.DialTimeout = cfg.NSQ.DialTimeout }
		if cfg.NSQ.ReadTimeout > 0 { nsqCfg.ReadTimeout = cfg.NSQ.ReadTimeout }
		if cfg.NSQ.WriteTimeout > 0 { nsqCfg.WriteTimeout = cfg.NSQ.WriteTimeout }
		if cfg.NSQ.HeartbeatInterval > 0 { nsqCfg.HeartbeatInterval = cfg.NSQ.HeartbeatInterval }
		if cfg.NSQ.OutputBufferSize > 0 { nsqCfg.OutputBufferSize = cfg.NSQ.OutputBufferSize }
	}

	// 鉴权 (NSQ 1.x 协议层只支持单一 AuthSecret / Bearer Token，
	// Username 在原生 NSQ 中无对应字段，此处仅记录用户配置用于日志，Password 走 AuthSecret。)
	if cfg.Auth.Password != "" {
		nsqCfg.AuthSecret = cfg.Auth.Password
	}
	if cfg.Auth.Username != "" {
		log.Printf("[nsqx] note: NSQ protocol does not natively support username; only password is applied as AuthSecret")
	}

	// TLS
	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}
	if tlsConfig != nil {
		nsqCfg.TlsV1 = true
		nsqCfg.TlsConfig = tlsConfig
	}

	// 地址处理 (NSQ 生产者只能连一个节点，如果配了多个，框架内部简化只取第一个)
	// 高级架构下通常在前面挂个 HAProxy 代理多个 nsqd
	addr := ""
	if cfg.NSQ != nil && cfg.NSQ.NsqdAddr != "" {
		addr = cfg.NSQ.NsqdAddr
	} else if len(cfg.Addrs) > 0 {
		addr = cfg.Addrs[0]
	} else {
		addr = "127.0.0.1:4150"
	}

	producer, err := nsq.NewProducer(addr, nsqCfg)
	if err != nil {
		return nil, fmt.Errorf("create nsq producer: %w", err)
	}

	err = producer.Ping()
	if err != nil {
		producer.Stop()
		return nil, fmt.Errorf("ping nsq producer failed: %w", err)
	}

	producerCache.Store(cacheKey, producer)
	log.Printf("[nsqx] producer ready [key=%s, nsqd=%s]", cacheKey, addr)

	return producer, nil
}
