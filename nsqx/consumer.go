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
	consumerCache sync.Map // key → *nsq.Consumer
	consumerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 消费者 (Single / Cluster) -> 返回 *nsq.Consumer
// NSQ 消费者可以直连 nsqd (Single)，或通过 lookupd 发现 (Cluster)
// =====================================================================

func C(path string) (*nsq.Consumer, error) { return CPS(path) }

func CPS(path string) (*nsq.Consumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("nsqx.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (*nsq.Consumer, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustC(path string) *nsq.Consumer { return MustCPS(path) }

func MustCPS(path string) *nsq.Consumer {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("nsqx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) *nsq.Consumer {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("nsqx MustCOS failure: %w", err))
	}
	return c
}

func CPC(path string) (*nsq.Consumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("nsqx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (*nsq.Consumer, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustCPC(path string) *nsq.Consumer {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("nsqx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) *nsq.Consumer {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("nsqx MustCOC failure: %w", err))
	}
	return c
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// =====================================================================

func getOrCreateConsumer(cacheKey string, cfg *mqx.Config) (*nsq.Consumer, error) {
	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(*nsq.Consumer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "nsq" {
		return nil, fmt.Errorf("nsqx: driver mismatch, expected nsq but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(*nsq.Consumer), nil
	}

	nsqCfg := nsq.NewConfig()

	// 消费组 (Channel) 与 Topic
	topic := ""
	if len(cfg.Consumer.Topics) > 0 {
		topic = cfg.Consumer.Topics[0] // NSQ 实例化消费时强绑定单一 Topic
	}
	channel := cfg.Consumer.Group
	if cfg.NSQ != nil && cfg.NSQ.Channel != "" {
		channel = cfg.NSQ.Channel
	}
	if topic == "" || channel == "" {
		return nil, fmt.Errorf("topic and group(channel) is required for nsq consumer")
	}

	// 映射高级配置
	if cfg.NSQ != nil {
		if cfg.NSQ.MaxInFlight > 0 { nsqCfg.MaxInFlight = cfg.NSQ.MaxInFlight }
		if cfg.NSQ.MaxAttempts > 0 { nsqCfg.MaxAttempts = uint16(cfg.NSQ.MaxAttempts) }
		if cfg.NSQ.MsgTimeout > 0 { nsqCfg.MsgTimeout = cfg.NSQ.MsgTimeout }
		if cfg.NSQ.DialTimeout > 0 { nsqCfg.DialTimeout = cfg.NSQ.DialTimeout }
		if cfg.NSQ.ReadTimeout > 0 { nsqCfg.ReadTimeout = cfg.NSQ.ReadTimeout }
		if cfg.NSQ.WriteTimeout > 0 { nsqCfg.WriteTimeout = cfg.NSQ.WriteTimeout }
	}

	// 鉴权 & TLS
	if cfg.Auth.Password != "" { nsqCfg.AuthSecret = cfg.Auth.Password }
	if cfg.Auth.Username != "" {
		log.Printf("[nsqx] note: NSQ protocol does not natively support username; only password is applied as AuthSecret")
	}
	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil { return nil, err }
	if tlsConfig != nil {
		nsqCfg.TlsV1 = true
		nsqCfg.TlsConfig = tlsConfig
	}

	consumer, err := nsq.NewConsumer(topic, channel, nsqCfg)
	if err != nil {
		return nil, fmt.Errorf("create nsq consumer: %w", err)
	}

	// 注：框架不在这里调用 ConnectToNSQD 启动网络
	// 因为 NSQ 需要业务侧先调用 AddHandler 挂载处理函数。
	// 所以业务侧拿到单例后需自行执行 AddHandler 和 ConnectToNSQLookupd。

	consumerCache.Store(cacheKey, consumer)
	log.Printf("[nsqx] consumer ready [key=%s, topic=%s, channel=%s]. (Warning: You must call AddHandler() and Connect() manually)", cacheKey, topic, channel)

	return consumer, nil
}
