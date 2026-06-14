package rocketx

import (
	"fmt"
	"log"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	producerCache sync.Map // key → rocketmq.Producer (interface)
	producerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 生产者 (Single / Cluster) -> 返回 rocketmq.Producer 接口
// 注意：RocketMQ 的 Producer 和 Consumer 是以 interface 形式返回的
// =====================================================================

func P(path string) (rocketmq.Producer, error) { return PPS(path) }

func PPS(path string) (rocketmq.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("rocketx.PPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateProducer(cacheKey, cfg)
}

func POS(cfg mqx.Config) (rocketmq.Producer, error) {
	cfg.Mode = "single"
	cacheKey := "producer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateProducer(cacheKey, &cfg)
}

func MustP(path string) rocketmq.Producer { return MustPPS(path) }

func MustPPS(path string) rocketmq.Producer {
	p, err := PPS(path)
	if err != nil {
		panic(fmt.Errorf("rocketx MustPPS failure: %w", err))
	}
	return p
}

func MustPOS(cfg mqx.Config) rocketmq.Producer {
	p, err := POS(cfg)
	if err != nil {
		panic(fmt.Errorf("rocketx MustPOS failure: %w", err))
	}
	return p
}

// 集群版

func PPC(path string) (rocketmq.Producer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("rocketx.PPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateProducer(cacheKey, cfg)
}

func POC(cfg mqx.Config) (rocketmq.Producer, error) {
	cfg.Mode = "cluster"
	cacheKey := "producer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateProducer(cacheKey, &cfg)
}

func MustPPC(path string) rocketmq.Producer {
	p, err := PPC(path)
	if err != nil {
		panic(fmt.Errorf("rocketx MustPPC failure: %w", err))
	}
	return p
}

func MustPOC(cfg mqx.Config) rocketmq.Producer {
	p, err := POC(cfg)
	if err != nil {
		panic(fmt.Errorf("rocketx MustPOC failure: %w", err))
	}
	return p
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// =====================================================================

func getOrCreateProducer(cacheKey string, cfg *mqx.Config) (rocketmq.Producer, error) {
	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(rocketmq.Producer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "rocketmq" {
		return nil, fmt.Errorf("rocketx: driver mismatch, expected rocketmq but got %s", cfg.Driver)
	}

	lockVal, _ := producerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := producerCache.Load(cacheKey); ok {
		return val.(rocketmq.Producer), nil
	}

	opts := []producer.Option{}

	// 1. 地址配置
	nameServers := cfg.Addrs
	if cfg.RocketMQ != nil && len(cfg.RocketMQ.NameServer) > 0 {
		nameServers = cfg.RocketMQ.NameServer
	}
	opts = append(opts, producer.WithNameServer(nameServers))

	// 2. 组名与实例名
	if cfg.RocketMQ != nil {
		if cfg.RocketMQ.GroupName != "" {
			opts = append(opts, producer.WithGroupName(cfg.RocketMQ.GroupName))
		}
		if cfg.RocketMQ.InstanceName != "" {
			opts = append(opts, producer.WithInstanceName(cfg.RocketMQ.InstanceName))
		}
		if cfg.RocketMQ.Namespace != "" {
			opts = append(opts, producer.WithNamespace(cfg.RocketMQ.Namespace))
		}
		if cfg.RocketMQ.SendTimeout > 0 {
			opts = append(opts, producer.WithSendMsgTimeout(cfg.RocketMQ.SendTimeout))
		}
		if cfg.RocketMQ.RetryOnSendFail > 0 {
			opts = append(opts, producer.WithRetry(cfg.RocketMQ.RetryOnSendFail))
		}
		
		// 阿里云 ACL 配置
		if cfg.RocketMQ.AccessKey != "" && cfg.RocketMQ.SecretKey != "" {
			opts = append(opts, producer.WithCredentials(primitive.Credentials{
				AccessKey: cfg.RocketMQ.AccessKey,
				SecretKey: cfg.RocketMQ.SecretKey,
			}))
		}
	} else {
		// 默认一个基于 Topic 的发送组
		opts = append(opts, producer.WithGroupName("mqx_producer_"+cfg.Producer.Topic))
	}

	// 3. 通用重试兜底配置
	if cfg.Retry.MaxRetries > 0 {
		opts = append(opts, producer.WithRetry(cfg.Retry.MaxRetries))
	}

	// 初始化
	p, err := rocketmq.NewProducer(opts...)

	if err != nil {
		return nil, fmt.Errorf("create rocketmq producer: %w", err)
	}

	// RocketMQ 必须手动启动
	err = p.Start()
	if err != nil {
		return nil, fmt.Errorf("start rocketmq producer: %w", err)
	}

	// 验证连通性
	// 阿里 SDK 中由于 NameServer 是 lazy 加载的，这里不做阻断式探测，由底层负责

	producerCache.Store(cacheKey, p)
	log.Printf("[rocketx] producer ready [key=%s]", cacheKey)

	return p, nil
}
