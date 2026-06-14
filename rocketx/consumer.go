package rocketx

import (
	"fmt"
	"log"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
)

var (
	consumerCache sync.Map // key → rocketmq.PushConsumer
	consumerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 消费者 (Single / Cluster)
// =====================================================================

func C(path string) (rocketmq.PushConsumer, error) { return CPS(path) }

func CPS(path string) (rocketmq.PushConsumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("rocketx.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (rocketmq.PushConsumer, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustC(path string) rocketmq.PushConsumer { return MustCPS(path) }

func MustCPS(path string) rocketmq.PushConsumer {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("rocketx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) rocketmq.PushConsumer {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("rocketx MustCOS failure: %w", err))
	}
	return c
}

// 集群版

func CPC(path string) (rocketmq.PushConsumer, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("rocketx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (rocketmq.PushConsumer, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustCPC(path string) rocketmq.PushConsumer {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("rocketx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) rocketmq.PushConsumer {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("rocketx MustCOC failure: %w", err))
	}
	return c
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// =====================================================================

func getOrCreateConsumer(cacheKey string, cfg *mqx.Config) (rocketmq.PushConsumer, error) {
	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(rocketmq.PushConsumer), nil
	}

	if cfg.Driver != "" && cfg.Driver != "rocketmq" {
		return nil, fmt.Errorf("rocketx: driver mismatch, expected rocketmq but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(rocketmq.PushConsumer), nil
	}

	opts := []consumer.Option{}

	// 1. 地址配置
	nameServers := cfg.Addrs
	if cfg.RocketMQ != nil && len(cfg.RocketMQ.NameServer) > 0 {
		nameServers = cfg.RocketMQ.NameServer
	}
	opts = append(opts, consumer.WithNameServer(nameServers))

	// 2. 消费组 (强制必须有)
	groupName := cfg.Consumer.Group
	if groupName == "" {
		if cfg.RocketMQ != nil && cfg.RocketMQ.GroupName != "" {
			groupName = cfg.RocketMQ.GroupName
		} else {
			return nil, fmt.Errorf("consumer group is required")
		}
	}
	opts = append(opts, consumer.WithGroupName(groupName))

	// 3. 高级配置映射
	if cfg.RocketMQ != nil {
		if cfg.RocketMQ.Namespace != "" {
			opts = append(opts, consumer.WithNamespace(cfg.RocketMQ.Namespace))
		}
		if cfg.RocketMQ.InstanceName != "" {
			opts = append(opts, consumer.WithInstance(cfg.RocketMQ.InstanceName))
		}
		
		// 消费起始位置映射
		if cfg.RocketMQ.ConsumeFromWhere != "" {
			switch cfg.RocketMQ.ConsumeFromWhere {
			case "CONSUME_FROM_LAST_OFFSET":
				opts = append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset))
			case "CONSUME_FROM_FIRST_OFFSET":
				opts = append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
			case "CONSUME_FROM_TIMESTAMP":
				opts = append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromTimestamp))
			}
		}

		if cfg.RocketMQ.ConsumeOrderly {
			opts = append(opts, consumer.WithConsumerOrder(true))
		}
		if cfg.RocketMQ.MaxReconsumeTimes > 0 {
			opts = append(opts, consumer.WithMaxReconsumeTimes(cfg.RocketMQ.MaxReconsumeTimes))
		}
		
		// 阿里云 ACL
		if cfg.RocketMQ.AccessKey != "" && cfg.RocketMQ.SecretKey != "" {
			opts = append(opts, consumer.WithCredentials(primitive.Credentials{
				AccessKey: cfg.RocketMQ.AccessKey,
				SecretKey: cfg.RocketMQ.SecretKey,
			}))
		}
	}

	// 4. 重试映射
	if cfg.Retry.MaxRetries > 0 {
		opts = append(opts, consumer.WithMaxReconsumeTimes(int32(cfg.Retry.MaxRetries)))
	}

	c, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		return nil, fmt.Errorf("create rocketmq push consumer: %w", err)
	}

	// 框架层不自动调用 c.Start()！
	// 因为 RocketMQ 要求必须先调用 c.Subscribe 注册完处理器后才能启动，
	// 我们把 c 暴露给业务层，让业务自行 Subscribe 和 Start。

	consumerCache.Store(cacheKey, c)
	log.Printf("[rocketx] consumer ready [key=%s, group=%s]. (Warning: You must call Subscribe() and Start() manually)", cacheKey, groupName)

	return c, nil
}
