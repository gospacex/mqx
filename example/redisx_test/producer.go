package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/redisx"
	"github.com/redis/go-redis/v9"
)

func runProducerTest(cfg *mqx.Config) {
	log.Println("=== Redisx Producer E2E 测试开始 ===")

	// 按 cfg.Trace.Enabled 决定是否走 *Trace 函数：true 时由 redisx 内部创建
	// / 注入 span；false 时直接调 client.XAdd，零开销。
	cfg, _, _ = redisx.ParseFile("mq.yaml#redis_single")
	traceEnabled := cfg != nil && cfg.Trace.Enabled

	// stream 与 max_len 严格来自 mq.yaml#redis_single 的 producer.* / redis.* 段；
	// 缺省时给出兜底，防止 cfg=nil 触发空指针。
	streamName := "example-stream"
	maxLen := int64(1000)
	if cfg != nil {
		if cfg.Producer.Topic != "" {
			streamName = cfg.Producer.Topic
		}
		if cfg.Redis != nil && cfg.Redis.MaxLen > 0 {
			maxLen = cfg.Redis.MaxLen
		}
	}
	log.Printf("[Producer] target stream=%q max_len=%d traceEnabled=%v", streamName, maxLen, traceEnabled)

	log.Println("[Producer 1] 并发触发 redisx.P(redis_single) x 100...")
	var wg sync.WaitGroup
	var redisConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			client, err := redisx.P("mq.yaml#redis_single")
			if err != nil {
				redisConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			redisConns.Store(fmt.Sprintf("conn_%d", idx), client)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	var firstPtr string
	isSingleton := true
	successCount := 0
	redisConns.Range(func(key, value any) bool {
		if v, ok := value.(*redis.Client); ok {
			successCount++
			ptrStr := fmt.Sprintf("%p", v)
			if firstPtr == "" {
				firstPtr = ptrStr
			} else if firstPtr != ptrStr {
				isSingleton = false
			}
		}
		return true
	})
	log.Printf("[Producer 1] 成功获取连接数: %d/100", successCount)
	if successCount > 0 {
		log.Printf("[Producer 1] 严格单例模式: %v (指针地址: %s)", isSingleton, firstPtr)
	}

	log.Println("\n[Producer 2] 获取单机 Client 并使用 XAdd 发送消息...")
	client, err := redisx.P("mq.yaml#redis_single")
	if err == nil {
		values := map[string]interface{}{"msg": "hello redis stream from mqx!"}
		ctx := context.Background()

		if traceEnabled {
			// trace=on：走 XAddTraceWithMaxLen，stream / max_len 都来自配置，
			// 并由 redisx 内部把 trace context 注入消息（traceparent）。
			_, err = redisx.XAddTraceWithMaxLen(ctx, client, streamName, maxLen, values)
		} else {
			// trace=off：直接调 client.XAdd，跳过 span 创建 / 注入。
			_, err = client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				Values: values,
				MaxLen: maxLen,
			}).Result()
		}

		if err != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", err)
		} else {
			log.Printf("[Producer 2] 发送 Stream 消息成功！")
		}
	} else {
		log.Printf("[Producer 2] 跳过发送: 环境未就绪 (%v)", err)
	}

	log.Println("\n[Producer 3] 模拟获取集群版 Producer (PPC)...")
	// 注意这里返回的是 *redis.ClusterClient
	clusterClient, err := redisx.PPC("mq.yaml#redis_cluster")
	if err == nil {
		log.Printf("[Producer 3] 集群版生产者就绪, 指针地址: %p", clusterClient)
	} else {
		log.Printf("[Producer 3] 跳过集群模拟: (%v)", err)
	}
}