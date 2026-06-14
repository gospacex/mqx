package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/gospacex/mqx/rocketx"
)

func runProducerTest() {
	log.Println("=== Rocketx Producer E2E 测试开始 ===")

	// 顶部读取 cfg.Trace.Enabled，作为本次 trace 开关。
	cfg, _, parseErr := rocketx.ParseFile("mq.yaml")
	traceEnabled := false
	if parseErr == nil && cfg != nil {
		traceEnabled = cfg.Trace.Enabled
	}

	log.Println("[Producer 1] 并发触发 rocketx.P(rocketmq_single) x 100...")
	var wg sync.WaitGroup
	var rocketConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			producer, err := rocketx.P("mq.yaml#rocketmq_single")
			if err != nil {
				rocketConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			rocketConns.Store(fmt.Sprintf("conn_%d", idx), producer)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	var firstPtr string
	isSingleton := true
	successCount := 0
	rocketConns.Range(func(key, value any) bool {
		if v, ok := value.(rocketmq.Producer); ok {
			successCount++
			// interface 类型打印其内部动态值的地址
			ptrStr := fmt.Sprintf("%p", v)
			if firstPtr == "" {
				firstPtr = ptrStr
			} else if firstPtr != ptrStr {
				isSingleton = false
			}
		}
		return true
	})
	log.Printf("[Producer 1] 成功获取接口实例数: %d/100", successCount)
	if successCount > 0 {
		log.Printf("[Producer 1] 严格单例模式: %v (指针地址: %s)", isSingleton, firstPtr)
	}

	log.Println("\n[Producer 2] 获取单机 Producer 并发送消息...")
	producer, err := rocketx.P("mq.yaml#rocketmq_single")
	if err == nil {
		ctx := context.Background()
		msg := primitive.NewMessage("example-topic", []byte("hello rocketmq from mqx!"))
		res, err := rocketx.SendSyncTrace(ctx, producer, msg, traceEnabled)
		if err != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", err)
		} else {
			log.Printf("[Producer 2] 发送消息成功: %s", res.String())
		}
	} else {
		log.Printf("[Producer 2] 跳过发送: 环境未就绪 (%v)", err)
	}

	log.Println("\n[Producer 3] 模拟获取集群版 Producer (PPC)...")
	_, err = rocketx.PPC("mq.yaml#rocketmq_cluster")
	if err == nil {
		log.Printf("[Producer 3] 集群版生产者就绪")
	} else {
		log.Printf("[Producer 3] 跳过集群模拟: (%v)", err)
	}
}