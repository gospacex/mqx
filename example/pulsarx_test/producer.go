package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx/pulsarx"
)

func runProducerTest() {
	log.Println("=== Pulsarx Producer E2E 测试开始 ===")

	// 读取 mq.yaml#pulsar_single，根据 trace.enabled 决定发送路径。
	// 这里和 main.go 一样必须显式带 #pulsar_single 后缀，因为 mq.yaml
	// 里 pulsar_single / pulsar_cluster 两个 key 共存。
	cfg, _, parseErr := pulsarx.ParseFile("mq.yaml#pulsar_single")
	if parseErr != nil {
		log.Printf("[runProducerTest] failed to parse mq.yaml: %v (continuing with tracing disabled)", parseErr)
		cfg = nil
	}
	traceEnabled := cfg != nil && cfg.Trace.Enabled
	log.Printf("[runProducerTest] trace.enabled=%v", traceEnabled)

	log.Println("[Producer 1] 并发触发 pulsarx.P(pulsar_single) x 100...")
	var wg sync.WaitGroup
	var pulsarConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			producer, err := pulsarx.P("mq.yaml#pulsar_single")
			if err != nil {
				pulsarConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			pulsarConns.Store(fmt.Sprintf("conn_%d", idx), producer)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	log.Println("\n[Producer 2] 获取单机 Producer 并发送消息...")
	producer, err := pulsarx.P("mq.yaml#pulsar_single")
	if err == nil {
		ctx := context.Background()
		msg := &pulsar.ProducerMessage{
			Payload: []byte("hello pulsar from mqx!"),
		}
		// traceEnabled=true 走 SendTrace（内部已建 span 并注入 trace context）；
		// traceEnabled=false 直接用 producer.Send，避免无谓的 OTel 开销。
		if traceEnabled {
			_, err = pulsarx.SendTrace(ctx, producer, msg)
		} else {
			_, err = producer.Send(ctx, msg)
		}
		if err != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", err)
		} else {
			log.Printf("[Producer 2] 发送消息成功")
		}
	} else {
		log.Printf("[Producer 2] 跳过发送: 环境未就绪 (%v)", err)
	}
}
