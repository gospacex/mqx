package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gospacex/mqx/nsqx"
)

func runProducerTest() {
	log.Println("=== Nsqx Producer E2E 测试开始 ===")

	// 读取配置：根据 trace.enabled 决定后续是否走 *Trace 函数。
	cfg, _, _ := nsqx.ParseFile("mq.yaml#nsq_cluster")
	traceEnabled := cfg != nil && cfg.Trace.Enabled

	log.Println("[Producer 1] 并发触发 nsqx.P(nsq_single) x 100...")
	var wg sync.WaitGroup
	var nsqConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			producer, err := nsqx.P("mq.yaml#nsq_single")
			if err != nil {
				nsqConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			nsqConns.Store(fmt.Sprintf("conn_%d", idx), producer)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	log.Println("\n[Producer 2] 获取单机 Producer 并发送消息...")
	p, err := nsqx.P("mq.yaml#nsq_single")
	if err != nil {
		log.Printf("[Producer 2] 获取 producer 失败: %v", err)
	} else if traceEnabled {
		// trace on：走 PublishTrace，自动在 body 中嵌入 traceparent。
		if err := nsqx.PublishTrace(context.Background(), p, "example_topic", []byte("hello nsq from mqx!")); err != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", err)
		} else {
			log.Printf("[Producer 2] 发送消息成功 (traced)")
		}
	} else {
		// trace off：走原生 Publish，body 原样发送，无 JSON 包裹。
		if err := p.Publish("example_topic", []byte("hello nsq from mqx!")); err != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", err)
		} else {
			log.Printf("[Producer 2] 发送消息成功 (native)")
		}
	}

	log.Println("\n[Producer 3] 模拟获取集群版 Producer (PPC)...")
	p2, err := nsqx.PPC("mq.yaml#nsq_cluster")
	if err != nil {
		log.Printf("[Producer 3] 跳过集群模拟: (%v)", err)
	} else {
		log.Printf("[Producer 3] 集群版生产者就绪, 指针地址: %p", p2)
	}
}