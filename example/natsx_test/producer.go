package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gospacex/mqx/natsx"
	"github.com/nats-io/nats.go"
)

// traceEnabledSingle / traceEnabledCluster 在 consumer.go 中定义(同包共享),
// 这里直接引用,避免 var 重复声明。

func runProducerTest() {
	log.Println("=== Natsx Producer E2E 测试开始 ===")

	// Producer 1: 并发触发 natsx.P(nats_single) x 100
	log.Println("[Producer 1] 并发触发 natsx.P(nats_single) x 100...")
	var wg sync.WaitGroup
	var natsConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			producer, err := natsx.P("mq.yaml#nats_single")
			if err != nil {
				natsConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			natsConns.Store(fmt.Sprintf("conn_%d", idx), producer)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	// Producer 2: 单机 Core Producer 发送消息 (按 traceEnabled 分支)
	log.Println("\n[Producer 2] 获取单机 Core Producer 并发送消息...")
	p, err := natsx.P("mq.yaml#nats_single")
	if err == nil {
		nc := p.(*nats.Conn)
		ctx := context.Background()
		subject, payload := "example.subject", []byte("hello nats core from mqx!")
		var sendErr error
		if traceEnabledSingle {
			sendErr = natsx.PublishTrace(ctx, nc, subject, payload)
		} else {
			sendErr = nc.PublishMsg(&nats.Msg{Subject: subject, Data: payload})
		}
		if sendErr != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", sendErr)
		} else {
			log.Printf("[Producer 2] 发送消息成功 (trace=%v)", traceEnabledSingle)
		}
	} else {
		log.Printf("[Producer 2] 跳过发送: 环境未就绪 (%v)", err)
	}

	// Producer 3: 集群版 JetStream Producer (PPC) 发送消息 (按 traceEnabled 分支)
	log.Println("\n[Producer 3] 获取集群版 JetStream Producer (PPC) 并发送消息...")
	p2, err := natsx.PPC("mq.yaml#nats_js_cluster")
	if err == nil {
		js := p2.(nats.JetStreamContext)
		ctx := context.Background()
		subject, payload := "orders.events", []byte("hello nats jetstream from mqx!")
		var sendErr error
		if traceEnabledCluster {
			_, sendErr = natsx.JSPublishTrace(ctx, js, subject, payload)
		} else {
			_, sendErr = js.PublishMsg(&nats.Msg{Subject: subject, Data: payload})
		}
		if sendErr != nil {
			log.Printf("[Producer 3] 发送消息失败: %v", sendErr)
		} else {
			log.Printf("[Producer 3] 发送消息成功 (trace=%v)", traceEnabledCluster)
		}
	} else {
		log.Printf("[Producer 3] 跳过集群发送: 环境未就绪 (%v)", err)
	}
}