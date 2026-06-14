package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/gospacex/mqx/rabbitx"
)

func runProducerTest() {
	log.Println("=== Rabbitx Producer E2E 测试开始 ===")

	// 读取配置以判断 trace.enabled —— 配置开关的实现位点。
	// 与 main.go 共用同一个 key "mq.yaml#rabbit_single"。
	cfg, _, parseErr := rabbitx.ParseFile("mq.yaml#rabbit_single")
	if parseErr != nil {
		log.Printf("[Producer] failed to parse mq.yaml: %v (continuing with trace disabled)", parseErr)
		cfg = nil
	}
	traceEnabled := cfg != nil && cfg.Trace.Enabled
	log.Printf("[Producer] trace.enabled = %v", traceEnabled)

	// ---------------------------------------------------------
	// 场景 1: 并发防抖测试
	// ---------------------------------------------------------
	log.Println("[Producer 1] 并发触发 rabbitx.P(rabbit_single) x 100...")
	var wg sync.WaitGroup
	var amqpConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := rabbitx.P("mq.yaml#rabbit_single")
			if err != nil {
				amqpConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			amqpConns.Store(fmt.Sprintf("conn_%d", idx), conn)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	var firstPtr string
	isSingleton := true
	successCount := 0
	amqpConns.Range(func(key, value any) bool {
		if v, ok := value.(*amqp.Connection); ok {
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

	// ---------------------------------------------------------
	// 场景 2: 发送原生消息 (直接操作 Channel) - 按 trace.enabled 走分支
	// ---------------------------------------------------------
	log.Println("\n[Producer 2] 获取单机 Connection 并打开 Channel 发送消息...")
	conn, err := rabbitx.P("mq.yaml#rabbit_single")
	if err == nil {
		// 原生零侵入体验：拿到 Connection 后完全用官方库
		ch, chErr := conn.Channel()
		if chErr == nil {
			err = rabbitx.PublishWithContextTrace(
				context.Background(),
				ch,
				"example.exchange",
				"example.route.test",
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte("hello rabbitmq from mqx!"),
					DeliveryMode: amqp.Persistent,
				},
				traceEnabled,
			)
			if err != nil {
				log.Printf("[Producer 2] 发送消息失败: %v", err)
			} else {
				log.Printf("[Producer 2] 发送消息成功 (trace=%v)", traceEnabled)
			}
			_ = ch.Close()
		} else {
			log.Printf("[Producer 2] 打开 Channel 失败: %v", chErr)
		}
	} else {
		log.Printf("[Producer 2] 跳过发送: 环境未就绪 (%v)", err)
	}

	// ---------------------------------------------------------
	// 场景 3: 模拟集群获取
	// ---------------------------------------------------------
	log.Println("\n[Producer 3] 模拟获取集群版 Producer (PPC)...")
	_, err = rabbitx.PPC("mq.yaml#rabbit_cluster")
	if err == nil {
		log.Printf("[Producer 3] 集群版生产者就绪")
	} else {
		log.Printf("[Producer 3] 跳过集群模拟: (%v)", err)
	}
}