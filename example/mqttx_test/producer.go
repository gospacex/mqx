package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/mqttx"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func runProducerTest(cfg *mqx.Config) {
	log.Println("=== Mqttx Producer E2E 测试开始 ===")

	// topic 严格来自 mq.yaml#mqtt_single.producer.topic，缺省兜底 "example/topic"。
	topic := "example/topic"
	if cfg != nil && cfg.Producer.Topic != "" {
		topic = cfg.Producer.Topic
	}
	log.Printf("[Producer] target topic=%q", topic)

	log.Println("[Producer 1] 并发触发 mqttx.P(mqtt_single) x 100...")
	var wg sync.WaitGroup
	var mqttConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			client, err := mqttx.P("mq.yaml#mqtt_single")
			if err != nil {
				mqttConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			mqttConns.Store(fmt.Sprintf("conn_%d", idx), client)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	var firstPtr string
	isSingleton := true
	successCount := 0
	mqttConns.Range(func(key, value any) bool {
		if v, ok := value.(mqtt.Client); ok {
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
	log.Printf("[Producer 1] 成功获取 Client 数: %d/100", successCount)
	if successCount > 0 {
		log.Printf("[Producer 1] 严格单例模式: %v (指针地址: %s)", isSingleton, firstPtr)
	}

	log.Println("\n[Producer 2] 获取单机 Client 并发送消息...")
	// 读取 trace 配置开关；不修改 driver 工厂或 tracing.go 签名。
	tCfg, _, _ := mqttx.ParseFile("mq.yaml#mqtt_single")
	traceEnabled := tCfg != nil && tCfg.Trace.Enabled
	if traceEnabled {
		log.Printf("[Producer 2] trace 链路已启用 -> mqttx.PublishTrace")
	} else {
		log.Printf("[Producer 2] trace 链路已关闭 -> client.Publish 直发")
	}

	client, err := mqttx.P("mq.yaml#mqtt_single")
	if err == nil {
		if traceEnabled {
			err = mqttx.PublishTrace(context.Background(), client, topic, 0, false, "hello mqtt from mqx!")
		} else {
			tok := client.Publish(topic, 0, false, "hello mqtt from mqx!")
			tok.Wait()
			if tok.Error() != nil {
				err = tok.Error()
			}
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
