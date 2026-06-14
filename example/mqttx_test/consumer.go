package main

import (
	"context"
	"log"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/mqttx"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func runConsumerTest(cfg *mqx.Config) {
	log.Println("\n=== Mqttx Consumer E2E 测试开始 ===")

	// topic 严格来自 mq.yaml#mqtt_single —— 优先取 consumer.topics[0]，
	// MQTT 风格下若没配 consumer 段则回退 producer.topic，缺省兜底 "example/topic"。
	topic := "example/topic"
	if cfg != nil {
		if len(cfg.Consumer.Topics) > 0 && cfg.Consumer.Topics[0] != "" {
			topic = cfg.Consumer.Topics[0]
		} else if cfg.Producer.Topic != "" {
			topic = cfg.Producer.Topic
		}
	}
	log.Printf("[Consumer 1] 获取单机 Client 并挂载订阅 (topic=%s)...", topic)

	// 读取 trace 配置开关；不修改 driver 工厂或 tracing.go 签名。
	tCfg, _, _ := mqttx.ParseFile("mq.yaml#mqtt_single")
	traceEnabled := tCfg != nil && tCfg.Trace.Enabled
	if traceEnabled {
		log.Printf("[Consumer 1] trace 链路已启用 -> mqttx.SubscribeTrace")
	} else {
		log.Printf("[Consumer 1] trace 链路已关闭 -> client.Subscribe 直订")
	}

	client, err := mqttx.C("mq.yaml#mqtt_single")
	if err == nil {
		handler := func(client mqtt.Client, msg mqtt.Message) {
			log.Printf("[Consumer 1] 收到消息: %s", string(msg.Payload()))
		}
		if traceEnabled {
			err = mqttx.SubscribeTrace(context.Background(), client, topic, 0, handler)
		} else {
			tok := client.Subscribe(topic, 0, handler)
			tok.Wait()
			if tok.Error() != nil {
				err = tok.Error()
			}
		}
		if err != nil {
			log.Printf("[Consumer 1] 订阅失败: %v", err)
		} else {
			log.Printf("[Consumer 1] 订阅成功，开始监听...")
		}
	} else {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	}

	log.Println("\n[Consumer 2] 模拟获取集群版 Client (CPC) 以验证遗嘱配置...")
	_, err = mqttx.CPC("mq.yaml#mqtt_cluster")
	if err == nil {
		log.Printf("[Consumer 2] 集群版 Client 就绪")
	} else {
		log.Printf("[Consumer 2] 跳过集群模拟: (%v)", err)
	}
}
