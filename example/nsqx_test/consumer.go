package main

import (
	"context"
	"log"

	"github.com/gospacex/mqx/nsqx"
	"github.com/nsqio/go-nsq"
)

func runConsumerTest() {
	log.Println("\n=== Nsqx Consumer E2E 测试开始 ===")

	// 读取配置：根据 trace.enabled 决定是否走 AddHandlerTrace。
	cfg, _, _ := nsqx.ParseFile("mq.yaml#nsq_cluster")
	traceEnabled := cfg != nil && cfg.Trace.Enabled

	log.Println("[Consumer 1] 获取单机消费者并挂载处理函数...")
	c, err := nsqx.C("mq.yaml#nsq_single")
	if err != nil {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	} else if traceEnabled {
		nsqx.AddHandlerTrace(context.Background(), c, func(msg *nsq.Message) error {
			log.Printf("[Consumer 1] 收到消息: %s", string(msg.Body))
			return nil
		})
	} else {
		c.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
			log.Printf("[Consumer 1] 收到消息: %s", string(msg.Body))
			return nil
		}))
	}

	if err == nil {
		if err := c.ConnectToNSQD("127.0.0.1:4150"); err != nil {
			log.Printf("[Consumer 1] 连接 nsqd 失败: %v", err)
		} else {
			log.Printf("[Consumer 1] 消费者启动成功，开始监听...")
		}
	}

	log.Println("\n[Consumer 2] 模拟获取集群版 Consumer 并通过 Lookupd 发现...")
	c2, err := nsqx.CPC("mq.yaml#nsq_cluster")
	if err != nil {
		log.Printf("[Consumer 2] 跳过集群模拟: (%v)", err)
		return
	}
	log.Printf("[Consumer 2] 集群版消费者就绪, 准备连接 Lookupd")

	if traceEnabled {
		nsqx.AddHandlerTrace(context.Background(), c2, func(msg *nsq.Message) error { return nil })
	} else {
		c2.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error { return nil }))
	}

	if err := c2.ConnectToNSQLookupd("http://10.0.0.1:4161"); err != nil {
		log.Printf("[Consumer 2] 连接 Lookupd 失败: %v", err)
	}
}