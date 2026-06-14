package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/redisx"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// messageCarrier 从 Redis Stream消息中提取 trace context
type messageCarrier struct {
	msg map[string]interface{}
}

func (m *messageCarrier) Get(key string) string {
	if v, ok := m.msg[key]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func (m *messageCarrier) Set(key, value string) {
	m.msg[key] = value
}

func (m *messageCarrier) Keys() []string {
	keys := make([]string, 0, len(m.msg))
	for k := range m.msg {
		keys = append(keys, k)
	}
	return keys
}

// extractTraceContext 从消息中提取 trace context
func extractTraceContext(ctx context.Context, msg map[string]interface{}) context.Context {
	carrier := &messageCarrier{msg: msg}
	propagator := observability.GetGlobalPropagator()
	return propagator.Extract(ctx, carrier)
}

func runConsumerTest(cfg *mqx.Config) {
	log.Println("\n=== Redisx Consumer E2E 测试开始 ===")

	// 按 cfg.Trace.Enabled 决定是否走 *Trace 函数：true 时由 redisx 内部创建
	// span / 提取子 span；false 时直接调 client.XReadGroup / XAck，零开销。
	cfg, _, _ = redisx.ParseFile("mq.yaml#redis_single")
	traceEnabled := cfg != nil && cfg.Trace.Enabled

	// stream 与 group 严格来自 mq.yaml#redis_single 的 consumer.* 段；
	// 缺省时给出兜底，防止 cfg=nil 触发空指针。
	streamName := "example-stream"
	groupName := "example-group"
	if cfg != nil {
		if len(cfg.Consumer.Topics) > 0 && cfg.Consumer.Topics[0] != "" {
			streamName = cfg.Consumer.Topics[0]
		}
		if cfg.Consumer.Group != "" {
			groupName = cfg.Consumer.Group
		}
	}

	log.Printf("[Consumer 1] 获取单机消费者 (stream=%s group=%s) 并使用 XReadGroup 阻塞拉取, traceEnabled=%v",
		streamName, groupName, traceEnabled)
	client, err := redisx.C("mq.yaml#redis_single")

	if err == nil {
		go func() {
			log.Printf("[Consumer 1] 开始监听 Stream (由于初始化已自动完成 XGroupCreateMkStream，可以直接读取)")

			// 模拟阻塞读取 3 秒
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			for {
				var streams []redis.XStream
				if traceEnabled {
					// trace=on：内部为每条消息创建子 span。
					streams, err = redisx.XReadGroupTrace(ctx, client, groupName, "worker-1",
						[]string{streamName, ">"}, 1000)
				} else {
					// trace=off：直接调 client.XReadGroup，跳过所有 span。
					streams, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
						Group:    groupName,
						Consumer: "worker-1",
						Streams:  []string{streamName, ">"},
						Block:    0,
						Count:    10,
					}).Result()
				}

				if err != nil {
					if err == redis.Nil {
						return
					}
					if err == context.DeadlineExceeded {
						log.Println("[Consumer 1] 拉取超时退出或达到设定的 3秒退出时间")
						return
					}
					log.Printf("[Consumer 1] 读取异常: %v", err)
					return
				}

				for _, stream := range streams {
					for _, msg := range stream.Messages {
						log.Printf("[Consumer 1] 收到消息 ID: %s, 载荷: %v", msg.ID, msg.Values)

						// 仅在 trace=on 时维护 consume span：
						// 从消息中提取 trace context 并创建子 span。
						var msgCtx context.Context
						if traceEnabled {
							msgCtx = extractTraceContext(ctx, msg.Values)
							var span trace.Span
							msgCtx, span = observability.StartSpan(msgCtx, "redisx.consume",
								trace.WithAttributes(
									attribute.String("stream", stream.Stream),
									attribute.String("msg_id", msg.ID),
								),
							)
							log.Printf("[Consumer 1] 消息 traceparent: %v", msg.Values["traceparent"])
							defer span.End()
						} else {
							msgCtx = ctx
						}

						if traceEnabled {
							_, err = redisx.XAckTrace(msgCtx, client, streamName, groupName, msg.ID)
						} else {
							_, err = client.XAck(msgCtx, streamName, groupName, msg.ID).Result()
						}
						if err != nil {
							log.Printf("[Consumer 1] XAck 失败: %v", err)
						} else {
							log.Printf("[Consumer 1] XAck 成功")
						}
					}
				}
			}
		}()
	} else {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	}

	log.Println("\n[Consumer 2] 模拟获取集群版 Consumer (CPC)...")
	_, err = redisx.CPC("mq.yaml#redis_cluster")
	if err == nil {
		log.Printf("[Consumer 2] 集群版消费者就绪")
	} else {
		log.Printf("[Consumer 2] 跳过集群模拟: (%v)", err)
	}
}