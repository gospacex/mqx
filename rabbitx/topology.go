package rabbitx

import (
	"fmt"
	"log"

	"github.com/gospacex/mqx"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SetupTopology 自动声明交换机、队列及死信拓扑
func SetupTopology(ch *amqp.Channel, cfg *mqx.Config) error {
	if cfg.RabbitMQ == nil {
		return nil // 无专属配置时不声明
	}
	rc := cfg.RabbitMQ

	// 1. 如果启用了 DLQ，优先声明死信交换机和死信队列
	var dlxArgs amqp.Table
	if cfg.DLQ.Enabled {
		dlxName := rc.DLXExchange
		if dlxName == "" {
			dlxName = "dlx." + rc.Exchange // 默认兜底名
		}
		dlqName := cfg.DLQ.Topic
		if dlqName == "" {
			dlqName = "dlq." + rc.Queue // 默认兜底名
		}

		err := ch.ExchangeDeclare(
			dlxName,
			"direct", // 死信一般用 direct 即可
			true,     // durable
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("declare dlx exchange: %w", err)
		}

		_, err = ch.QueueDeclare(
			dlqName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("declare dlq queue: %w", err)
		}

		err = ch.QueueBind(dlqName, rc.DLXRoutingKey, dlxName, false, nil)
		if err != nil {
			return fmt.Errorf("bind dlq queue: %w", err)
		}

		// 准备给主队列挂载死信参数
		dlxArgs = amqp.Table{
			"x-dead-letter-exchange":    dlxName,
			"x-dead-letter-routing-key": rc.DLXRoutingKey,
		}
		log.Printf("[rabbitx] DLX topology declared [DLX=%s, DLQ=%s]", dlxName, dlqName)
	}

	// 2. 声明主交换机
	if rc.Exchange != "" {
		err := ch.ExchangeDeclare(
			rc.Exchange,
			rc.ExchangeType,
			rc.Durable,
			rc.AutoDelete,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("declare main exchange: %w", err)
		}
	}

	// 3. 声明主队列并绑定
	if rc.Queue != "" {
		_, err := ch.QueueDeclare(
			rc.Queue,
			rc.Durable,
			rc.AutoDelete,
			false,
			false,
			dlxArgs, // 将 DLX 参数挂载到主队列上
		)
		if err != nil {
			return fmt.Errorf("declare main queue: %w", err)
		}

		if rc.Exchange != "" {
			err = ch.QueueBind(rc.Queue, rc.RoutingKey, rc.Exchange, false, nil)
			if err != nil {
				return fmt.Errorf("bind main queue: %w", err)
			}
		}
		log.Printf("[rabbitx] Main topology declared [Exchange=%s, Queue=%s]", rc.Exchange, rc.Queue)
	}

	// 4. 设置 QoS (背压)
	if rc.Prefetch > 0 {
		if err := ch.Qos(rc.Prefetch, 0, false); err != nil {
			return fmt.Errorf("set qos prefetch: %w", err)
		}
	}

	// 5. 设置 Publisher Confirm (可靠投递)
	if rc.Confirm {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("set publisher confirm: %w", err)
		}
	}

	return nil
}
