package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ProduceSuccessTotal 记录发送成功的消息数
	ProduceSuccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mqx_produce_success_total",
			Help: "Total number of successfully produced MQ messages",
		},
		[]string{"driver", "topic", "instance_id"},
	)

	// ProduceErrorsTotal 记录发送失败的消息数
	ProduceErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mqx_produce_errors_total",
			Help: "Total number of failed MQ message productions",
		},
		[]string{"driver", "topic", "instance_id"},
	)
)
