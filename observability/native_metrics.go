package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// NativeQueueLength 抓取底层驱动的等待队列长度
	NativeQueueLength = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mqx_native_queue_length",
			Help: "Current number of messages in the native driver queue",
		},
		[]string{"driver", "instance_id"},
	)

	// NativeInFlight 抓取底层驱动的正在网络中传输 (In-Flight) 的请求数
	NativeInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mqx_native_in_flight_requests",
			Help: "Current number of in-flight requests in the native driver",
		},
		[]string{"driver", "instance_id"},
	)

	// NativeRedisPoolStats 抓取 Redis 底层连接池的核心指标
	NativeRedisPoolStats = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mqx_native_redis_pool_stats",
			Help: "Current status of go-redis internal connection pool",
		},
		[]string{"driver", "instance_id", "stat_type"}, // stat_type: total, idle, stale, hits, misses, timeouts
	)

	// NativeNatsStats 抓取 NATS 底层连接的统计信息
	NativeNatsStats = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mqx_native_nats_stats",
			Help: "Current status of NATS connection (in_msgs, out_msgs, reconnects)",
		},
		[]string{"driver", "instance_id", "stat_type"},
	)
)
