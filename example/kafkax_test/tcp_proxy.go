package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

const (
	ApiKeyProduce = 0
	ApiKeyFetch   = 1
)

// KafkaProxy TCP 代理服务器，拦截 Kafka 流量并注入/提取 trace headers
type KafkaProxy struct {
	listenAddr string
	targetAddr string
	propagator propagation.TextMapPropagator
	listener   net.Listener
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewKafkaProxy(listenAddr, targetAddr string) *KafkaProxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &KafkaProxy{
		listenAddr: listenAddr,
		targetAddr: targetAddr,
		propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (p *KafkaProxy) Start() error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.listenAddr, err)
	}
	p.listener = ln

	log.Printf("[KafkaProxy] 启动代理: %s -> %s", p.listenAddr, p.targetAddr)

	p.wg.Add(1)
	go p.acceptLoop()
	return nil
}

func (p *KafkaProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return
			default:
				log.Printf("[KafkaProxy] Accept error: %v", err)
				continue
			}
		}
		p.wg.Add(1)
		go p.handleConnection(conn)
	}
}

func (p *KafkaProxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr().String()
	log.Printf("[KafkaProxy] 收到连接 from %s,正在连接目标 %s", clientAddr, p.targetAddr)

	targetConn, err := net.DialTimeout("tcp", p.targetAddr, 5*time.Second)
	if err != nil {
		log.Printf("[KafkaProxy] 连接目标 Kafka失败: %v", err)
		return
	}
	defer targetConn.Close()

	log.Printf("[KafkaProxy] 已建立连接: %s -> %s", clientAddr, p.targetAddr)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		log.Printf("[KafkaProxy] goroutine: copyWithTraceInjection (%s)", clientAddr)
		p.copyWithTraceInjection(clientConn, targetConn, clientAddr)
	}()
	go func() {
		defer wg.Done()
		log.Printf("[KafkaProxy] goroutine: copyWithTraceExtraction (%s)", clientAddr)
		p.copyWithTraceExtraction(targetConn, clientConn, clientAddr)
	}()
	wg.Wait()
}

func (p *KafkaProxy) copyWithTraceInjection(dst, src net.Conn, clientAddr string) {
	buf := make([]byte, 32*1024)
	for {
		src.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := src.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("[KafkaProxy][%s] Read error (produce): %v", clientAddr, err)
			}
			return
		}

		// 记录原始请求类型和前16字节hex
		if n >= 6 {
			apiKey := binary.BigEndian.Uint16(buf[4:6])
			hexDump := fmt.Sprintf("%x", buf[:min(16, n)])
			log.Printf("[KafkaProxy][%s] copyWithTraceInjection: 收到客户端请求, apiKey=%d, size=%d, hex=%s", clientAddr, apiKey, n, hexDump)
		}

		modified := p.injectTraceHeaders(buf[:n])
		if _, err := dst.Write(modified); err != nil {
			log.Printf("[KafkaProxy][%s] Write error (produce): %v", clientAddr, err)
			return
		}
	}
}

func (p *KafkaProxy) copyWithTraceExtraction(dst, src net.Conn, clientAddr string) {
	buf := make([]byte, 32*1024)
	for {
		src.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := src.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("[KafkaProxy][%s] Read error (fetch): %v", clientAddr, err)
			}
			return
		}

		log.Printf("[KafkaProxy][%s] 收到 Kafka响应: %d bytes", clientAddr, n)

		modified := p.extractTraceHeaders(buf[:n])
		if _, err := dst.Write(modified); err != nil {
			log.Printf("[KafkaProxy][%s] Write error (fetch): %v", clientAddr, err)
			return
		}
	}
}

// injectTraceHeaders 向 Kafka Produce消息注入 trace context
func (p *KafkaProxy) injectTraceHeaders(data []byte) []byte {
	if len(data) < 6 {
		return data
	}

	offset := 0
	binary.BigEndian.Uint32(data[offset:]) // Message size
	offset += 4

	apiKey := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Fetch 请求直通不处理
	if apiKey == ApiKeyFetch {
		log.Printf("[KafkaProxy] injectTraceHeaders: Fetch 请求, 直通 (size=%d)", len(data))
		return data
	}

	// 非 Produce 请求也直通
	if apiKey != ApiKeyProduce {
		log.Printf("[KafkaProxy] injectTraceHeaders: apiKey=%d, 非 Produce 请求, 直通", apiKey)
		return data
	}

	apiVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	correlationID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Client ID
	clientIDLen := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if clientIDLen > 0 {
		offset += int(clientIDLen)
	}

	log.Printf("[KafkaProxy] 收到 Produce 请求: apiVersion=%d, correlationID=%d", apiVersion, correlationID)

	// Create trace span for produce operation
	// 始终通过 otel.Tracer(...) 获取 tracer：未配置 SDK 时它会返回 no-op 实现，
	// 不会因全局变量未初始化而触发 nil pointer dereference。
	ctx, span := otel.Tracer("kafkax").Start(context.Background(), "kafka.produce_proxy")
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.operation", "publish"),
		attribute.String("messaging.kafka.correlation_id", fmt.Sprintf("%d", correlationID)),
	)

	// Inject trace context to carrier
	carrier := make(propagation.HeaderCarrier)
	p.propagator.Inject(ctx, carrier)

	traceparent := carrier.Get("traceparent")
	tracestate := carrier.Get("tracestate")

	if traceparent == "" {
		log.Printf("[KafkaProxy] 未生成 traceparent，跳过注入")
		return data
	}

	// Parse record batch and inject headers
	result := p.modifyProduceRequest(data, apiVersion, traceparent, tracestate)
	log.Printf("[KafkaProxy] kafka.produce_proxy注入 trace 成功 (traceparent=%s)", traceparent[:min(20, len(traceparent))])

	return result
}

// modifyProduceRequest 解析 Produce 请求并注入 trace headers 到每个消息
func (p *KafkaProxy) modifyProduceRequest(data []byte, apiVersion int, traceparent, tracestate string) []byte {
	var result bytes.Buffer
	offset := 0

	// 写入原始 header 部分
	headerSize := 4 + 2 + 2 + 4 // size + api_key + api_version + correlation_id
	result.Write(data[offset:headerSize])
	offset += headerSize

	// Client ID
	clientIDLen := binary.BigEndian.Uint16(data[offset : offset+2])
	result.Write(data[offset : offset+2])
	offset += 2
	if int(clientIDLen) > 0 {
		result.Write(data[offset : offset+int(clientIDLen)])
		offset += int(clientIDLen)
	}

	// RequiredAcks + Timeout
	result.Write(data[offset : offset+6])
	offset += 6

	// TopicData array
	numTopics := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	result.Write(data[offset : offset+2])
	offset += 2

	for t := 0; t < numTopics; t++ {
		// Topic name
		topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		result.Write(data[offset : offset+2])
		offset += 2
		result.Write(data[offset : offset+topicLen])
		offset += topicLen

		// PartitionData array
		numPartitions := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		result.Write(data[offset : offset+4])
		offset += 4

		for partIdx := 0; partIdx < numPartitions; partIdx++ {
			binary.BigEndian.Uint32(data[offset : offset+4]) // partition
			result.Write(data[offset : offset+4])
			offset += 4

			// Record batch size
			batchSize := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			result.Write(data[offset : offset+4])
			offset += 4

			batchStart := offset
			offset += batchSize

			// 解析 batch 中的 records 并注入 trace headers
			if batchSize > 0 {
				modifiedBatch := p.injectTraceToRecordBatch(data[batchStart:batchStart+batchSize], apiVersion, traceparent, tracestate)
				result.Write(modifiedBatch)
			}
		}
	}

	return result.Bytes()
}

// injectTraceToRecordBatch 向 record batch 中的每条消息注入 trace headers
func (p *KafkaProxy) injectTraceToRecordBatch(batch []byte, apiVersion int, traceparent, tracestate string) []byte {
	if len(batch) < 61 {
		return batch
	}

	var result bytes.Buffer
	offset := 0

	// Record batch header (61 bytes base)
	result.Write(batch[offset : offset+61])
	offset += 61

	// Records count
	recordsCount := int(binary.BigEndian.Uint32(batch[offset : offset+4]))
	result.Write(batch[offset : offset+4])
	offset += 4

	// Parse each record and inject trace header
	for i := 0; i < recordsCount && offset < len(batch); i++ {
		recordStart := offset

		// Read record attributes (1 byte)
		attributes := batch[offset]
		result.WriteByte(attributes)
		offset++

		binary.BigEndian.Uint32(batch[offset : offset+4]) // timestampDelta
		result.Write(batch[offset : offset+4])
		offset += 4

		binary.BigEndian.Uint32(batch[offset : offset+4]) // offsetDelta
		result.Write(batch[offset : offset+4])
		offset += 4

		// Key length
		keyLen := int(binary.BigEndian.Uint32(batch[offset : offset+4]))
		result.Write(batch[offset : offset+4])
		offset += 4
		if keyLen > 0 {
			result.Write(batch[offset : offset+keyLen])
			offset += keyLen
		}

		// Value length
		valueLen := int(binary.BigEndian.Uint32(batch[offset : offset+4]))
		result.Write(batch[offset : offset+4])
		offset += 4
		if valueLen > 0 {
			result.Write(batch[offset : offset+valueLen])
			offset += valueLen
		}

		// Headers count (v1+ only)
		headersCount := 0
		if apiVersion >= 1 {
			headersCount = int(binary.BigEndian.Uint32(batch[offset : offset+4]))
			result.Write(batch[offset : offset+4])
			offset += 4
		}

		// Write existing headers
		for h := 0; h < headersCount; h++ {
			headerKeyLen := int(binary.BigEndian.Uint32(batch[offset : offset+4]))
			result.Write(batch[offset : offset+4])
			offset += 4
			result.Write(batch[offset : offset+headerKeyLen])
			offset += headerKeyLen
			headerValueLen := int(binary.BigEndian.Uint32(batch[offset : offset+4]))
			result.Write(batch[offset : offset+4])
			offset += 4
			result.Write(batch[offset : offset+headerValueLen])
			offset += headerValueLen
		}

		// Inject trace headers at the beginning
		traceparentHeader := []byte("traceparent")
		traceparentValue := []byte(traceparent)
		tracestateHeader := []byte("tracestate")
		tracestateValue := []byte(tracestate)

		// New headers count = existing + 2
		newHeadersCount := uint32(headersCount + 2)
		if apiVersion >= 1 {
			binary.Write(&result, binary.BigEndian, newHeadersCount)
		}

		// Write traceparent header
		binary.Write(&result, binary.BigEndian, uint32(len(traceparentHeader)))
		result.Write(traceparentHeader)
		binary.Write(&result, binary.BigEndian, uint32(len(traceparentValue)))
		result.Write(traceparentValue)

		// Write tracestate header
		binary.Write(&result, binary.BigEndian, uint32(len(tracestateHeader)))
		result.Write(tracestateHeader)
		binary.Write(&result, binary.BigEndian, uint32(len(tracestateValue)))
		result.Write(tracestateValue)

		// Write original record data
		recordEnd := offset
		result.Write(batch[recordStart:recordEnd])
	}

	return result.Bytes()
}

// extractTraceHeaders 从 Kafka Fetch Response 中提取 trace context
func (p *KafkaProxy) extractTraceHeaders(data []byte) []byte {
	if len(data) < 6 {
		return data
	}

	offset := 0
	messageSize := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	apiKey := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	log.Printf("[KafkaProxy] extractTraceHeaders: apiKey=%d, dataLen=%d, msgSize=%d", apiKey, len(data), messageSize)

	// ApiVersion 响应直通不处理
	if apiKey == 18 {
		log.Printf("[KafkaProxy] extractTraceHeaders: ApiVersion 响应, 直通")
		return data
	}

	if apiKey != ApiKeyFetch {
		log.Printf("[KafkaProxy] extractTraceHeaders: 非 Fetch 响应, apiKey=%d, 返回原始数据", apiKey)
		return data
	}

	apiVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	correlationID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	log.Printf("[KafkaProxy] extractTraceHeaders: Fetch Response, apiVersion=%d, correlationID=%d, msgSize=%d", apiVersion, correlationID, messageSize)

	// 从 Fetch Response 中解析消息并提取 trace headers
	traceparent := p.extractTraceFromFetchResponse(data, apiVersion)

	log.Printf("[KafkaProxy] extractTraceHeaders: traceparent found: %v, value=%s", traceparent != "", traceparent)

	// 先提取 trace context，再用它作为父上下文创建 span
	ctx := context.Background()
	if traceparent != "" {
		// 使用真实的 traceparent 创建 carrier 并提取
		carrier := make(propagation.HeaderCarrier)
		carrier.Set("traceparent", traceparent)

		ctx = p.propagator.Extract(ctx, carrier)
		log.Printf("[KafkaProxy] kafka.consume_proxy 提取 trace context成功 (traceparent=%s)", traceparent[:min(20, len(traceparent))])
	}

	// 使用正确的父上下文创建 span
	// 始终通过 otel.Tracer(...) 获取 tracer：未配置 SDK 时它会返回 no-op 实现，
	// 不会因全局变量未初始化而触发 nil pointer dereference。
	_, span := otel.Tracer("kafkax").Start(ctx, "kafka.consume_proxy")
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.operation", "consume"),
		attribute.String("messaging.kafka.correlation_id", fmt.Sprintf("%d", correlationID)),
		attribute.Int("messaging.kafka.api_version", apiVersion),
	)

	if traceparent != "" {
		span.SetAttributes(
			attribute.String("messaging.kafka.traceparent", traceparent),
		)
	}

	return data
}

func (p *KafkaProxy) extractTraceFromFetchResponse(data []byte, apiVersion int) string {
	offset := 0
	binary.BigEndian.Uint32(data[offset:]) // Message size
	offset += 4

	apiKey := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	if apiKey != ApiKeyFetch {
		return ""
	}

	fetchApiVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	binary.BigEndian.Uint32(data[offset : offset+4]) // Correlation ID
	offset += 4

	// Throttle time (4 bytes) if apiVersion >= 1
	if fetchApiVersion >= 1 {
		offset += 4
	}

	// Session ID (if apiVersion >= 2)
	if fetchApiVersion >= 2 {
		if offset+2 > len(data) {
			return ""
		}
		sessionIDLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2 + sessionIDLen
	}

	// Num Topics
	if offset+2 > len(data) {
		return ""
	}
	numTopics := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	for t := 0; t < numTopics && offset < len(data); t++ {
		// Topic name
		if offset+2 > len(data) {
			break
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2 + topicLen

		// Num partitions
		if offset+4 > len(data) {
			break
		}
		numPartitions := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		for p := 0; p < numPartitions && offset < len(data); p++ {
			offset += 4 // partition
			offset += 2 // error code (2 bytes in Fetch response)

			// Fetch offset, log start offset, etc.
			if fetchApiVersion >= 5 {
				offset += 8 // committed offset
			}
			offset += 8 // fetch offset
			if fetchApiVersion >= 9 {
				offset += 8 // last stable offset
			}
			if fetchApiVersion >= 4 {
				offset += 8 // log start offset
			}

			// Record batch count
			if offset+4 > len(data) {
				break
			}
			batchCount := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			for b := 0; b < batchCount && offset < len(data); b++ {
				// Record batch header (61 bytes base)
				if offset+61 > len(data) {
					break
				}
				offset += 61

				// Records count
				if offset+4 > len(data) {
					break
				}
				recordsCount := int(binary.BigEndian.Uint32(data[offset : offset+4]))
				offset += 4

				for r := 0; r < recordsCount && offset < len(data); r++ {
					// Skip record fields to find headers
					if offset+1 > len(data) {
						break
					}
					offset++ // attributes
					if offset+4 > len(data) {
						break
					}
					offset += 4 // timestamp delta
					if offset+4 > len(data) {
						break
					}
					offset += 4 // offset delta

					// Key length
					if offset+4 > len(data) {
						break
					}
					keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
					offset += 4
					if keyLen > 0 {
						offset += keyLen
					}

					// Value length
					if offset+4 > len(data) {
						break
					}
					valueLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
					offset += 4
					if valueLen > 0 {
						offset += valueLen
					}

					// Headers (v1+ only)
					if fetchApiVersion >= 1 {
						if offset+4 > len(data) {
							break
						}
						headersCount := int(binary.BigEndian.Uint32(data[offset : offset+4]))
						offset += 4

						for h := 0; h < headersCount && offset < len(data); h++ {
							if offset+4 > len(data) {
								break
							}
							headerKeyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
							offset += 4
							if offset+headerKeyLen > len(data) {
								break
							}
							headerKey := string(data[offset : offset+headerKeyLen])
							offset += headerKeyLen

							if offset+4 > len(data) {
								break
							}
							headerValueLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
							offset += 4
							if offset+headerValueLen > len(data) {
								break
							}
							headerValue := data[offset : offset+headerValueLen]
							offset += headerValueLen

							if headerKey == "traceparent" {
								return string(headerValue)
							}
						}
					}
				}
			}
		}
	}

	return ""
}

func (p *KafkaProxy) Shutdown(ctx context.Context) error {
	p.cancel()
	if p.listener != nil {
		p.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[KafkaProxy] 代理已关闭")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func RunKafkaProxyTest() {
	log.Println("=== [Kafka Proxy] TCP 代理模式测试 ===")

	proxy := NewKafkaProxy("127.0.0.1:9094", "127.0.0.1:9092")
	if err := proxy.Start(); err != nil {
		log.Printf("[Kafka Proxy] 启动失败: %v", err)
		return
	}

	log.Println("[Kafka Proxy] 代理已启动，等待连接...")

	<-proxy.ctx.Done()
}