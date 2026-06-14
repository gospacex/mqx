package mqx

import (
	"fmt"
)

// ErrorCode 错误码类型
type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
	ErrConfigInvalid
	ErrConnectionFailed
	ErrTLSConfig
	ErrMessageSend
	ErrMessageConsume
	ErrMessageAck
	ErrTopicNotFound
	ErrGroupNotFound
	ErrPoolExhausted
	ErrTimeout
	ErrShutdown
	ErrDuplicateRegistration
	ErrDriverNotSupported
	ErrHotReloadFailed
	ErrObservabilityFailed
)

func (e ErrorCode) String() string {
	switch e {
	case ErrConfigInvalid:
		return "CONFIG_INVALID"
	case ErrConnectionFailed:
		return "CONNECTION_FAILED"
	case ErrTLSConfig:
		return "TLS_CONFIG_ERROR"
	case ErrMessageSend:
		return "MESSAGE_SEND_ERROR"
	case ErrMessageConsume:
		return "MESSAGE_CONSUME_ERROR"
	case ErrMessageAck:
		return "MESSAGE_ACK_ERROR"
	case ErrTopicNotFound:
		return "TOPIC_NOT_FOUND"
	case ErrGroupNotFound:
		return "GROUP_NOT_FOUND"
	case ErrPoolExhausted:
		return "POOL_EXHAUSTED"
	case ErrTimeout:
		return "TIMEOUT"
	case ErrShutdown:
		return "SHUTDOWN"
	case ErrDuplicateRegistration:
		return "DUPLICATE_REGISTRATION"
	case ErrDriverNotSupported:
		return "DRIVER_NOT_SUPPORTED"
	case ErrHotReloadFailed:
		return "HOT_RELOAD_FAILED"
	case ErrObservabilityFailed:
		return "OBSERVABILITY_FAILED"
	default:
		return "UNKNOWN"
	}
}

// MQError MQX 自定义错误类型
type MQError struct {
	Code    ErrorCode
	Message string
	Cause   error
	Driver  string
	Topic   string
	Group   string
}

func (e *MQError) Error() string {
	msg := fmt.Sprintf("[%s] %s", e.Code, e.Message)
	if e.Driver != "" {
		msg += fmt.Sprintf(" (driver=%s)", e.Driver)
	}
	if e.Topic != "" {
		msg += fmt.Sprintf(" (topic=%s)", e.Topic)
	}
	if e.Group != "" {
		msg += fmt.Sprintf(" (group=%s)", e.Group)
	}
	if e.Cause != nil {
		msg += fmt.Sprintf(": %v", e.Cause)
	}
	return msg
}

func (e *MQError) Unwrap() error {
	return e.Cause
}

// Is 支持 errors.Is 模式匹配
func (e *MQError) Is(target error) bool {
	if t, ok := target.(*MQError); ok {
		return e.Code == t.Code
	}
	return false
}

// ConfigError 配置错误
func ConfigError(msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrConfigInvalid,
		Message: msg,
		Cause:   cause,
	}
}

// ConnectionError 连接错误
func ConnectionError(driver, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrConnectionFailed,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
	}
}

// TLSError TLS 配置错误
func TLSError(msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrTLSConfig,
		Message: msg,
		Cause:   cause,
	}
}

// SendError 发送错误
func SendError(driver, topic string, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrMessageSend,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
		Topic:   topic,
	}
}

// ConsumeError 消费错误
func ConsumeError(driver, topic, group string, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrMessageConsume,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
		Topic:   topic,
		Group:   group,
	}
}

// AckError 确认错误
func AckError(driver, topic, group string, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrMessageAck,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
		Topic:   topic,
		Group:   group,
	}
}

// PoolError 连接池错误
func PoolError(driver string, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrPoolExhausted,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
	}
}

// TimeoutError 超时错误
func TimeoutError(driver, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrTimeout,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
	}
}

// ShutdownError 关闭错误
func ShutdownError(driver string, msg string, cause error) *MQError {
	return &MQError{
		Code:    ErrShutdown,
		Message: msg,
		Cause:   cause,
		Driver:  driver,
	}
}
