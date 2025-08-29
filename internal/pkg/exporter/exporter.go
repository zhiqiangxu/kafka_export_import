package exporter

import (
	"context"
	"fmt"
	"kafka_export_import/internal/pkg/logger"
	"kafka_export_import/internal/pkg/mq"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"kafka_export_import/internal/pkg/diskqueue"

	kafkaImpl "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	diskqueueImpl "github.com/nsqio/go-diskqueue"
)

type ExporterConfig struct {
	KafkaConsumer mq.KafkaConsumerConf `json:"kafka" yaml:"kafka"`         // Kafka 消费者配置
	DataPath      string               `json:"data_path" yaml:"data_path"` // data_path 数据存储路径
	MaxMsg        uint64               `json:"max_msg" yaml:"max_msg"`     // 最大导出的消息数量
	Readonly      bool                 `json:"read_only" yaml:"read_only"` // 是否只读
}

type exporter struct {
	c        *ExporterConfig
	consumer *mq.KafkaConsumer
	ctx      context.Context
	cancel   context.CancelFunc
	dq       diskqueueImpl.Interface
	msgCount atomic.Uint64
}

func New(c *ExporterConfig) (*exporter, error) {

	var dq diskqueueImpl.Interface
	if !c.Readonly {
		// 初始化diskqueue
		dqPath := filepath.Join(c.DataPath, c.KafkaConsumer.Topic)
		err := os.MkdirAll(dqPath, 0777)
		if err != nil {
			return nil, fmt.Errorf("os.MkdirAll failed:%v", err)
		}
		dq = diskqueue.New(
			c.DataPath, c.KafkaConsumer.Topic,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ex := &exporter{c: c, ctx: ctx, cancel: cancel, dq: dq}
	consumer, err := mq.NewKafkaConsumer(&c.KafkaConsumer, ex.handle)
	if err != nil {
		return nil, fmt.Errorf("kafka.NewConsumer failed:%v", err)
	}
	ex.consumer = consumer

	return ex, nil
}

func (e *exporter) Start() error {
	return e.consumer.Start(e.ctx)
}

func (e *exporter) handle(msg *kafkaImpl.Message) {
	if e.c.Readonly {
		logger.Info("readonly mode, skipping message")
		return
	}
	if e.c.MaxMsg > 0 {
		if e.msgCount.Load() >= e.c.MaxMsg {
			return
		}
	}
	simpleMsg := mq.SimpleMessage{Partition: msg.TopicPartition.Partition, Data: msg.Value}
	msgBytes, err := simpleMsg.Marshal()
	if err != nil {
		logger.Errorf("simpleMsg.Marshal failed:%v", err)
		return
	}
	err = e.dq.Put(msgBytes)
	if err != nil {
		logger.Errorf("dq.Put failed:%v", err)
		return
	}
	if e.c.MaxMsg > 0 {
		msgCount := e.msgCount.Add(1)
		if msgCount >= e.c.MaxMsg {
			logger.Info("max_msg reached, quitting")
			syscall.Kill(os.Getpid(), syscall.SIGINT)
		}
	}

}

func (e *exporter) Stop() {
	e.cancel()
	e.consumer.Stop()
	err := e.dq.Close()
	if err != nil {
		logger.Errorf("dq.Close failed:%v", err)
	}
}
