package importer

import (
	"context"
	"fmt"
	"kafka_export_import/internal/pkg/logger"
	"kafka_export_import/internal/pkg/mq"
	"kafka_export_import/internal/pkg/utils"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"kafka_export_import/internal/pkg/diskqueue"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	diskqueueImpl "github.com/nsqio/go-diskqueue"
)

type ImportConfig struct {
	KafkaProducer mq.KafkaProducerConf `json:"kafka_producer" yaml:"kafka_producer"` // Kafka 生产者配置
	DataPath      string               `json:"data_path" yaml:"data_path"`           // 数据存储路径
	Topics        []string             `json:"topics" yaml:"topics"`                 // data_path下需要导入的topics
	BatchDuration int                  `json:"batch_duration" yaml:"batch_duration"` // Kafka批次发送超时时间，单位秒
	BatchInterval int                  `json:"batch_interval" yaml:"batch_interval"` // Kafka批次发送间隔时间，模拟出块时间，单位毫秒
}

type importer struct {
	sync.WaitGroup
	c        *ImportConfig
	ctx      context.Context
	cancel   context.CancelFunc
	dqs      []diskqueueImpl.Interface
	dqDepth  []int64
	producer *kafka.Producer
}

func New(c *ImportConfig) (*importer, error) {
	// 验证
	if len(c.DataPath) == 0 {
		return nil, fmt.Errorf("no data_path provided")
	}
	if len(c.Topics) == 0 {
		return nil, fmt.Errorf("no topics provided")
	}

	for _, topic := range c.Topics {
		dqPath := filepath.Join(c.DataPath, topic)
		if !utils.PathExists(dqPath) {
			return nil, fmt.Errorf("topic %s doesn't exists under data_path", topic)
		}
	}

	// 验证通过，开始实例化
	producer, err := mq.NewKafkaProducer(&c.KafkaProducer)
	if err != nil {
		return nil, fmt.Errorf("mq.NewKafkaProducer failed:%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	imp := &importer{c: c, producer: producer, ctx: ctx, cancel: cancel}
	for _, topic := range c.Topics {
		dq := diskqueue.New(
			c.DataPath, topic,
		)
		imp.dqs = append(imp.dqs, dq)
		imp.dqDepth = append(imp.dqDepth, dq.Depth())
	}

	return imp, nil
}

func (i *importer) Start() error {
	i.WaitGroup.Add(1)
	go func() {
		defer i.WaitGroup.Done()
		i.workLoop()
	}()
	return nil
}

func (i *importer) workLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop() // always stop the ticker to release resources

	for {
		idx := rand.Intn(len(i.dqs))
		select {
		case data := <-i.dqs[idx].ReadChan():
			var simpleMsg mq.SimpleMessage
			err := simpleMsg.Unmarshal(data)
			if err != nil {
				logger.Errorf("simpleMsg.Unmarshal failed:%v", err)
				continue
			}

			mq.SendKafkaMessagesBatch(i.ctx, i.producer, []*kafka.Message{simpleMsg.ToKafkaMessage(i.c.Topics[idx])}, time.Duration(i.c.BatchDuration)*time.Second)

			i.dqDepth[idx] -= 1
			if i.dqDepth[idx] <= 0 {
				// close and remove dq if messages are drained
				i.dqs[idx].Close()
				i.dqs = append(i.dqs[:idx], i.dqs[idx+1:]...)
				i.dqDepth = append(i.dqDepth[:idx], i.dqDepth[idx+1:]...)
				i.c.Topics = append(i.c.Topics[:idx], i.c.Topics[idx+1:]...)
				if len(i.dqs) == 0 {
					logger.Info("all data is drained, quitting")
					syscall.Kill(os.Getpid(), syscall.SIGINT)
					return
				}
			}
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			// re-shuffle if the chosen channel has no message within 1s
			continue
		}

		time.Sleep(time.Duration(i.c.BatchInterval) * time.Millisecond)
	}
}

func (i *importer) Stop() {
	i.cancel()
	// 等workLoop退出
	i.WaitGroup.Wait()
	// 关闭剩余资源
	i.producer.Close()
	for _, dq := range i.dqs {
		err := dq.Close()
		if err != nil {
			logger.Errorf("dq.Close failed:%v", err)
		}
	}
}
