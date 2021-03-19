package consumer

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	Group   string
	Topic   string
	PartNum int
}

func NewConsumer(group, topic string, num int) *Consumer {
	return &Consumer{
		Group:   group,
		Topic:   topic,
		PartNum: num,
	}
}
func (s *Consumer) Setup(sarama.ConsumerGroupSession) error {
	fmt.Printf("setup called")
	return nil
}

func (s *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("Cleanup called")

	return nil
}

// mvcc、token、去重表
func (s *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("ConsumeClaim called")
	var currentOffset int64 = -1
	print(currentOffset)
	for message := range claim.Messages() {
		p, err := loadPayload(message.Value)
		if err != nil {
			sess.MarkMessage(message, "done")
			continue
		}
		// 没有标记消费完成，延迟时间到了之后会重复消费
		_, err = p.delay(delay10m) // 延迟： graceful stop
		if err != nil {            // 回退offset
			sess.MarkOffset(message.Topic, message.Partition, message.Offset, "graceful回退")
			return nil
		}
		// 判断重复
		if message.Offset <= currentOffset {
			continue
		}
		currentOffset = message.Offset
		// 转发
		ok, errDeliver := p.deliver() // 转发到消费队列
		if errDeliver != nil {
			if ok { //  错误topic 直接丢弃
				sess.MarkMessage(message, "done")
			}
			continue
		}
		// 正常消费标记完成
		sess.MarkMessage(message, "done")
		continue
	}
	return nil
}
