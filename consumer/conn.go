package consumer

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
)

func defaultConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Consumer.Offsets.AutoCommit.Enable = false
	conf.Version = sarama.V1_0_0_0
	conf.Consumer.Group.Rebalance.Timeout = 60 * time.Second // default 60s
	return conf
}

func NewConsumerGroup(addrs []string, consumer *Consumer) error {
	cg, err := sarama.NewConsumerGroup(addrs, consumer.Group, defaultConfig())
	if err != nil {
		return err
	}
	ctx := context.Background()
	return cg.Consume(ctx, []string{consumer.Topic}, consumer)
}
