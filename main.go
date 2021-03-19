package main

import (
	"time"

	"github.com/asppj/delay-queue/consumer"

	"github.com/asppj/delay-queue/conf"
)

func init() {
	err := conf.LoadConfFile()
	if err != nil {
		panic(err)
	}
}

// go run
func main() {
	k := conf.Get().Kafka
	err := consumer.NewConsumerGroup(k.Addrs, consumer.NewConsumer(k.Group.Delay, k.Topic.Delay10m, k.PartNum))
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Hour)
}
