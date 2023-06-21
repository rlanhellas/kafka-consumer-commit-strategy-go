package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	strategyAutoCommitAutoStoreOffset   = "AUTOCOMMIT_AUTOSTORE"
	strategyAutoCommitManualStoreOffset = "AUTOCOMMIT_MANUALSTORE"
	strategyManualCommitAutoStoreOffset = "MANUALCOMMIT_AUTOSTORE"
)

const (
	topic   = "perfv2-sample3"
	qtd     = 10000
	produce = true
)

func main() {

	if produce {
		produceNow()
	}

	//mude aqui a estratégia
	currentStrategy := strategyManualCommitAutoStoreOffset

	kafkaCfg := kafka.ConfigMap{
		"bootstrap.servers":       "localhost",
		"group.id":                "lanhellas",
		"auto.offset.reset":       "earliest",
		"auto.commit.interval.ms": "5000",
	}

	if currentStrategy == strategyAutoCommitAutoStoreOffset {
		kafkaCfg["enable.auto.commit"] = true
		kafkaCfg["enable.auto.offset.store"] = true
	} else if currentStrategy == strategyManualCommitAutoStoreOffset {
		kafkaCfg["enable.auto.commit"] = false
		kafkaCfg["enable.auto.offset.store"] = true
	} else if currentStrategy == strategyAutoCommitManualStoreOffset {
		kafkaCfg["enable.auto.commit"] = true
		kafkaCfg["enable.auto.offset.store"] = false
	}

	c, err := kafka.NewConsumer(&kafkaCfg)

	if err != nil {
		panic(err)
	}

	_ = c.SubscribeTopics([]string{topic}, nil)

	start := time.Now()
	for i := 0; i < qtd; i++ {
		msg, err := c.ReadMessage(time.Second * 10)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			//fakeProcess()
			commit(c, msg, currentStrategy)
		}
	}
	elapsed := time.Since(start)
	log.Printf("took: %s \n", elapsed)

	_ = c.Close()
}

func produceNow() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Produce messages to topic (asynchronously)
	topic := topic
	for i := 0; i < qtd; i++ {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("test"),
		}, nil)

	}
	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

}

func commit(c *kafka.Consumer, msg *kafka.Message, strategy string) {
	var err error
	if strategy == strategyManualCommitAutoStoreOffset {
		_, err = c.Commit()
	} else if strategy == strategyAutoCommitManualStoreOffset {
		_, err = c.StoreMessage(msg)
	}

	if err != nil {
		panic(err)
	}
}

func fakeProcess() {
	time.Sleep(15 * time.Second)
	log.Println("process finished")
}
