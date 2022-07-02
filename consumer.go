package hermes

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

var (
	consumerReadBatchMinBytes = 10e3
	consumerReadBatchMaxBytes = 1e6
)

func SetReadBatchMinBytes(n float64) {
	consumerReadBatchMinBytes = n
}

func SetReadBatchMaxBytes(n float64) {
	consumerReadBatchMaxBytes = n
}

type Consumer func(ctx context.Context, ev *Event)

func (p *Pool) SetConsumerOnTopicAndRead(ctx context.Context, topic string, c Consumer) (chan struct{}, chan *Event) {
	conn := p.connections[topic]
	if conn == nil {
		return nil, nil
	}

	brokers, err := conn.conn.Brokers()
	if err != nil {
		log.Printf("could not get connection brokers: %v", err)
		return nil, nil
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: func(b []kafka.Broker) []string {
			ret := []string{}
			for _, broker := range b {
				ret = append(ret, broker.Host+fmt.Sprintf(":%d", broker.Port))
			}
			return ret
		}(brokers),
		GroupID:  serviceID,
		MinBytes: int(consumerReadBatchMinBytes),
		MaxBytes: int(consumerReadBatchMaxBytes),
	})

	doneChan, eventChan := make(chan struct{}, 1000), make(chan *Event, 1000)

	go readUntilCancelled(ctx, doneChan, eventChan, r, c)

	return doneChan, eventChan
}

func readUntilCancelled(
	ctx context.Context,
	doneChan chan struct{},
	eventChannel chan *Event,
	batch *kafka.Reader,
	consumer Consumer,
) {
	for range doneChan {
		for {

			n, err := batch.ReadMessage(ctx)
			if err != nil {
				break
			}
			ev := DecodeEvent(n.Value)
			consumer(ctx, ev)
		}
	}

	if err := batch.Close(); err != nil {
		log.Fatalf("unable to close batch: %v", err)
	}
}
