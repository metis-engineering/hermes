package hermes

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	eventChannel chan *Event
}

func (p *Producer) EventChannel() chan *Event {
	return p.eventChannel
}

func NewProducer() *Producer {
	return &Producer{
		eventChannel: make(chan *Event, 1000),
	}
}

func (p *Pool) NewProducerOnTopic(ctx context.Context, topic string) *Producer {
	prod := &Producer{
		eventChannel: make(chan *Event, 1000),
	}

	go p.SetProducerOnTopic(ctx, topic, prod)

	return prod
}

func (p *Pool) SetProducerOnTopic(ctx context.Context, topic string, prod *Producer) {
	conn := p.connections[topic]
	if conn == nil {
		return
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	for msg := range prod.EventChannel() {

		for _, mw := range conn.middleware {
			msg = mw(msg)
		}

		enc, err := msg.Encode()
		if err != nil {
			log.Printf("failed to encode message: %v", msg)
			continue
		}
		conn.conn.WriteMessages(kafka.Message{Value: enc})
	}

}
