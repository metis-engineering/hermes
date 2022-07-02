package hermes

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Connection struct {
	mu         sync.Mutex
	conn       *kafka.Conn
	middleware []Middleware
}

func (c *Connection) SetMiddleware(mw ...Middleware) {
	c.middleware = append(c.middleware, mw...)
}

type Pool struct {
	connections map[string]*Connection
	cfg         *ConnectionConfig
}

func (p *Pool) GetConnection(topic string) *Connection {
	if conn, ok := p.connections[topic]; ok {
		return conn
	}
	return nil
}

type ConnectionConfig struct {
	Protocol  string
	Address   string
	Partition int
	Topics    []string
}

func initConnection(ctx context.Context, pool *Pool, partition int, protocol, address, topic string) error {
	conn, err := kafka.DialLeader(ctx, protocol, address, topic, partition)
	if err != nil {
		return err
	}

	if _, ok := pool.connections[topic]; ok {
		return nil
	}

	pool.connections[topic] = &Connection{conn: conn}

	return nil
}

func Connect(ctx context.Context, cfg *ConnectionConfig) (*Pool, error) {
	if len(cfg.Topics) == 0 {
		return nil, ErrorNoTopicsSet
	}

	pool := &Pool{
		cfg: cfg,
	}

	for _, t := range cfg.Topics {
		err := initConnection(ctx, pool, cfg.Partition, cfg.Protocol, cfg.Address, t)
		if err != nil {
			return nil, err
		}
	}

	return pool, nil
}
