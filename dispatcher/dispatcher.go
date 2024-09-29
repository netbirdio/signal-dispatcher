package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"

	"github.com/netbirdio/netbird/signal/proto"
)

type Dispatcher struct {
	peerChannels map[string]chan *proto.EncryptedMessage
	mu           sync.RWMutex
	ctx          context.Context
}

func NewDispatcher(ctx context.Context, meter metric.Meter) (*Dispatcher, error) {
	return &Dispatcher{
		peerChannels: make(map[string]chan *proto.EncryptedMessage),
		ctx:          ctx,
	}, nil
}

func (d *Dispatcher) SendMessage(ctx context.Context, msg *proto.EncryptedMessage) (*proto.EncryptedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	default:
		// Continue only if the context is still active
	}

	d.mu.RLock()
	ch, ok := d.peerChannels[msg.RemoteKey]
	d.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("peer %s not connected", msg.RemoteKey)
	}

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	case ch <- msg:
		return &proto.EncryptedMessage{}, nil
	}
}

func (d *Dispatcher) ListenForMessages(ctx context.Context, id string, messageHandler func(context.Context, *proto.EncryptedMessage)) {
	ch := make(chan *proto.EncryptedMessage)

	d.mu.Lock()
	d.peerChannels[id] = ch
	d.mu.Unlock()

	go func() {
		defer func() {
			d.mu.Lock()
			close(ch)
			delete(d.peerChannels, id)
			d.mu.Unlock()
			log.Debugf("stream closed for peer %s", id)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					// Channel was closed, exit the goroutine
					return
				}
				if msg != nil {
					messageHandler(ctx, msg)
				}
			}
		}
	}()
}
