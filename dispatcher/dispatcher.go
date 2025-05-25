package dispatcher

import (
	"context"
	"errors"
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

func (d *Dispatcher) SendMessage(ctx context.Context, msg *proto.EncryptedMessage) (message *proto.EncryptedMessage, err error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	default:
	}

	if msg.RemoteKey == "dummy" {
		// Test message send during netbird status
		return &proto.EncryptedMessage{}, nil
	}

	d.mu.RLock()
	ch, ok := d.peerChannels[msg.RemoteKey]
	d.mu.RUnlock()

	if !ok {
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, nil
	}

	// Edge case: channel was closed after we took it from the map
	// This will lead to panic -> should recover and ensure the channel is removed from the map
	defer func() {
		if r := recover(); r != nil {
			log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
			d.mu.Lock()
			delete(d.peerChannels, msg.RemoteKey)
			d.mu.Unlock()

			message = &proto.EncryptedMessage{}
			err = nil
		}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	case ch <- msg:
		return &proto.EncryptedMessage{}, nil
	}
}

func (d *Dispatcher) ListenForMessages(ctx context.Context, id string, messageHandler func(context.Context, *proto.EncryptedMessage)) error {
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

	return nil
}
