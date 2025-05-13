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

func (d *Dispatcher) SendMessage(ctx context.Context, msg *proto.EncryptedMessage) (*proto.EncryptedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	default:
		// Continue only if the context is still active
	}

	if msg.RemoteKey == "dummy" {
		// Test message send during netbird status
		return &proto.EncryptedMessage{}, nil
	}

	log.Tracef("message [%s] -> [%s], trying to get channel", msg.Key, msg.RemoteKey)
	d.mu.RLock()
	ch, ok := d.peerChannels[msg.RemoteKey]
	d.mu.RUnlock()

	log.Tracef("message [%s] -> [%s], trying got channel, with result [%v]", msg.Key, msg.RemoteKey, ok)

	if !ok {
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, nil
	}

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	default:
		var recovered bool
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Warnf("recovered from panic when sending message to peer [%s]: %v", msg.RemoteKey, r)
					d.mu.Lock()
					delete(d.peerChannels, msg.RemoteKey)
					d.mu.Unlock()
					log.Tracef("removed channel for [%s]", msg.RemoteKey)
					recovered = true
				}
			}()
			ch <- msg
		}()

		if recovered {
			log.Debugf("channel was closed for peer [%s], message not delivered", msg.RemoteKey)
		}
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
			log.Tracef("started closing stream for %s", id)
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
