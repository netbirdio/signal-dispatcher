package dispatcher

import (
	"context"
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"

	"github.com/netbirdio/netbird/shared/signal/proto"
)

type Dispatcher struct {
	peerChannels map[string]peerChan
	mu           sync.RWMutex
	ctx          context.Context
}

type peerChan struct {
	ch     chan *proto.EncryptedMessage
	ctx    context.Context
	cancel context.CancelFunc
}

func NewDispatcher(ctx context.Context, meter metric.Meter) (*Dispatcher, error) {
	return &Dispatcher{
		peerChannels: make(map[string]peerChan),
		ctx:          ctx,
	}, nil
}

func newPeerChan(parentCtx context.Context) peerChan {
	ctx, cancel := context.WithCancel(parentCtx)
	return peerChan{
		ch:     make(chan *proto.EncryptedMessage),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (d *Dispatcher) SendMessage(ctx context.Context, msg *proto.EncryptedMessage) (*proto.EncryptedMessage, error) {
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
	peerChan, ok := d.peerChannels[msg.RemoteKey]
	d.mu.RUnlock()

	if !ok {
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, nil
	}

	select {
	case <-ctx.Done():
		return nil, errors.New("context cancelled")
	case <-peerChan.ctx.Done():
		// Channel was disconnected after we took it from the map
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, nil
	case peerChan.ch <- msg:
		return &proto.EncryptedMessage{}, nil
	}
}

func (d *Dispatcher) ListenForMessages(ctx context.Context, id string, messageHandler func(context.Context, *proto.EncryptedMessage)) error {
	peerChan := newPeerChan(ctx)

	d.mu.Lock()
	d.peerChannels[id] = peerChan
	d.mu.Unlock()

	go func() {
		defer func() {
			d.mu.Lock()
			// Cancel the context in case if different goroutine already took channel from the map
			// but didn't sent to it yet
			peerChan.cancel()
			close(peerChan.ch)
			delete(d.peerChannels, id)
			d.mu.Unlock()
			log.Debugf("stream closed for peer %s", id)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-peerChan.ch:
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
