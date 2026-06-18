package dispatcher

import (
	"context"
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/netbirdio/netbird/shared/signal/proto"
)

type Dispatcher struct {
	peerChannels map[string]peerChan
	mu           sync.RWMutex
	ctx          context.Context
}

type DispatchRequest struct {
	Msg  *proto.EncryptedMessage
	Resp chan error
}

type peerChan struct {
	ch     chan *DispatchRequest
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
		ch:     make(chan *DispatchRequest),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (d *Dispatcher) SendMessage(ctx context.Context, msg *proto.EncryptedMessage, awaitResponse bool) (*proto.EncryptedMessage, error) {
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
		return &proto.EncryptedMessage{}, status.Errorf(codes.NotFound, "remote peer not connected")
	}

	req := &DispatchRequest{
		Msg:  msg,
		Resp: make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		return nil, status.Errorf(codes.Aborted, "context cancelled")
	case <-peerChan.ctx.Done():
		// Channel was disconnected after we took it from the map
		log.Tracef("message from peer [%s] can't be forwarded to peer [%s] because destination peer is not connected", msg.Key, msg.RemoteKey)
		return &proto.EncryptedMessage{}, status.Errorf(codes.Aborted, "remote peer disconnected")
	case peerChan.ch <- req:
		if !awaitResponse {
			return &proto.EncryptedMessage{}, nil
		}

		select {
		case <-ctx.Done():
			return nil, status.Errorf(codes.DeadlineExceeded, "timeout waiting for ack")
		case err := <-req.Resp:
			if err != nil {
				return nil, err
			}
			return &proto.EncryptedMessage{}, nil
		}
	}
}

func (d *Dispatcher) ListenForMessages(ctx context.Context, id string, messageHandler func(context.Context, *proto.EncryptedMessage) error) error {
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
			if c, ok := d.peerChannels[id]; ok && c.ch != peerChan.ch { // only delete the peerChan if it's ours
				delete(d.peerChannels, id)
			}
			d.mu.Unlock()
			log.Debugf("stream closed for peer %s", id)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case req, ok := <-peerChan.ch:
				if !ok {
					// Channel was closed, exit the goroutine
					return
				}
				if req != nil {
					err := messageHandler(ctx, req.Msg)
					select {
					case req.Resp <- err:
					default:
					}
				}
			}
		}
	}()

	return nil
}
