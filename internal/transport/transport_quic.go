package transport

import (
	"context"
	"crypto/tls"
	"sync"

	"github.com/lucas-clemente/quic-go"
)

type quicServerTransport struct {
	addr     string
	acceptor ServerTransportAcceptor
	once     sync.Once
	tlsConf  *tls.Config
	listener quic.Listener
}

func (p *quicServerTransport) Close() (err error) {
	if p.listener == nil {
		return
	}
	p.once.Do(func() {
		err = p.listener.Close()
	})
	return
}

func (p *quicServerTransport) Accept(acceptor ServerTransportAcceptor) {
	p.acceptor = acceptor
}

func (p *quicServerTransport) Listen(ctx context.Context, notifier chan<- struct{}) error {
	listener, err := quic.ListenAddr(p.addr, p.tlsConf, nil)
	if err != nil {
		return err
	}
	p.listener = listener
	notifier <- struct{}{}
	for {
		session, err := listener.Accept(ctx)
		if err != nil {
			return err
		}
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			return err
		}
		tp := newTransportClient(newQuicRConnection(session, stream))
		go func(ctx context.Context, tp *Transport) {
			p.acceptor(ctx, tp)
		}(ctx, tp)
	}
}

func newQuicServerTransport(addr string, tlsConf *tls.Config) *quicServerTransport {
	return &quicServerTransport{
		addr:    addr,
		tlsConf: tlsConf,
	}
}

func newQuicClientTransport(addr string, tlsConf *tls.Config) (tp *Transport, err error) {
	session, err := quic.DialAddr(addr, tlsConf, nil)
	if err != nil {
		return
	}
	stream, err := session.OpenStream()
	if err != nil {
		return
	}
	tp = newTransportClient(newQuicRConnection(session, stream))
	return
}
