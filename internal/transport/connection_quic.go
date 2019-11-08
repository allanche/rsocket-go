package transport

import (
	"bufio"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/rsocket/rsocket-go/internal/framing"
)

type quicConn struct {
	counter *Counter
	session quic.Session
	stream  quic.Stream
	writer  *bufio.Writer
	decoder *LengthBasedFrameDecoder
}

func (p *quicConn) Close() (err error) {
	err = p.stream.Close()
	if err != nil {
		_ = p.session.Close()
	} else {
		err = p.session.Close()
	}
	return
}

func (p *quicConn) SetDeadline(deadline time.Time) (err error) {
	err = p.stream.SetReadDeadline(deadline)
	return
}

func (p *quicConn) SetCounter(c *Counter) {
	p.counter = c
}

func (p *quicConn) Read() (framing.Frame, error) {
	return readFromDecoder(p.counter, p.decoder)
}

func (p *quicConn) Write(frame framing.Frame) (err error) {
	return writeTo(p.counter, frame, p.writer)
}

func (p *quicConn) Flush() (err error) {
	err = p.writer.Flush()
	if err != nil {
		err = errors.Wrap(err, "flush failed")
	}
	return
}

func newQuicRConnection(session quic.Session, stream quic.Stream) *quicConn {
	return &quicConn{
		session: session,
		stream:  stream,
		writer:  bufio.NewWriter(stream),
		decoder: NewLengthBasedFrameDecoder(stream),
	}
}
