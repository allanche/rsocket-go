package transport

import (
	"bufio"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/rsocket/rsocket-go/internal/framing"
)

type tcpConn struct {
	rawConn net.Conn
	writer  *bufio.Writer
	decoder *LengthBasedFrameDecoder
	counter *Counter
}

func (p *tcpConn) SetCounter(c *Counter) {
	p.counter = c
}

func (p *tcpConn) SetDeadline(deadline time.Time) error {
	return p.rawConn.SetReadDeadline(deadline)
}

func (p *tcpConn) Read() (f framing.Frame, err error) {
	return readFromDecoder(p.counter, p.decoder)
}

func (p *tcpConn) Flush() (err error) {
	err = p.writer.Flush()
	if err != nil {
		err = errors.Wrap(err, "flush failed")
	}
	return
}

func (p *tcpConn) Write(frame framing.Frame) (err error) {
	return writeTo(p.counter, frame, p.writer)
}

func (p *tcpConn) Close() error {
	return p.rawConn.Close()
}

func newTCPRConnection(rawConn net.Conn) *tcpConn {
	return &tcpConn{
		rawConn: rawConn,
		writer:  bufio.NewWriter(rawConn),
		decoder: NewLengthBasedFrameDecoder(rawConn),
	}
}
