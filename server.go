package rsocket

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/rsocket/rsocket-go/internal/common"
	"github.com/rsocket/rsocket-go/internal/fragmentation"
	"github.com/rsocket/rsocket-go/internal/framing"
	"github.com/rsocket/rsocket-go/internal/session"
	"github.com/rsocket/rsocket-go/internal/socket"
	"github.com/rsocket/rsocket-go/internal/transport"
	"github.com/rsocket/rsocket-go/lease"
	"github.com/rsocket/rsocket-go/logger"
)

const (
	serverSessionCleanInterval = 500 * time.Millisecond
	serverSessionDuration      = 30 * time.Second
)

var (
	errUnavailableResume    = []byte("resume not supported")
	errUnavailableLease     = []byte("lease not supported")
	errDuplicatedSetupToken = []byte("duplicated setup token")
)

type (
	// OpServerResume represents resume options for RSocket server.
	OpServerResume func(o *serverResumeOptions)
	// ServerBuilder can be used to build a RSocket server.
	ServerBuilder interface {
		// Fragment set fragmentation size which default is 16_777_215(16MB).
		Fragment(mtu int) ServerBuilder
		// Lease enable feature of Lease.
		Lease(leases lease.Leases) ServerBuilder
		// Resume enable resume for current server.
		Resume(opts ...OpServerResume) ServerBuilder
		// Acceptor register server acceptor which is used to handle incoming RSockets.
		Acceptor(acceptor ServerAcceptor) ServerTransportBuilder
		// OnStart register a handler when serve success.
		OnStart(onStart func()) ServerBuilder
	}

	// ServerTransportBuilder is used to build a RSocket server with custom Transport string.
	ServerTransportBuilder interface {
		// Transport specify transport string.
		Transport(transport string) Start
	}

	// Start start a RSocket server.
	Start interface {
		// Serve serve RSocket server.
		Serve(ctx context.Context) error
		// Serve serve RSocket server with TLS.
		//
		// You can generate cert.pem and key.pem for local testing:
		//
		//	 go run $GOROOT/src/crypto/tls/generate_cert.go --host localhost
		//
		//	 Load X509
		//	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
		//	if err != nil {
		//		panic(err)
		//	}
		//	// Init TLS configuration.
		//	tc := &tls.Config{
		//		MinVersion:               tls.VersionTLS12,
		//		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		//		PreferServerCipherSuites: true,
		//		CipherSuites: []uint16{
		//			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		//			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		//			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		//			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		//		},
		//		Certificates: []tls.Certificate{cert},
		//	}
		ServeTLS(ctx context.Context, c *tls.Config) error
	}
)

// Receive receives server connections from client RSockets.
func Receive() ServerBuilder {
	return &server{
		fragment: fragmentation.MaxFragment,
		sm:       session.NewManager(),
		done:     make(chan struct{}),
		resumeOpts: &serverResumeOptions{
			sessionDuration: serverSessionDuration,
		},
	}
}

type serverResumeOptions struct {
	enable          bool
	sessionDuration time.Duration
}

type server struct {
	resumeOpts *serverResumeOptions
	fragment   int
	addr       string
	acc        ServerAcceptor
	sm         *session.Manager
	done       chan struct{}
	onServe    []func()
	leases     lease.Leases
}

func (p *server) Lease(leases lease.Leases) ServerBuilder {
	p.leases = leases
	return p
}

func (p *server) OnStart(onStart func()) ServerBuilder {
	if onStart != nil {
		p.onServe = append(p.onServe, onStart)
	}
	return p
}

func (p *server) Resume(opts ...OpServerResume) ServerBuilder {
	p.resumeOpts.enable = true
	for _, it := range opts {
		it(p.resumeOpts)
	}
	return p
}

func (p *server) Fragment(mtu int) ServerBuilder {
	p.fragment = mtu
	return p
}

func (p *server) Acceptor(acceptor ServerAcceptor) ServerTransportBuilder {
	p.acc = acceptor
	return p
}

func (p *server) Transport(transport string) Start {
	p.addr = transport
	return p
}

func (p *server) ServeTLS(ctx context.Context, c *tls.Config) error {
	return p.serve(ctx, c)
}

func (p *server) Serve(ctx context.Context) error {
	return p.serve(ctx, nil)
}

func (p *server) serve(ctx context.Context, tc *tls.Config) error {
	u, err := transport.ParseURI(p.addr)
	if err != nil {
		return err
	}
	err = fragmentation.IsValidFragment(p.fragment)
	if err != nil {
		return err
	}
	t, err := u.MakeServerTransport(tc)
	if err != nil {
		return err
	}

	defer func() {
		_ = t.Close()
	}()

	go func(ctx context.Context) {
		_ = p.loopCleanSession(ctx)
	}(ctx)

	t.Accept(func(ctx context.Context, tp *transport.Transport) {
		socketChan := make(chan socket.ServerSocket, 1)
		defer func() {
			select {
			case ssk, ok := <-socketChan:
				if !ok {
					break
				}
				_, ok = ssk.Token()
				if !ok {
					_ = ssk.Close()
					break
				}
				ssk.Pause()
				deadline := time.Now().Add(p.resumeOpts.sessionDuration)
				s := session.NewSession(deadline, ssk)
				p.sm.Push(s)
				if logger.IsDebugEnabled() {
					logger.Debugf("store session: %s\n", s)
				}
			default:
			}
			close(socketChan)
		}()

		first, err := tp.ReadFirst(ctx)
		if err != nil {
			logger.Errorf("read first frame failed: %s\n", err)
			_ = tp.Close()
			return
		}

		switch frame := first.(type) {
		case *framing.FrameResume:
			p.doResume(frame, tp, socketChan)
		case *framing.FrameSetup:
			sendingSocket, err := p.doSetup(frame, tp, socketChan)
			if err != nil {
				_ = tp.Send(err, true)
				_ = tp.Close()
				return
			}
			go func(ctx context.Context, sendingSocket socket.ServerSocket) {
				if err := sendingSocket.Start(ctx); err != nil && logger.IsDebugEnabled() {
					logger.Debugf("sending socket exit: %w\n", err)
				}
			}(ctx, sendingSocket)
		default:
			err := framing.NewFrameError(0, common.ErrorCodeConnectionError, []byte("first frame must be setup or resume"))
			_ = tp.Send(err, true)
			_ = tp.Close()
			return
		}
		if err := tp.Start(ctx); err != nil {
			logger.Warnf("transport exit: %s\n", err.Error())
		}
	})

	serveNotifier := make(chan struct{})
	go func(c <-chan struct{}, fn []func()) {
		<-c
		for i := range fn {
			fn[i]()
		}
	}(serveNotifier, p.onServe)
	return t.Listen(ctx, serveNotifier)
}

func (p *server) doSetup(
	frame *framing.FrameSetup,
	tp *transport.Transport,
	socketChan chan<- socket.ServerSocket,
) (sendingSocket socket.ServerSocket, err *framing.FrameError) {
	if frame.Header().Flag().Check(framing.FlagLease) && p.leases == nil {
		err = framing.NewFrameError(0, common.ErrorCodeUnsupportedSetup, errUnavailableLease)
		return
	}

	isResume := frame.Header().Flag().Check(framing.FlagResume)

	// 1. receive a token but server doesn't support resume.
	if isResume && !p.resumeOpts.enable {
		err = framing.NewFrameError(0, common.ErrorCodeUnsupportedSetup, errUnavailableResume)
		return
	}

	rawSocket := socket.NewServerDuplexRSocket(p.fragment, p.leases)

	// 2. no resume
	if !isResume {
		sendingSocket = socket.NewServer(rawSocket)
		if responder, e := p.acc(frame, sendingSocket); e != nil {
			err = framing.NewFrameError(0, common.ErrorCodeRejectedSetup, []byte(e.Error()))
		} else {
			sendingSocket.SetResponder(responder)
			sendingSocket.SetTransport(tp)
			socketChan <- sendingSocket
		}
		return
	}

	token := make([]byte, len(frame.Token()))

	// 3. resume reject because of duplicated token.
	if _, ok := p.sm.Load(token); ok {
		err = framing.NewFrameError(0, common.ErrorCodeRejectedSetup, errDuplicatedSetupToken)
		return
	}

	// 4. resume success
	copy(token, frame.Token())
	sendingSocket = socket.NewServerResume(rawSocket, token)
	if responder, e := p.acc(frame, sendingSocket); e != nil {
		switch vv := e.(type) {
		case *framing.FrameError:
			err = framing.NewFrameError(0, vv.ErrorCode(), vv.ErrorData())
		default:
			err = framing.NewFrameError(0, common.ErrorCodeInvalidSetup, []byte(e.Error()))
		}
	} else {
		sendingSocket.SetResponder(responder)
		sendingSocket.SetTransport(tp)
		socketChan <- sendingSocket
	}
	return
}

func (p *server) doResume(frame *framing.FrameResume, tp *transport.Transport, socketChan chan<- socket.ServerSocket) {
	var sending framing.Frame
	if !p.resumeOpts.enable {
		sending = framing.NewFrameError(0, common.ErrorCodeRejectedResume, errUnavailableResume)
	} else if s, ok := p.sm.Load(frame.Token()); ok {
		sending = framing.NewResumeOK(0)
		s.Socket().SetTransport(tp)
		socketChan <- s.Socket()
		if logger.IsDebugEnabled() {
			logger.Debugf("recover session: %s\n", s)
		}
	} else {
		sending = framing.NewFrameError(
			0,
			common.ErrorCodeRejectedResume,
			[]byte("no such session"),
		)
	}
	if err := tp.Send(sending, true); err != nil {
		logger.Errorf("send resume response failed: %s\n", err)
		_ = tp.Close()
	}
}

func (p *server) loopCleanSession(ctx context.Context) (err error) {
	tk := time.NewTicker(serverSessionCleanInterval)
	defer func() {
		tk.Stop()
		p.destroySessions()
	}()
L:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break L
		case <-p.done:
			break L
		case <-tk.C:
			p.doCleanSession()
		}
	}
	return
}

func (p *server) destroySessions() {
	for p.sm.Len() > 0 {
		nextSession := p.sm.Pop()
		if err := nextSession.Close(); err != nil {
			logger.Warnf("kill session failed: %s\n", err)
		} else if logger.IsDebugEnabled() {
			logger.Debugf("kill session success: %s\n", nextSession)
		}
	}
}

func (p *server) doCleanSession() {
	deads := make(chan *session.Session)
	go func(deads chan *session.Session) {
		for it := range deads {
			if err := it.Close(); err != nil {
				logger.Warnf("close dead session failed: %s\n", err)
			} else if logger.IsDebugEnabled() {
				logger.Debugf("close dead session success: %s\n", it)
			}
		}
	}(deads)
	var cur *session.Session
	for p.sm.Len() > 0 {
		cur = p.sm.Pop()
		// Push back if session is still alive.
		if !cur.IsDead() {
			p.sm.Push(cur)
			break
		}
		deads <- cur
	}
	close(deads)
}

// WithServerResumeSessionDuration sets resume session duration for RSocket server.
func WithServerResumeSessionDuration(duration time.Duration) OpServerResume {
	return func(o *serverResumeOptions) {
		o.sessionDuration = duration
	}
}
