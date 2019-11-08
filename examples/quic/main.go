package main

import (
	"context"
	"log"

	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/mono"
)

func main() {
	started := make(chan struct{})
	go func(started chan<- struct{}) {
		runServer(context.Background(), started)
	}(started)
	<-started

	cli, err := rsocket.Connect().Transport("quic://127.0.0.1:7878").Start(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	resp, err := cli.RequestResponse(payload.NewString("PING", "Hello QUIC!")).Block(context.Background())
	if err != nil {
		panic(err)
	}
	m, _ := resp.MetadataUTF8()
	log.Printf("rcv response:\tdata=%s, metadata=%s\n", resp.DataUTF8(), m)
}

func runServer(ctx context.Context, notify chan<- struct{}) {
	acceptor := func(setup payload.SetupPayload, sendingSocket rsocket.CloseableRSocket) (socket rsocket.RSocket, err error) {
		socket = rsocket.NewAbstractSocket(rsocket.RequestResponse(func(msg payload.Payload) mono.Mono {
			m, _ := msg.MetadataUTF8()
			log.Printf("rcv request:\tdata=%s, metadata=%s\n", msg.DataUTF8(), m)
			return mono.Just(payload.NewString("PONG", m))
		}))
		return
	}
	onStart := func() {
		notify <- struct{}{}
	}
	if err := rsocket.Receive().OnStart(onStart).Acceptor(acceptor).Transport("quic://127.0.0.1:7878").Serve(ctx); err != nil {
		log.Fatal(err)
	}
}
