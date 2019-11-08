package transport

import (
	"io"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"github.com/rsocket/rsocket-go/internal/common"
	"github.com/rsocket/rsocket-go/internal/framing"
	"github.com/rsocket/rsocket-go/logger"
)

func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if err == http.ErrServerClosed {
		return true
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	return false
}

func writeTo(counter *Counter, frame framing.Frame, writer io.Writer) (err error) {
	size := frame.Len()
	if counter != nil && frame.IsResumable() {
		counter.incrWriteBytes(size)
	}
	_, err = common.NewUint24(size).WriteTo(writer)
	if err != nil {
		err = errors.Wrap(err, "write frame failed")
		return
	}
	var debugStr string
	if logger.IsDebugEnabled() {
		debugStr = frame.String()
	}
	_, err = frame.WriteTo(writer)
	if err != nil {
		err = errors.Wrap(err, "write frame failed")
		return
	}
	if logger.IsDebugEnabled() {
		logger.Debugf("---> snd: %s\n", debugStr)
	}
	return
}

func readFromDecoder(counter *Counter, decoder *LengthBasedFrameDecoder) (f framing.Frame, err error) {
	raw, err := decoder.Read()
	if err == io.EOF {
		return
	}
	if err != nil {
		err = errors.Wrap(err, "read frame failed")
		return
	}
	h := framing.ParseFrameHeader(raw)
	bf := common.NewByteBuff()
	_, err = bf.Write(raw[framing.HeaderLen:])
	if err != nil {
		err = errors.Wrap(err, "read frame failed")
		return
	}
	base := framing.NewBaseFrame(h, bf)
	if counter != nil && base.IsResumable() {
		counter.incrReadBytes(base.Len())
	}
	f, err = framing.NewFromBase(base)
	if err != nil {
		err = errors.Wrap(err, "read frame failed")
		return
	}
	err = f.Validate()
	if err != nil {
		err = errors.Wrap(err, "read frame failed")
		return
	}
	if logger.IsDebugEnabled() {
		logger.Debugf("<--- rcv: %s\n", f)
	}
	return
}
