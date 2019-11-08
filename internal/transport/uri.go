package transport

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

const (
	schemaUNIX            = "unix"
	schemaTCP             = "tcp"
	schemaTCPS            = "tcps"
	schemaWebsocket       = "ws"
	schemaWebsocketSecure = "wss"
	schemaQUIC            = "quic"
)

const tlsProtoQUIC = "quic-rsocket"

// URI represents a URI of RSocket transport.
type URI url.URL

// IsWebsocket returns true if current uri is websocket.
func (p *URI) IsWebsocket() bool {
	switch strings.ToLower(p.Scheme) {
	case schemaWebsocket, schemaWebsocketSecure:
		return true
	default:
		return false
	}
}

// MakeClientTransport creates a new client-side transport.
func (p *URI) MakeClientTransport(tc *tls.Config, headers map[string][]string) (*Transport, error) {
	switch strings.ToLower(p.Scheme) {
	case schemaTCP:
		return newTCPClientTransport(schemaTCP, p.Host, tc)
	case schemaTCPS:
		if tc == nil {
			tc = generateInsecureTLSConfig(false)
		}
		return newTCPClientTransport(schemaTCP, p.Host, tc)
	case schemaWebsocket:
		if tc == nil {
			return newWebsocketClientTransport(p.pp().String(), nil, headers)
		}
		var clone = (url.URL)(*p)
		clone.Scheme = "wss"
		return newWebsocketClientTransport(clone.String(), tc, headers)
	case schemaWebsocketSecure:
		if tc == nil {
			tc = generateInsecureTLSConfig(false)
		}
		return newWebsocketClientTransport(p.pp().String(), tc, headers)
	case schemaUNIX:
		return newTCPClientTransport(schemaUNIX, p.Path, tc)
	case schemaQUIC:
		if tc == nil {
			tc = generateInsecureTLSConfig(true)
		}
		return newQuicClientTransport(p.Host, tc)
	default:
		return nil, errors.Errorf("unsupported transport url: %s", p.pp().String())
	}
}

// MakeServerTransport creates a new server-side transport.
func (p *URI) MakeServerTransport(c *tls.Config) (tp ServerTransport, err error) {
	switch strings.ToLower(p.Scheme) {
	case schemaTCP:
		tp = newTCPServerTransport(schemaTCP, p.Host, c)
	case schemaTCPS:
		if c == nil {
			c = generateTLSConfig(false)
		}
		tp = newTCPServerTransport(schemaTCP, p.Host, c)
	case schemaWebsocket:
		tp = newWebsocketServerTransport(p.Host, p.Path, c)
	case schemaWebsocketSecure:
		if c == nil {
			c = generateTLSConfig(false)
		}
		tp = newWebsocketServerTransport(p.Host, p.Path, c)
	case schemaUNIX:
		tp = newTCPServerTransport(schemaUNIX, p.Path, c)
	case schemaQUIC:
		if c == nil {
			c = generateTLSConfig(true)
		}
		tp = newQuicServerTransport(p.Host, c)
	default:
		err = errors.Errorf("unsupported transport url: %s", p.pp().String())
	}
	return
}

func (p *URI) String() string {
	return p.pp().String()
}

func (p *URI) pp() *url.URL {
	return (*url.URL)(p)
}

// ParseURI parse URI string and returns a URI.
func ParseURI(rawurl string) (*URI, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, errors.Wrapf(err, "parse url failed: %s", rawurl)
	}
	return (*URI)(u), nil
}

func generateInsecureTLSConfig(quic bool) (tlsConf *tls.Config) {
	tlsConf = &tls.Config{
		InsecureSkipVerify: true,
	}
	if quic {
		tlsConf.NextProtos = append(tlsConf.NextProtos, tlsProtoQUIC)
	}
	return
}

func generateTLSConfig(quic bool) (tlsConf *tls.Config) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	tlsConf = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}
	if quic {
		tlsConf.NextProtos = append(tlsConf.NextProtos, tlsProtoQUIC)
	}
	return tlsConf
}
