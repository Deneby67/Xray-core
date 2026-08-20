package splithttp_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/udp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func Test_H3XHTTP_streamOne_preservesBidirectionalStreamContract(t *testing.T) {
	// Given
	listenPort := udp.PickPort()
	certificate, certificateHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "stream-one-contract", Mode: "stream-one"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
	serverClosed := make(chan struct{})
	serverGreeting := []byte("server-ready")
	listener, err := ListenXH(context.Background(), net.LocalHostIP, listenPort, streamSettings, func(conn stat.Connection) {
		go func() {
			defer close(serverClosed)
			defer conn.Close()
			if _, writeErr := conn.Write(serverGreeting); writeErr != nil {
				return
			}
			_, _ = io.Copy(conn, conn)
		}()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := Dial(ctx, net.UDPDestination(net.DomainAddress("localhost"), listenPort), streamSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// When
	receivedGreeting := make([]byte, len(serverGreeting))
	_, err = io.ReadFull(conn, receivedGreeting)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{0x5a}, 256*1024)
	_, err = conn.Write(payload)
	require.NoError(t, err)
	receivedPayload := make([]byte, len(payload))
	_, err = io.ReadFull(conn, receivedPayload)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// Then
	require.Equal(t, serverGreeting, receivedGreeting)
	require.Equal(t, payload, receivedPayload)
	select {
	case <-serverClosed:
	case <-ctx.Done():
		t.Fatal("server-side stream did not close after client close")
	}
}
