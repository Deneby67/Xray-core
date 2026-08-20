package splithttp

import (
	"context"
	gotls "crypto/tls"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/require"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func Test_h3StreamClient_releasesRetiredConnectionAfterGOAWAY(t *testing.T) {
	// Given
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server, packetConn, certificate, certificateHash := startLifecycleH3Server(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte{0x5a})
		writer.(http.Flusher).Flush()
		close(requestStarted)
		<-releaseRequest
	}))
	port := xnet.Port(packetConn.LocalAddr().(*stdnet.UDPAddr).Port)
	config := &Config{Host: "localhost", Path: "stream-one-goaway", Mode: "stream-one"}
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: config,
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
	client, ok := createHTTPClient(xnet.UDPDestination(xnet.DomainAddress("localhost"), port), streamSettings).(*h3StreamClient)
	require.True(t, ok)
	t.Cleanup(func() { _ = client.Close() })
	rawURL := fmt.Sprintf("https://localhost:%d%s", port, config.GetNormalizedPath())
	requestURL, err := url.Parse(rawURL)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	reader, writer, _, _, err := client.OpenH3StreamOne(ctx, rawURL)
	require.NoError(t, err)
	responseByte := make([]byte, 1)
	_, err = io.ReadFull(reader, responseByte)
	require.NoError(t, err)
	require.Equal(t, []byte{0x5a}, responseByte)
	select {
	case <-requestStarted:
	case <-ctx.Done():
		t.Fatal("first HTTP/3 request did not start")
	}
	client.mu.Lock()
	require.Len(t, client.connections, 1)
	retiredConn := client.connections[0]
	client.mu.Unlock()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(ctx) }()

	// When
	for {
		_, probe, openErr := client.openRequestStream(ctx, requestURL)
		if openErr != nil {
			break
		}
		probe.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		probe.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("client did not observe HTTP/3 GOAWAY")
		}
	}
	close(releaseRequest)
	require.NoError(t, writer.Close())
	require.NoError(t, reader.Close())
	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("HTTP/3 graceful shutdown did not complete")
	}
	select {
	case <-retiredConn.Context().Done():
	case <-ctx.Done():
		t.Fatal("retired HTTP/3 connection did not close")
	}

	// Then
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.connections) == 0
	}, time.Second, 10*time.Millisecond, "closed retired HTTP/3 connection remained registered")
}

func startLifecycleH3Server(t *testing.T, handler http.Handler) (*http3.Server, *stdnet.UDPConn, *cert.Certificate, [32]byte) {
	t.Helper()
	certificate, certificateHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	certificatePEM, keyPEM := certificate.ToPEM()
	serverCertificate, err := gotls.X509KeyPair(certificatePEM, keyPEM)
	require.NoError(t, err)
	packetConn, err := stdnet.ListenUDP("udp", &stdnet.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	server := &http3.Server{
		Handler:   handler,
		TLSConfig: &gotls.Config{Certificates: []gotls.Certificate{serverCertificate}},
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(packetConn)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = packetConn.Close()
		<-serveDone
	})
	return server, packetConn, certificate, certificateHash
}
