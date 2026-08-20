package splithttp_test

import (
	"bytes"
	"context"
	gotls "crypto/tls"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/require"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type capturedStreamRequest struct {
	method        string
	path          string
	rawQuery      string
	host          string
	contractValue string
	contentType   string
	contentLength int64
	body          []byte
}

func Test_H3XHTTP_streamOne_preservesRequestWireContract(t *testing.T) {
	// Given
	payload := bytes.Repeat([]byte{0x3c}, 128*1024)
	serverGreeting := []byte("response-before-upload-completes")
	captured := make(chan capturedStreamRequest, 1)
	listenPort, certificate, certificateHash := startH3ContractServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		_, _ = writer.Write(serverGreeting)
		writer.(http.Flusher).Flush()

		body := make([]byte, len(payload))
		_, readErr := io.ReadFull(request.Body, body)
		if readErr != nil {
			return
		}
		captured <- capturedStreamRequest{
			method:        request.Method,
			path:          request.URL.EscapedPath(),
			rawQuery:      request.URL.RawQuery,
			host:          request.Host,
			contractValue: request.Header.Get("X-Stream-Contract"),
			contentType:   request.Header.Get("Content-Type"),
			contentLength: request.ContentLength,
			body:          body,
		}
		_, _ = writer.Write(body)
		writer.(http.Flusher).Flush()
	}))
	config := &Config{
		Host:             "localhost",
		Path:             "wire-contract?token=stable",
		Mode:             "stream-one",
		Headers:          map[string]string{"X-Stream-Contract": "preserved"},
		UplinkHTTPMethod: http.MethodPut,
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := Dial(ctx, xnet.UDPDestination(xnet.DomainAddress("localhost"), listenPort), streamSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// When
	receivedGreeting := make([]byte, len(serverGreeting))
	_, err = io.ReadFull(conn, receivedGreeting)
	require.NoError(t, err)
	_, err = conn.Write(payload)
	require.NoError(t, err)
	receivedPayload := make([]byte, len(payload))
	_, err = io.ReadFull(conn, receivedPayload)
	require.NoError(t, err)

	// Then
	require.Equal(t, serverGreeting, receivedGreeting)
	require.Equal(t, payload, receivedPayload)
	select {
	case request := <-captured:
		require.Equal(t, http.MethodPut, request.method)
		require.Equal(t, config.GetNormalizedPath(), request.path)
		require.Equal(t, "token=stable", request.rawQuery)
		require.Equal(t, "localhost", request.host)
		require.Equal(t, "preserved", request.contractValue)
		require.Equal(t, "application/grpc", request.contentType)
		require.Equal(t, int64(-1), request.contentLength)
		require.Equal(t, payload, request.body)
	case <-ctx.Done():
		t.Fatal("server did not capture stream-one request")
	}
}

func Test_H3XHTTP_streamOne_closesBothDirections_onNonOKResponse(t *testing.T) {
	// Given
	listenPort, certificate, certificateHash := startH3ContractServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusTeapot)
		_, _ = writer.Write([]byte("rejected"))
	}))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Host: "localhost", Path: "stream-one-rejected", Mode: "stream-one"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := Dial(ctx, xnet.UDPDestination(xnet.DomainAddress("localhost"), listenPort), streamSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// When
	_, readErr := conn.Read(make([]byte, 1))
	_, writeErr := conn.Write([]byte("must-not-upload"))

	// Then
	require.Error(t, readErr)
	require.Error(t, writeErr)
}

func Test_H3XHTTP_streamOne_doesNotWaitForUnboundedNonOKBody(t *testing.T) {
	// Given
	serverSawCancellation := make(chan struct{})
	listenPort, certificate, certificateHash := startH3ContractServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusTeapot)
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
		close(serverSawCancellation)
	}))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Host: "localhost", Path: "stream-one-held-error-body", Mode: "stream-one"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := Dial(ctx, xnet.UDPDestination(xnet.DomainAddress("localhost"), listenPort), streamSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	readDone := make(chan error, 1)

	// When
	go func() {
		_, readErr := conn.Read(make([]byte, 1))
		readDone <- readErr
	}()

	// Then
	select {
	case readErr := <-readDone:
		require.Error(t, readErr)
	case <-time.After(time.Second):
		_ = conn.Close()
		t.Fatal("stream-one read waited for an unbounded non-200 response body")
	}
	select {
	case <-serverSawCancellation:
	case <-time.After(time.Second):
		t.Fatal("stream-one non-200 response did not cancel the server request")
	}
}

func Test_H3XHTTP_streamOne_closeBeforeResponseClosesLateBody(t *testing.T) {
	// Given
	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	serverWriteDone := make(chan error, 1)
	listenPort, certificate, certificateHash := startH3ContractServer(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseResponse
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		block := make([]byte, 64*1024)
		for {
			if _, writeErr := writer.Write(block); writeErr != nil {
				serverWriteDone <- writeErr
				return
			}
			writer.(http.Flusher).Flush()
		}
	}))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Host: "localhost", Path: "stream-one-late-response", Mode: "stream-one"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, err := Dial(ctx, xnet.UDPDestination(xnet.DomainAddress("localhost"), listenPort), streamSettings)
	require.NoError(t, err)
	select {
	case <-requestStarted:
	case <-ctx.Done():
		t.Fatal("server did not receive the stream-one request")
	}

	// When
	require.NoError(t, conn.Close())
	close(releaseResponse)

	// Then
	select {
	case writeErr := <-serverWriteDone:
		require.Error(t, writeErr)
	case <-time.After(2 * time.Second):
		t.Fatal("late HTTP/3 response body remained open after the client closed")
	}
}

func Test_H3XHTTP_streamOne_supportsConcurrentReusedH3Connection(t *testing.T) {
	// Given
	const (
		parallelStreams = 8
		payloadSize     = 32 * 1024
	)
	listenPort, certificate, certificateHash := startH3ContractServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		payload := make([]byte, payloadSize)
		if _, err := io.ReadFull(request.Body, payload); err != nil {
			return
		}
		_, _ = writer.Write(payload)
		writer.(http.Flusher).Flush()
	}))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Host: "localhost", Path: "stream-one-concurrent", Mode: "stream-one"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	errs := make(chan error, parallelStreams)
	var streams sync.WaitGroup
	streams.Add(parallelStreams)

	// When
	for streamID := range parallelStreams {
		go func() {
			defer streams.Done()
			conn, err := Dial(ctx, xnet.UDPDestination(xnet.DomainAddress("localhost"), listenPort), streamSettings)
			if err != nil {
				errs <- fmt.Errorf("dial stream %d: %w", streamID, err)
				return
			}
			defer conn.Close()
			payload := bytes.Repeat([]byte{byte(streamID + 1)}, payloadSize)
			if _, err = conn.Write(payload); err != nil {
				errs <- fmt.Errorf("write stream %d: %w", streamID, err)
				return
			}
			received := make([]byte, payloadSize)
			if _, err = io.ReadFull(conn, received); err != nil {
				errs <- fmt.Errorf("read stream %d: %w", streamID, err)
				return
			}
			if !bytes.Equal(payload, received) {
				errs <- fmt.Errorf("payload mismatch on stream %d", streamID)
			}
		}()
	}
	streams.Wait()
	close(errs)

	// Then
	for err := range errs {
		require.NoError(t, err)
	}
}

func startH3ContractServer(t *testing.T, handler http.Handler) (xnet.Port, *cert.Certificate, [32]byte) {
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

	return xnet.Port(packetConn.LocalAddr().(*stdnet.UDPAddr).Port), certificate, certificateHash
}
