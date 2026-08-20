package splithttp

import (
	"context"
	gotls "crypto/tls"
	stderrors "errors"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
	"net/url"
	"sync"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
)

type h3StreamOneOpener interface {
	OpenH3StreamOne(context.Context, string) (io.ReadCloser, io.WriteCloser, net.Addr, net.Addr, error)
}

type h3StreamClient struct {
	*DefaultDialerClient

	transport *http3.Transport

	mu          sync.Mutex
	closed      bool
	conn        *quic.Conn
	clientConn  *http3.ClientConn
	connections []*quic.Conn
}

func newH3StreamClient(client *DefaultDialerClient, transport *http3.Transport) *h3StreamClient {
	return &h3StreamClient{DefaultDialerClient: client, transport: transport}
}

func (c *h3StreamClient) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *h3StreamClient) OpenH3StreamOne(ctx context.Context, rawURL string) (io.ReadCloser, io.WriteCloser, net.Addr, net.Addr, error) {
	requestCtx := context.WithoutCancel(ctx)
	req, err := http.NewRequestWithContext(requestCtx, c.transportConfig.GetNormalizedUplinkHTTPMethod(), rawURL, http.NoBody)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("create HTTP/3 stream-one request: %w", err)
	}
	c.transportConfig.FillStreamRequest(req, "", "")

	var requestStream *http3.RequestStream
	var conn *quic.Conn
	for attempt := 0; attempt < 2; attempt++ {
		conn, requestStream, err = c.openRequestStream(requestCtx, req.URL)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open HTTP/3 request stream: %w", err)
	}
	if err := requestStream.SendRequestHeader(req); err != nil {
		requestStream.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		requestStream.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
		return nil, nil, nil, nil, fmt.Errorf("send HTTP/3 request headers: %w", err)
	}

	reader := newWaitReadCloser()
	writer := newH3StreamWriter(requestStream)
	go c.readStreamOneResponse(ctx, requestStream, reader, writer)
	return reader, writer, conn.RemoteAddr(), conn.LocalAddr(), nil
}

func (c *h3StreamClient) openRequestStream(ctx context.Context, requestURL *url.URL) (*quic.Conn, *http3.RequestStream, error) {
	conn, clientConn, err := c.connection(ctx, requestURL)
	if err != nil {
		return nil, nil, err
	}
	requestStream, err := clientConn.OpenRequestStream(ctx)
	if err != nil {
		c.retire(conn)
		return nil, nil, err
	}
	return conn, requestStream, nil
}

func (c *h3StreamClient) connection(ctx context.Context, requestURL *url.URL) (*quic.Conn, *http3.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, http3.ErrTransportClosed
	}
	if c.conn != nil && c.conn.Context().Err() == nil {
		return c.conn, c.clientConn, nil
	}

	host := requestURL.Host
	if requestURL.Port() == "" {
		host = stdnet.JoinHostPort(requestURL.Hostname(), "443")
	}
	tlsConfig := &gotls.Config{}
	if c.transport.TLSClientConfig != nil {
		tlsConfig = c.transport.TLSClientConfig.Clone()
	}
	if tlsConfig.ServerName == "" {
		tlsConfig.ServerName = requestURL.Hostname()
	}
	tlsConfig.NextProtos = []string{http3.NextProtoH3}
	if c.transport.Dial == nil || c.transport.QUICConfig == nil {
		return nil, nil, fmt.Errorf("HTTP/3 direct stream requires configured transport dialer")
	}

	conn, err := c.transport.Dial(ctx, host, tlsConfig, c.transport.QUICConfig.Clone())
	if err != nil {
		return nil, nil, fmt.Errorf("dial HTTP/3 direct connection: %w", err)
	}
	c.conn = conn
	c.clientConn = c.transport.NewClientConn(conn)
	c.connections = append(c.connections, conn)
	go c.removeConnectionWhenDone(conn)
	return c.conn, c.clientConn, nil
}

func (c *h3StreamClient) removeConnectionWhenDone(conn *quic.Conn) {
	<-conn.Context().Done()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.conn = nil
		c.clientConn = nil
	}
	for index, candidate := range c.connections {
		if candidate != conn {
			continue
		}
		last := len(c.connections) - 1
		c.connections[index] = c.connections[last]
		c.connections[last] = nil
		c.connections = c.connections[:last]
		return
	}
}

func (c *h3StreamClient) retire(conn *quic.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.conn = nil
		c.clientConn = nil
	}
}

func (c *h3StreamClient) readStreamOneResponse(ctx context.Context, stream *http3.RequestStream, reader *WaitReadCloser, writer *h3StreamWriter) {
	for informational := 0; ; informational++ {
		resp, err := stream.ReadResponse()
		if err != nil {
			errors.LogInfoInner(ctx, err, "failed to read HTTP/3 stream-one response")
			c.cancelStream(ctx, stream, reader, writer)
			return
		}
		if resp.StatusCode >= 100 && resp.StatusCode < 200 && resp.StatusCode != http.StatusSwitchingProtocols {
			if informational == 5 {
				errors.LogInfo(ctx, "too many HTTP/3 stream-one informational responses")
				c.cancelStream(ctx, stream, reader, writer)
				return
			}
			continue
		}
		if resp.StatusCode == http.StatusOK {
			reader.Set(resp.Body)
			return
		}

		errors.LogInfo(ctx, "unexpected HTTP/3 stream-one status ", resp.StatusCode)
		c.cancelStream(ctx, stream, reader, writer)
		if err := resp.Body.Close(); err != nil {
			errors.LogInfoInner(ctx, err, "failed to close HTTP/3 stream-one response")
		}
		return
	}
}

func (c *h3StreamClient) cancelStream(ctx context.Context, stream *http3.RequestStream, reader *WaitReadCloser, writer *h3StreamWriter) {
	writer.fail()
	stream.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
	stream.CancelWrite(quic.StreamErrorCode(http3.ErrCodeRequestCanceled))
	_ = writer.Close()
	if err := reader.Close(); err != nil {
		errors.LogInfoInner(ctx, err, "failed to close HTTP/3 stream-one reader")
	}
}

func (c *h3StreamClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.conn = nil
	c.clientConn = nil
	connections := c.connections
	c.connections = nil
	c.mu.Unlock()

	closeErrors := make([]error, 0, len(connections)+1)
	for _, conn := range connections {
		if err := conn.CloseWithError(0, ""); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close HTTP/3 direct connection: %w", err))
		}
	}
	if err := c.DefaultDialerClient.Close(); err != nil {
		closeErrors = append(closeErrors, fmt.Errorf("close HTTP/3 transport: %w", err))
	}
	return stderrors.Join(closeErrors...)
}
