package splithttp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xtls/xray-core/common/buf"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func Test_DefaultDialerClient_PostPacket_preservesWireContract(t *testing.T) {
	// Given
	var capturedRequest *http.Request
	var capturedBody []byte
	client := &DefaultDialerClient{
		transportConfig: &Config{Path: "/x/"},
		client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			var err error
			capturedRequest = request
			capturedBody, err = io.ReadAll(request.Body)
			require.NoError(t, err)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		})},
		httpVersion: "3",
	}

	// When
	err := client.PostPacket(
		context.Background(),
		"https://example.test/x/",
		"session",
		"10",
		buf.MergeBytes(nil, []byte("abc")),
	)

	// Then
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, capturedRequest.Method)
	require.Equal(t, "/x/session/10", capturedRequest.URL.EscapedPath())
	require.Equal(t, int64(3), capturedRequest.ContentLength)
	require.Equal(t, []byte("abc"), capturedBody)
}

func Test_DefaultDialerClient_PostPacket_rejectsNonOKStatus(t *testing.T) {
	// Given
	client := &DefaultDialerClient{
		transportConfig: &Config{Path: "/x/"},
		client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusTeapot,
				Status:     "418 I'm a teapot",
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		})},
		httpVersion: "3",
	}

	// When
	err := client.PostPacket(
		context.Background(),
		"https://example.test/x/",
		"session",
		"10",
		buf.MergeBytes(nil, []byte("abc")),
	)

	// Then
	require.ErrorContains(t, err, "bad status code")
}
