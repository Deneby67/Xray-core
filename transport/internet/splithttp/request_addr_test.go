package splithttp

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xtls/xray-core/transport/internet"
)

func Test_requestHandler_resolveRemoteAddr_preservesAddressSemantics(t *testing.T) {
	tests := []struct {
		name          string
		protoMajor    int
		remoteAddr    string
		trustedHeader bool
		wantNetwork   string
		wantAddress   string
	}{
		{
			name:        "HTTP/2 uses TCP address",
			protoMajor:  2,
			remoteAddr:  "127.0.0.1:1234",
			wantNetwork: "tcp",
			wantAddress: "127.0.0.1:1234",
		},
		{
			name:        "HTTP/3 uses UDP address",
			protoMajor:  3,
			remoteAddr:  "127.0.0.1:1234",
			wantNetwork: "udp",
			wantAddress: "127.0.0.1:1234",
		},
		{
			name:        "malformed HTTP/3 address uses fallback",
			protoMajor:  3,
			remoteAddr:  "malformed",
			wantNetwork: "udp",
			wantAddress: "0.0.0.0:0",
		},
		{
			name:          "trusted forwarded address preserves TCP override",
			protoMajor:    3,
			remoteAddr:    "127.0.0.1:1234",
			trustedHeader: true,
			wantNetwork:   "tcp",
			wantAddress:   "203.0.113.7:0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			headers := make(http.Header)
			var trusted []string
			if test.trustedHeader {
				headers.Set("X-Forwarded-For", "203.0.113.7")
				headers.Set("X-Trusted-Proxy", "true")
				trusted = []string{"X-Trusted-Proxy"}
			}
			handler := &requestHandler{
				socketSettings: &internet.SocketConfig{TrustedXForwardedFor: trusted},
			}
			request := &http.Request{
				Header:     headers,
				ProtoMajor: test.protoMajor,
				RemoteAddr: test.remoteAddr,
			}

			// When
			address := handler.resolveRemoteAddr(request)

			// Then
			require.Equal(t, test.wantNetwork, address.Network())
			require.Equal(t, test.wantAddress, address.String())
		})
	}
}
