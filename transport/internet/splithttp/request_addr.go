package splithttp

import (
	"net/http"

	"github.com/xtls/xray-core/common/net"
	http_proto "github.com/xtls/xray-core/common/protocol/http"
)

func (h *requestHandler) resolveRemoteAddr(request *http.Request) net.Addr {
	var remoteAddr net.Addr
	remoteAddr, err := net.ResolveTCPAddr("tcp", request.RemoteAddr)
	if err != nil {
		remoteAddr = &net.TCPAddr{
			IP:   []byte{0, 0, 0, 0},
			Port: 0,
		}
	}
	if request.ProtoMajor == 3 {
		remoteAddr = &net.UDPAddr{
			IP:   remoteAddr.(*net.TCPAddr).IP,
			Port: remoteAddr.(*net.TCPAddr).Port,
		}
	}
	var trustedXFF []string
	if h.socketSettings != nil {
		trustedXFF = h.socketSettings.TrustedXForwardedFor
	}
	return http_proto.ApplyTrustedXForwardedFor(request.Header, trustedXFF, remoteAddr)
}
