package splithttp_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/udp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func Benchmark_H3XHTTP_packetUpRoundTrip(b *testing.B) {
	listenPort := udp.PickPort()
	certificate, certificateHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path: "benchmark",
			Mode: "packet-up",
			ScMaxEachPostBytes: &RangeConfig{
				From: 64 * 1024,
				To:   64 * 1024,
			},
		},
		SecurityType: "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(certificate)},
			PinnedPeerCertSha256: [][]byte{certificateHash[:]},
			NextProtocol:         []string{"h3"},
		},
	}

	listener, err := ListenXH(context.Background(), net.LocalHostIP, listenPort, streamSettings, func(conn stat.Connection) {
		go func() {
			defer conn.Close()
			block := make([]byte, 64*1024)
			for {
				n, readErr := conn.Read(block)
				if n > 0 {
					if _, writeErr := conn.Write(block[:n]); writeErr != nil {
						return
					}
				}
				if readErr != nil {
					return
				}
			}
		}()
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = listener.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	b.Cleanup(cancel)
	conn, err := Dial(ctx, net.UDPDestination(net.DomainAddress("localhost"), listenPort), streamSettings)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })

	for _, benchmark := range []struct {
		name string
		size int
	}{
		{name: "1KiB", size: 1024},
		{name: "64KiB", size: 64 * 1024},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			payload := bytes.Repeat([]byte{0x5a}, benchmark.size)
			received := make([]byte, benchmark.size)

			if _, err := conn.Write(payload); err != nil {
				b.Fatal(err)
			}
			if _, err := io.ReadFull(conn, received); err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(received, payload) {
				b.Fatal("preflight payload mismatch")
			}

			b.ReportAllocs()
			b.SetBytes(int64(benchmark.size))
			b.ResetTimer()
			for b.Loop() {
				if _, err := conn.Write(payload); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(conn, received); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
