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
	benchmarkH3XHTTPRoundTrip(b, "packet-up")
}

func Benchmark_H3XHTTP_streamOneRoundTrip(b *testing.B) {
	benchmarkH3XHTTPRoundTrip(b, "stream-one")
}

func benchmarkH3XHTTPRoundTrip(b *testing.B, mode string) {
	listenPort := udp.PickPort()
	certificate, certificateHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path: "benchmark",
			Mode: mode,
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

	type benchmarkCase struct {
		name   string
		size   int
		chunks int
	}
	benchmarks := []benchmarkCase{
		{name: "64B", size: 64, chunks: 1},
		{name: "1KiB", size: 1024, chunks: 1},
		{name: "4KiB", size: 4 * 1024, chunks: 1},
	}
	if mode == "stream-one" {
		benchmarks = append(benchmarks, benchmarkCase{name: "16x256B", size: 16 * 256, chunks: 16})
	}
	benchmarks = append(benchmarks,
		benchmarkCase{name: "64KiB", size: 64 * 1024, chunks: 1},
		benchmarkCase{name: "1MiB", size: 1024 * 1024, chunks: 1},
	)
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			payload := bytes.Repeat([]byte{0x5a}, benchmark.size)
			received := make([]byte, benchmark.size)
			chunkSize := benchmark.size / benchmark.chunks
			writePayload := func() error {
				for start := 0; start < len(payload); start += chunkSize {
					if _, err := conn.Write(payload[start : start+chunkSize]); err != nil {
						return err
					}
				}
				return nil
			}

			if err := writePayload(); err != nil {
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
				if err := writePayload(); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(conn, received); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
