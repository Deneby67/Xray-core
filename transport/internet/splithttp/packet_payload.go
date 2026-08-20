package splithttp

import (
	"io"

	"github.com/xtls/xray-core/common/buf"
)

type packetPayload struct {
	direct      []byte
	multiBuffer buf.MultiBuffer
}

func (p *packetPayload) appendBytes(payload []byte) {
	if len(payload) == 0 {
		return
	}
	if p.multiBuffer.IsEmpty() {
		if len(p.direct) == 0 {
			p.direct = payload
			return
		}
		p.direct = append(p.direct, payload...)
		return
	}
	p.multiBuffer = buf.MergeBytes(p.multiBuffer, payload)
}

// appendMultiBuffer takes ownership of payload.
func (p *packetPayload) appendMultiBuffer(payload buf.MultiBuffer) {
	if payload.IsEmpty() {
		return
	}
	if len(p.direct) == 0 && p.multiBuffer.IsEmpty() {
		p.multiBuffer = payload
		return
	}
	if len(p.direct) > 0 {
		p.multiBuffer = buf.MergeBytes(p.multiBuffer, p.direct)
		p.direct = nil
	}
	p.multiBuffer, _ = buf.MergeMulti(p.multiBuffer, payload)
}

func (p *packetPayload) len() int {
	return len(p.direct) + int(p.multiBuffer.Len())
}

func (p *packetPayload) release() {
	p.multiBuffer = buf.ReleaseMulti(p.multiBuffer)
}

func (p *packetPayload) packet(seq uint64) Packet {
	return Packet{
		Payload:            p.direct,
		payloadMultiBuffer: p.multiBuffer,
		Seq:                seq,
	}
}

func readPacketBody(reader io.Reader, contentLength int64) (buf.MultiBuffer, error) {
	payload, err := buf.ReadFrom(io.LimitReader(reader, contentLength))
	if err != nil {
		buf.ReleaseMulti(payload)
		return nil, err
	}
	if int64(payload.Len()) != contentLength {
		buf.ReleaseMulti(payload)
		return nil, io.ErrUnexpectedEOF
	}
	return payload, nil
}
