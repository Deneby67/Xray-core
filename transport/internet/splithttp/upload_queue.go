package splithttp

// upload_queue is a specialized priorityqueue + channel to reorder generic
// packets by a sequence number

import (
	"container/heap"
	"io"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/signal/done"
)

type Packet struct {
	Reader             *httpServerConn
	Payload            []byte
	payloadMultiBuffer buf.MultiBuffer
	Seq                uint64
}

func (p *Packet) release() {
	p.payloadMultiBuffer = buf.ReleaseMulti(p.payloadMultiBuffer)
}

type uploadQueue struct {
	reader        atomic.Pointer[httpServerConn]
	pushedPackets chan Packet
	heap          uploadHeap
	nextSeq       uint64
	maxPackets    int
	closed        *done.Instance
	pushMu        sync.RWMutex
	readMu        sync.Mutex
}

func NewUploadQueue(maxPackets int) *uploadQueue {
	return &uploadQueue{
		pushedPackets: make(chan Packet, maxPackets),
		heap:          uploadHeap{},
		nextSeq:       0,
		closed:        done.New(),
		maxPackets:    maxPackets,
	}
}

// Push takes ownership of p, including when it returns an error.
func (h *uploadQueue) Push(p Packet) error {
	h.pushMu.RLock()
	defer h.pushMu.RUnlock()

	if h.closed.Done() {
		p.release()
		return errors.New("packet queue closed")
	}
	if h.reader.Load() != nil || (p.Reader != nil && !h.reader.CompareAndSwap(nil, p.Reader)) {
		p.release()
		return errors.New("h.reader already exists")
	}
	select {
	case h.pushedPackets <- p: // no panic
		if h.closed.Done() {
			return errors.New("packet queue closed")
		}
		return nil
	case <-h.closed.Wait():
		p.release()
		return errors.New("packet queue closed")
	}
}

func (h *uploadQueue) Close() error {
	h.closed.Close()
	h.pushMu.Lock()
	defer h.pushMu.Unlock()

	var closeErr error
	if reader := h.reader.Load(); reader != nil {
		closeErr = reader.Close()
	}

	h.readMu.Lock()
	h.releasePending()
	h.readMu.Unlock()

	return closeErr
}

func (h *uploadQueue) Read(b []byte) (int, error) {
	if reader := h.reader.Load(); reader != nil {
		return reader.Read(b)
	}

	h.readMu.Lock()
	n, err, closeQueue := h.readPackets(b)
	h.readMu.Unlock()

	if closeQueue {
		_ = h.Close()
	}
	if n == 0 && err == nil {
		if reader := h.reader.Load(); reader != nil {
			return reader.Read(b)
		}
	}
	return n, err
}

func (h *uploadQueue) readPackets(b []byte) (int, error, bool) {
	if h.closed.Done() {
		return 0, io.EOF, false
	}

	if len(h.heap) == 0 {
		select {
		case p := <-h.pushedPackets:
			if p.Reader != nil {
				return 0, nil, false
			}
			heap.Push(&h.heap, p)
		case <-h.closed.Wait():
			return 0, io.EOF, false
		}
	}

	for len(h.heap) > 0 {
		packet := heap.Pop(&h.heap).(Packet)

		if packet.Seq == h.nextSeq {
			if !packet.payloadMultiBuffer.IsEmpty() {
				var n int
				packet.payloadMultiBuffer, n = buf.SplitBytes(packet.payloadMultiBuffer, b)
				if packet.payloadMultiBuffer.IsEmpty() {
					h.nextSeq = packet.Seq + 1
				} else {
					heap.Push(&h.heap, packet)
				}
				return n, nil, false
			}

			copy(b, packet.Payload)
			n := min(len(b), len(packet.Payload))

			if n < len(packet.Payload) {
				// partial read
				packet.Payload = packet.Payload[n:]
				heap.Push(&h.heap, packet)
			} else {
				h.nextSeq = packet.Seq + 1
			}

			return n, nil, false
		}

		// misordered packet
		if packet.Seq > h.nextSeq {
			if len(h.heap) > h.maxPackets {
				// the "reassembly buffer" is too large, and we want to
				// constrain memory usage somehow. let's tear down the
				// connection, and hope the application retries.
				packet.release()
				return 0, errors.New("packet queue is too large"), true
			}
			heap.Push(&h.heap, packet)
			select {
			case p := <-h.pushedPackets:
				heap.Push(&h.heap, p)
			case <-h.closed.Wait():
				return 0, io.EOF, false
			}
		}
	}

	return 0, nil, false
}

func (h *uploadQueue) releasePending() {
	for len(h.heap) > 0 {
		packet := heap.Pop(&h.heap).(Packet)
		packet.release()
	}

	for {
		select {
		case packet := <-h.pushedPackets:
			packet.release()
		default:
			return
		}
	}
}

// heap code directly taken from https://pkg.go.dev/container/heap
type uploadHeap []Packet

func (h uploadHeap) Len() int           { return len(h) }
func (h uploadHeap) Less(i, j int) bool { return h[i].Seq < h[j].Seq }
func (h uploadHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *uploadHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Packet))
}

func (h *uploadHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
