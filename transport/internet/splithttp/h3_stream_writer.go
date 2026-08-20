package splithttp

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common/buf"
)

type h3StreamWriter struct {
	stream io.WriteCloser

	mu          sync.Mutex
	cond        *sync.Cond
	pending     []byte
	spare       []byte
	pendingSize int
	start       chan []byte
	done        chan struct{}
	writing     bool
	closed      bool
	writeErr    error
	failed      atomic.Bool
}

func newH3StreamWriter(stream io.WriteCloser) *h3StreamWriter {
	writer := &h3StreamWriter{
		stream:  stream,
		pending: make([]byte, buf.Size),
		spare:   make([]byte, buf.Size),
		start:   make(chan []byte, 1),
		done:    make(chan struct{}),
	}
	writer.cond = sync.NewCond(&writer.mu)
	go writer.run()
	return writer
}

func (w *h3StreamWriter) run() {
	defer close(w.done)
	for batch := range w.start {
		for {
			written, err := w.stream.Write(batch)
			if err == nil && written != len(batch) {
				err = io.ErrShortWrite
			}

			w.mu.Lock()
			completed := batch[:cap(batch)]
			if err != nil {
				w.writeErr = err
				w.pendingSize = 0
				w.spare = completed
				w.writing = false
				w.cond.Broadcast()
				w.mu.Unlock()
				break
			}
			if w.pendingSize == 0 {
				w.spare = completed
				w.writing = false
				w.cond.Broadcast()
				w.mu.Unlock()
				break
			}
			batch = w.pending[:w.pendingSize]
			w.pending = completed
			w.pendingSize = 0
			w.cond.Broadcast()
			w.mu.Unlock()
		}
	}
}

func (w *h3StreamWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	if w.closed || w.failed.Load() {
		w.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if w.writeErr != nil {
		err := w.writeErr
		w.mu.Unlock()
		return 0, err
	}
	if len(payload) == 0 {
		w.mu.Unlock()
		return 0, nil
	}
	if len(payload) > buf.Size {
		for w.writing && !w.closed && w.writeErr == nil {
			w.cond.Wait()
		}
		if w.closed || w.failed.Load() {
			w.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if w.writeErr != nil {
			err := w.writeErr
			w.mu.Unlock()
			return 0, err
		}
		written, err := w.stream.Write(payload)
		w.mu.Unlock()
		return written, err
	}

	for len(payload) > len(w.pending)-w.pendingSize && !w.closed && w.writeErr == nil {
		w.cond.Wait()
	}
	if w.closed || w.failed.Load() {
		w.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if w.writeErr != nil {
		err := w.writeErr
		w.mu.Unlock()
		return 0, err
	}
	copy(w.pending[w.pendingSize:], payload)
	w.pendingSize += len(payload)
	if w.writing {
		w.mu.Unlock()
		return len(payload), nil
	}

	batch := w.pending[:w.pendingSize]
	w.pending = w.spare
	w.pendingSize = 0
	w.spare = nil
	w.writing = true
	w.mu.Unlock()
	w.start <- batch
	return len(payload), nil
}

func (w *h3StreamWriter) fail() {
	w.failed.Store(true)
}

func (w *h3StreamWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.cond.Broadcast()
	for w.writing {
		w.cond.Wait()
	}
	writeErr := w.writeErr
	close(w.start)
	w.mu.Unlock()
	<-w.done
	return errors.Join(writeErr, w.stream.Close())
}
