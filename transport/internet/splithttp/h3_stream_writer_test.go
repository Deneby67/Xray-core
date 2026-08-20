package splithttp

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingWriteCloser struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	written []byte
	writes  int
}

func (w *blockingWriteCloser) Write(payload []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	w.written = append(w.written, payload...)
	w.writes++
	return len(payload), nil
}

func (w *blockingWriteCloser) Close() error {
	return nil
}

func Test_h3StreamWriter_returnsAfterCopy_beforeQUICWriteCompletes(t *testing.T) {
	// Given
	underlying := &blockingWriteCloser{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := newH3StreamWriter(underlying)
	payload := bytes.Repeat([]byte{0x5a}, 1024)
	original := bytes.Clone(payload)
	writeDone := make(chan error, 1)

	// When
	go func() {
		_, err := writer.Write(payload)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("small write waited for the underlying QUIC write")
	}
	for index := range payload {
		payload[index] = 0
	}
	select {
	case <-underlying.started:
	case <-time.After(time.Second):
		t.Fatal("underlying write did not start")
	}
	close(underlying.release)
	require.NoError(t, writer.Close())

	// Then
	require.Equal(t, original, underlying.written)
}

func Test_h3StreamWriter_returnsPendingError_onClose(t *testing.T) {
	// Given
	underlying := errorWriteCloser{err: io.ErrClosedPipe}
	writer := newH3StreamWriter(underlying)

	// When
	_, writeErr := writer.Write([]byte("buffered"))
	closeErr := writer.Close()

	// Then
	require.NoError(t, writeErr)
	require.ErrorIs(t, closeErr, io.ErrClosedPipe)
}

func Test_h3StreamWriter_batchesBurstWhileQUICWriteIsPending(t *testing.T) {
	// Given
	underlying := &blockingWriteCloser{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	writer := newH3StreamWriter(underlying)
	t.Cleanup(func() {
		select {
		case <-underlying.release:
		default:
			close(underlying.release)
		}
		_ = writer.Close()
	})
	chunks := make([][]byte, 16)
	expected := make([]byte, 0, 16*256)
	for index := range chunks {
		chunks[index] = bytes.Repeat([]byte{byte(index)}, 256)
		expected = append(expected, chunks[index]...)
	}
	require.NoError(t, writeChunk(writer, chunks[0]))
	select {
	case <-underlying.started:
	case <-time.After(time.Second):
		t.Fatal("first QUIC write did not start")
	}
	burstDone := make(chan error, 1)

	// When
	go func() {
		for _, chunk := range chunks[1:] {
			if err := writeChunk(writer, chunk); err != nil {
				burstDone <- err
				return
			}
		}
		burstDone <- nil
	}()

	// Then
	select {
	case err := <-burstDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("burst writes waited for the pending QUIC write instead of batching")
	}
	close(underlying.release)
	require.NoError(t, writer.Close())
	require.Equal(t, expected, underlying.written)
	require.Equal(t, 2, underlying.writes)
}

func writeChunk(writer io.Writer, payload []byte) error {
	written, err := writer.Write(payload)
	if err == nil && written != len(payload) {
		return io.ErrShortWrite
	}
	return err
}

type errorWriteCloser struct {
	err error
}

func (w errorWriteCloser) Write([]byte) (int, error) {
	return 0, w.err
}

func (w errorWriteCloser) Close() error {
	return nil
}
