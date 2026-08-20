package splithttp

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type countingReadCloser struct {
	closes atomic.Int32
}

func (*countingReadCloser) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (r *countingReadCloser) Close() error {
	r.closes.Add(1)
	return nil
}

func Test_WaitReadCloser_closeIsIdempotentAfterSet(t *testing.T) {
	// Given
	reader := new(countingReadCloser)
	waitReader := &WaitReadCloser{Wait: make(chan struct{})}
	waitReader.Set(reader)

	// When
	firstErr := waitReader.Close()
	secondErr := waitReader.Close()

	// Then
	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	require.Equal(t, int32(1), reader.closes.Load())
}

func Test_WaitReadCloser_closeBeforeSetRejectsLateReader(t *testing.T) {
	// Given
	waitReader := &WaitReadCloser{Wait: make(chan struct{})}
	reader := new(countingReadCloser)
	require.NoError(t, waitReader.Close())

	// When
	waitReader.Set(reader)
	_, readErr := waitReader.Read(make([]byte, 1))
	secondCloseErr := waitReader.Close()

	// Then
	require.ErrorIs(t, readErr, io.ErrClosedPipe)
	require.NoError(t, secondCloseErr)
	require.Equal(t, int32(1), reader.closes.Load())
}
