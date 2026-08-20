package splithttp

import (
	"bytes"
	"container/heap"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xtls/xray-core/common/buf"
)

func Test_readPacketBody_preservesPayloadInPooledChunks(t *testing.T) {
	// Given
	want := bytes.Repeat([]byte{0x5a}, 2*buf.Size+123)

	// When
	payload, err := readPacketBody(bytes.NewReader(want), int64(len(want)))

	// Then
	require.NoError(t, err)
	t.Cleanup(func() { buf.ReleaseMulti(payload) })
	require.Greater(t, len(payload), 1)
	got := make([]byte, 0, len(want))
	for _, block := range payload {
		require.LessOrEqual(t, block.Cap(), int32(buf.Size))
		got = append(got, block.Bytes()...)
	}
	require.Equal(t, want, got)
}

func Test_readPacketBody_rejectsTruncatedBody(t *testing.T) {
	// Given
	const contentLength = 2 * buf.Size
	body := bytes.NewReader(make([]byte, contentLength-1))

	// When
	payload, err := readPacketBody(body, contentLength)

	// Then
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Empty(t, payload)
}

func Test_uploadQueue_readsPooledPayloadInSequence(t *testing.T) {
	// Given
	queue := NewUploadQueue(2)
	first := buf.MergeBytes(nil, []byte("first-packet"))
	second := buf.MergeBytes(nil, []byte("second-packet"))
	require.NoError(t, queue.Push(Packet{payloadMultiBuffer: second, Seq: 1}))
	require.NoError(t, queue.Push(Packet{payloadMultiBuffer: first, Seq: 0}))

	// When
	got := make([]byte, len("first-packetsecond-packet"))
	_, err := io.ReadFull(queue, got)

	// Then
	require.NoError(t, err)
	require.Equal(t, "first-packetsecond-packet", string(got))
}

func Test_packetPayload_preservesAutoPlacementOrder(t *testing.T) {
	// Given
	var payload packetPayload
	payload.appendBytes([]byte("header-"))
	payload.appendBytes([]byte("cookie-"))
	body, err := readPacketBody(bytes.NewReader([]byte("body")), 4)
	require.NoError(t, err)
	payload.appendMultiBuffer(body)
	queue := NewUploadQueue(1)
	require.NoError(t, queue.Push(payload.packet(0)))

	// When
	got := make([]byte, len("header-cookie-body"))
	_, err = io.ReadFull(queue, got)

	// Then
	require.NoError(t, err)
	require.Equal(t, "header-cookie-body", string(got))
}

func Test_packetPayload_appendMultiBuffer_transfersWithoutAllocation(t *testing.T) {
	// Given
	block := buf.FromBytes([]byte("payload"))
	source := make(buf.MultiBuffer, 1)
	var payload packetPayload

	// When
	allocations := testing.AllocsPerRun(1000, func() {
		source[0] = block
		payload = packetPayload{}
		payload.appendMultiBuffer(source)
	})

	// Then
	require.Zero(t, allocations)
}

func Test_uploadQueue_releasesPooledPayloadWhenClosed(t *testing.T) {
	// Given
	queue := NewUploadQueue(1)
	require.NoError(t, queue.Close())
	block := buf.New()
	_, err := block.WriteString("payload")
	require.NoError(t, err)

	// When
	err = queue.Push(Packet{payloadMultiBuffer: buf.MultiBuffer{block}})

	// Then
	require.Error(t, err)
	require.True(t, block.IsEmpty())
}

func Test_uploadQueue_releasesPendingPooledPayloadOnClose(t *testing.T) {
	// Given
	queue := NewUploadQueue(2)
	queuedBlock := buf.New()
	_, err := queuedBlock.WriteString("queued")
	require.NoError(t, err)
	require.NoError(t, queue.Push(Packet{
		payloadMultiBuffer: buf.MultiBuffer{queuedBlock},
		Seq:                1,
	}))
	reorderedBlock := buf.New()
	_, err = reorderedBlock.WriteString("reordered")
	require.NoError(t, err)
	heap.Push(&queue.heap, Packet{
		payloadMultiBuffer: buf.MultiBuffer{reorderedBlock},
		Seq:                2,
	})

	// When
	err = queue.Close()

	// Then
	require.NoError(t, err)
	require.True(t, queuedBlock.IsEmpty())
	require.True(t, reorderedBlock.IsEmpty())
}

func Test_uploadQueue_releasesPooledPayloadOnOverflow(t *testing.T) {
	// Given
	queue := NewUploadQueue(1)
	blocks := make([]*buf.Buffer, 3)
	for index := range blocks {
		blocks[index] = buf.New()
		_, err := blocks[index].WriteString("payload")
		require.NoError(t, err)
		heap.Push(&queue.heap, Packet{
			payloadMultiBuffer: buf.MultiBuffer{blocks[index]},
			Seq:                uint64(index + 1),
		})
	}

	// When
	_, err := queue.Read(make([]byte, 1))

	// Then
	require.ErrorContains(t, err, "packet queue is too large")
	for _, block := range blocks {
		require.True(t, block.IsEmpty())
	}
}
