package kafka

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// Batch payload: count, then instance length/bytes, int64 expiry and envelope
// length/bytes. The Kafka routing header is validated before enqueue, not sent
// again over this hop. Metadata views borrow the complete received batch body.
const maxPooledBatchCapacity = 64 << 10

type binaryBatchBuffer struct{ data []byte }

var binaryBatchBuffers sync.Pool

// borrowBinaryBatch exclusively owns the encoded bytes until release. Callers
// must finish all synchronous I/O before returning the buffer.
func borrowBinaryBatch(messages []batchMessage) *binaryBatchBuffer {
	b, _ := binaryBatchBuffers.Get().(*binaryBatchBuffer)
	if b == nil {
		b = &binaryBatchBuffer{}
	}
	b.data = appendBinaryBatch(b.data[:0], messages)
	return b
}

// Retain at most 64 KiB per entry, including after cancellation or send failure.
func releaseBinaryBatch(b *binaryBatchBuffer) {
	if cap(b.data) > maxPooledBatchCapacity {
		b.data = nil
	} else {
		b.data = b.data[:0]
	}
	binaryBatchBuffers.Put(b)
}

func encodeBinaryBatch(messages []batchMessage) []byte {
	return appendBinaryBatch(nil, messages)
}
func appendBinaryBatch(dst []byte, messages []batchMessage) []byte {
	size := 4
	for _, m := range messages {
		size += 16 + len(m.Instance) + len(m.Data)
	}
	if cap(dst) < size {
		dst = make([]byte, 0, size)
	}
	b := binary.BigEndian.AppendUint32(dst[:0], uint32(len(messages)))
	for _, m := range messages {
		b = binary.BigEndian.AppendUint32(b, uint32(len(m.Instance)))
		b = append(b, m.Instance...)
		b = binary.BigEndian.AppendUint64(b, uint64(m.Expires))
		b = binary.BigEndian.AppendUint32(b, uint32(len(m.Data)))
		b = append(b, m.Data...)
	}
	return b
}

// Header/Data slices borrow the batch body. Finish delivery before releasing
// that body; an asynchronous consumer must retain independently owned data.
type receivedBatchMessage struct {
	Instance []byte
	Expires  int64
	Data     []byte
}

func decodeBinaryBatch(b []byte, max int) ([]receivedBatchMessage, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("missing batch count")
	}
	n := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	if n < 1 || n > 32 {
		return nil, fmt.Errorf("invalid batch count")
	}
	out := make([]receivedBatchMessage, 0, n)
	take := func(limit int) ([]byte, error) {
		if len(b) < 4 {
			return nil, fmt.Errorf("missing field length")
		}
		size := uint64(binary.BigEndian.Uint32(b))
		b = b[4:]
		if size > uint64(limit) || size > uint64(len(b)) {
			return nil, fmt.Errorf("invalid field size")
		}
		v := b[:int(size):int(size)]
		b = b[int(size):]
		return v, nil
	}
	for i := 0; i < n; i++ {
		h, e := take(maxRouteBytes)
		if e != nil {
			return nil, e
		}
		if len(b) < 8 {
			return nil, fmt.Errorf("missing expiry")
		}
		expires := int64(binary.BigEndian.Uint64(b[:8]))
		b = b[8:]
		d, e := take(max)
		if e != nil {
			return nil, e
		}
		out = append(out, receivedBatchMessage{Instance: h, Expires: expires, Data: d})
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("trailing batch data")
	}
	return out, nil
}
func encodeBinaryStatuses(statuses []int) []byte {
	b := make([]byte, 0, 4*len(statuses))
	for _, s := range statuses {
		b = binary.BigEndian.AppendUint32(b, uint32(s))
	}
	return b
}
func decodeBinaryStatuses(b []byte, n int) ([]int, error) {
	if len(b) != 4*n {
		return nil, fmt.Errorf("invalid batch results")
	}
	out := make([]int, n)
	for i := range out {
		out[i] = int(binary.BigEndian.Uint32(b[4*i:]))
	}
	return out, nil
}
func (r *replyRouter) deliverBinaryBatch(body []byte) (int, []byte) {
	if int64(len(body)) > r.batchLimit() {
		return 413, nil
	}
	messages, err := decodeBinaryBatch(body, r.t.opts.MaxFrameBytes)
	if err != nil {
		return 400, nil
	}
	select {
	case r.inbound <- struct{}{}:
		defer func() { <-r.inbound }()
	default:
		return 429, nil
	}
	statuses := make([]int, len(messages))
	for i, m := range messages {
		statuses[i] = r.deliverCompact(m)
	}
	return 200, encodeBinaryStatuses(statuses)
}
