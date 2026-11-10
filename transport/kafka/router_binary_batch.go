package kafka

import "github.com/nextpkg/courier/transport/kafka/internal/wire"

func (r *replyRouter) deliverBinaryBatch(body []byte) (int, []byte) {
	if int64(len(body)) > r.batchLimit() {
		return 413, nil
	}
	messages, err := wire.DecodeBatch(body, r.t.opts.MaxFrameBytes)
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
	return 200, wire.EncodeStatuses(statuses)
}
