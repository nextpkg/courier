package kafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"

	"github.com/nextpkg/courier/transport/kafka/internal/wire"
)

// forwardRemote makes one attempt. Network failures produce status zero so the
// owner retries; invalid request construction remains a terminal error.
func (r *replyRouter) forwardRemote(ctx context.Context, f wire.Route, header, data []byte) (int, error) {
	status := 0
	if r.opts.BatchSize > 1 {
		status = r.enqueueForward(ctx, f.Endpoint, f.Instance, f.Expires, data)
	} else if r.rpc != nil {
		r.httpRequests.Add(1)
		r.httpMessages.Add(1)
		endpoint, err := url.Parse(f.Endpoint)
		if err != nil {
			return 0, err
		}
		result, err := r.rpc.forward(ctx, endpoint.Host, "Forward", header, data, int64(r.t.opts.MaxFrameBytes))
		if err == nil {
			status = result.status
		}
	} else {
		r.httpRequests.Add(1)
		r.httpMessages.Add(1)
		req, err := http.NewRequestWithContext(ctx, "POST", f.Endpoint+"/courier/reply", bytes.NewReader(data))
		if err != nil {
			return 0, err
		}
		req.Header.Set("X-Courier-Route", "base64:"+base64.StdEncoding.EncodeToString(header))
		resp, err := r.client.Do(req)
		if err == nil {
			status = resp.StatusCode
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
		}
	}
	return status, nil
}
