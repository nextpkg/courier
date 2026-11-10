package kafka

import (
	"fmt"
	"net/url"
	"time"
)

// normalizeReplyRouter validates configuration before opening any resources.
func normalizeReplyRouter(o ReplyRouterOptions, transport Options) (ReplyRouterOptions, *url.URL, error) {
	if o.ForwardProtocol == "" {
		o.ForwardProtocol = "http"
	}
	if o.ForwardProtocol != "http" && o.ForwardProtocol != "thrift" {
		return o, nil, fmt.Errorf("kafka: unknown reply forwarding protocol %q", o.ForwardProtocol)
	}
	if o.MaxReplyAge == 0 {
		o.MaxReplyAge = 30 * time.Second
	}
	if o.DedupEntries == 0 {
		o.DedupEntries = 4096
	}
	if o.Group == "" {
		o.Group = transport.Prefix + ".reply-router." + transport.ReplyTopic
	}
	u, err := url.Parse(o.AdvertiseURL)
	validEndpoint := err == nil && u != nil && u.Hostname() != "" && u.Port() != "" &&
		u.User == nil && u.RawQuery == "" && u.Fragment == "" &&
		(u.Path == "" || u.Path == "/") &&
		(u.Scheme == "https" || u.Scheme == "http" && o.AllowInsecureHTTP)
	validLimits := o.ListenAddress != "" &&
		o.MaxReplyAge >= time.Millisecond && o.MaxReplyAge <= transport.ProcessingTimeout &&
		o.DedupEntries >= 1 && o.DedupEntries <= 65536 &&
		o.BatchSize >= 0 && o.BatchSize <= 32 &&
		o.BatchConcurrency >= 0 && o.BatchConcurrency <= 2
	if !validEndpoint || !validLimits {
		return o, nil, fmt.Errorf("kafka: invalid reply router endpoint or limits")
	}
	if o.ForwardProtocol == "thrift" && (u.Scheme != "http" || o.ServerTLS != nil || o.ClientTLS != nil) {
		return o, nil, fmt.Errorf("kafka: thrift reply forwarding requires trusted plaintext endpoints; use http forwarding for TLS")
	}
	if u.Scheme == "https" && (o.ServerTLS == nil || len(o.ServerTLS.Certificates) == 0 && o.ServerTLS.GetCertificate == nil) {
		return o, nil, fmt.Errorf("kafka: HTTPS reply router requires server certificate")
	}
	return o, u, nil
}
