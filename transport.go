package courier

import "context"

// Route is allocated before the client admits calls. Partition is used by Kafka.
type Route struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// Handler returns a response frame. A non-nil error means no response was safely
// produced, and the adapter must apply its documented delivery failure policy.
type Handler func(context.Context, []byte) ([]byte, error)

type Subscription interface{ Close(context.Context) error }
type Inbox interface {
	Subscription
	Route() Route
	// Done closes when reply delivery ends permanently, including broker close.
	Done() <-chan struct{}
}

// Transport instances are caller-owned and may be shared. Send must honor ctx;
// successful Send means only the adapter's documented publication guarantee.
// Listen must be ready before returning. Closing a subscription must stop new
// deliveries; context bounds waiting for handlers already executing.
type Transport interface {
	OpenInbox(context.Context, string, func([]byte)) (Inbox, error)
	Send(context.Context, string, Route, []byte) error
	Listen(context.Context, string, Handler) (Subscription, error)
}

// ServiceListener optionally installs all service handlers together. Transports
// that share a physical subscription can validate complete routing before
// advertising readiness. On error it must roll back all subscriptions.
type ServiceListener interface {
	ListenServices(context.Context, map[string]Handler) (Subscription, error)
}
