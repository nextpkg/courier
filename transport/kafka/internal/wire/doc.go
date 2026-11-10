// Package wire encodes Kafka reply routes, forwarded batches and status lists.
// It validates wire structure and owns codec pools; delivery policy, routing
// identity caches, scheduling and acknowledgements belong to the kafka package.
package wire
