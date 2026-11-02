package courier

import (
	"encoding/json"
	"fmt"
	"strings"
)

const ProtocolVersion = 1
const DefaultMaxFrameBytes = 1 << 20

// Envelope is the broker-independent frame. Payload is a complete Thrift binary
// message, including the method header. JSON is intentionally only framing.
type Envelope struct {
	Version  int       `json:"version"`
	Kind     string    `json:"kind"`
	Service  string    `json:"service"`
	Method   string    `json:"method"`
	ID       string    `json:"id"`
	Session  string    `json:"session"`
	Deadline int64     `json:"deadline_unix_nano,omitempty"`
	Reply    Route     `json:"reply"`
	Payload  []byte    `json:"payload,omitempty"`
	Error    *RPCError `json:"error,omitempty"`
}

func EncodeEnvelope(e Envelope, limit int) ([]byte, error) {
	if e.Error != nil && len(e.Error.Code)+len(e.Error.Message) > limit {
		return nil, ErrTooLarge
	}
	if limit < 1 || len(e.Payload)+len(e.Service)+len(e.Method)+len(e.ID)+len(e.Session)+len(e.Reply.Topic) > limit {
		return nil, ErrTooLarge
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	if len(b) > limit {
		return nil, ErrTooLarge
	}
	if err := checkErrorReplyBudget(e, limit); err != nil {
		return nil, err
	}
	return b, nil
}
func DecodeEnvelope(b []byte, limit int) (Envelope, error) {
	var e Envelope
	if len(b) > limit {
		return e, ErrTooLarge
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("courier: invalid envelope: %w", err)
	}
	if e.Version != ProtocolVersion || e.ID == "" || e.Session == "" || e.Service == "" || e.Method == "" || (e.Kind != "request" && e.Kind != "response") {
		return e, &RPCError{Code: "protocol", Message: "invalid envelope fields or version"}
	}
	if err := checkErrorReplyBudget(e, limit); err != nil {
		return e, err
	}
	return e, nil
}

// Requests must leave room for an error with their original correlation fields.
// Validate incoming frames too: older or raw producers can bypass EncodeEnvelope.
func checkErrorReplyBudget(e Envelope, limit int) error {
	if e.Kind != "request" {
		return nil
	}
	e.Kind, e.Payload, e.Deadline = "response", nil, 0
	e.Error = &RPCError{Code: "method_not_found", Message: strings.Repeat("x", 64)}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(b) > limit {
		return ErrTooLarge
	}
	return nil
}
