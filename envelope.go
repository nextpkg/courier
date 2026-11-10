package courier

import (
	"fmt"
	"strings"
)

const ProtocolVersion = 1
const DefaultMaxFrameBytes = 1 << 20

// Envelope is the broker-independent frame. Payload is a complete Thrift binary
// message, including the method header. The outer envelope also uses Thrift binary framing.
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
	return encodeBinaryEnvelope(e, limit)
}

func DecodeEnvelope(b []byte, limit int) (Envelope, error) {
	if len(b) > limit {
		return Envelope{}, ErrTooLarge
	}
	if !hasBinaryEnvelopePrefix(b) {
		return Envelope{}, fmt.Errorf("courier: invalid binary envelope prefix")
	}
	return decodeBinaryEnvelope(b, limit)
}

// Requests must leave room for an error with their original correlation fields.
// Validate incoming frames too: older or raw producers can bypass EncodeEnvelope.
func checkErrorReplyBudget(e Envelope, limit int) error {
	if e.Kind != "request" {
		return nil
	}
	// Binary string bytes are encoded directly. The fixed framing and
	// bounded error below fit within 512 bytes.
	// Use a conservative bound; retain exact serialization near the limit.
	if limit >= 512 {
		remaining := limit - 512
		fits := true
		for _, field := range []string{e.Service, e.Method, e.ID, e.Session, e.Reply.Topic} {
			if len(field) > remaining {
				fits = false
				break
			}
			remaining -= len(field)
		}
		if fits {
			return nil
		}
	}
	e.Kind, e.Payload, e.Deadline = "response", nil, 0
	e.Error = &RPCError{Code: "method_not_found", Message: strings.Repeat("x", 64)}
	b, err := encodeBinaryEnvelope(e, limit)
	if err != nil {
		return err
	}
	if len(b) > limit {
		return ErrTooLarge
	}
	return nil
}
