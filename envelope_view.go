package courier

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ReplyEnvelopeView borrows immutable bytes from the complete input frame.
// Keep the input alive and unchanged while using the view. Do not retain its
// slices in async callbacks, pending maps or dedup rings; use OwnedEnvelope.
type ReplyEnvelopeView struct {
	Service, Method, ID, Session, Topic, Payload []byte
	Deadline                                     int64
	Partition                                    int32
	ErrorCode, ErrorMessage                      []byte
	HasError                                     bool
	owned                                        *Envelope
}

// OwnedEnvelope materializes the validated reply with independent strings and
// payload storage, including on the noncanonical full-decoder fallback path.
func (v ReplyEnvelopeView) OwnedEnvelope() Envelope {
	if v.owned != nil {
		e := *v.owned
		e.Payload = bytes.Clone(e.Payload)
		if e.Error != nil {
			copyError := *e.Error
			e.Error = &copyError
		}
		return e
	}
	e := Envelope{Version: ProtocolVersion, Kind: "response", Service: string(v.Service), Method: string(v.Method), ID: string(v.ID), Session: string(v.Session), Deadline: v.Deadline, Reply: Route{Topic: string(v.Topic), Partition: v.Partition}, Payload: bytes.Clone(v.Payload)}
	if v.HasError {
		e.Error = &RPCError{Code: string(v.ErrorCode), Message: string(v.ErrorMessage)}
	}
	return e
}

type envelopeCursor struct {
	data []byte
	at   int
}

func (c *envelopeCursor) fixed(id, typ byte, n int) ([]byte, bool) {
	if len(c.data)-c.at < n+3 || c.data[c.at] != typ || c.data[c.at+1] != 0 || c.data[c.at+2] != id {
		return nil, false
	}
	c.at += 3
	v := c.data[c.at : c.at+n : c.at+n]
	c.at += n
	return v, true
}
func (c *envelopeCursor) blob(id byte) ([]byte, bool) {
	length, ok := c.fixed(id, 11, 4)
	if !ok {
		return nil, false
	}
	n := uint64(binary.BigEndian.Uint32(length))
	if n > uint64(len(c.data)-c.at) {
		return nil, false
	}
	v := c.data[c.at : c.at+int(n) : c.at+int(n)]
	c.at += int(n)
	return v, true
}

// DecodeReplyEnvelopeView validates the whole envelope shape and required
// fields before exposing borrowed data. Canonical writer order needs no scratch
// copy or string allocation; reordered/extended frames use the full decoder.
// This API accepts replies only and does not decode opaque business payloads.
func DecodeReplyEnvelopeView(data []byte, limit int) (ReplyEnvelopeView, error) {
	if len(data) > limit {
		return ReplyEnvelopeView{}, ErrTooLarge
	}
	if !hasBinaryEnvelopePrefix(data) {
		return ReplyEnvelopeView{}, fmt.Errorf("courier: invalid binary envelope prefix")
	}
	if v, err, ok := canonicalReplyView(data); ok {
		return v, err
	}
	e, err := DecodeEnvelope(data, limit)
	if err != nil {
		return ReplyEnvelopeView{}, err
	}
	if e.Kind != "response" {
		return ReplyEnvelopeView{}, fmt.Errorf("courier: expected reply envelope")
	}
	v := ReplyEnvelopeView{Service: []byte(e.Service), Method: []byte(e.Method), ID: []byte(e.ID), Session: []byte(e.Session), Topic: []byte(e.Reply.Topic), Partition: e.Reply.Partition, Deadline: e.Deadline, Payload: e.Payload, owned: &e}
	if e.Error != nil {
		v.HasError = true
		v.ErrorCode = []byte(e.Error.Code)
		v.ErrorMessage = []byte(e.Error.Message)
	}
	return v, nil
}

func canonicalReplyView(data []byte) (ReplyEnvelopeView, error, bool) {
	c := envelopeCursor{data: data, at: len(binaryEnvelopePrefix)}
	var v ReplyEnvelopeView
	version, ok := c.fixed(1, 8, 4)
	if !ok {
		return v, nil, false
	}
	kind, ok := c.blob(2)
	if !ok {
		return v, nil, false
	}
	if v.Service, ok = c.blob(3); !ok {
		return v, nil, false
	}
	if v.Method, ok = c.blob(4); !ok {
		return v, nil, false
	}
	if v.ID, ok = c.blob(5); !ok {
		return v, nil, false
	}
	if v.Session, ok = c.blob(6); !ok {
		return v, nil, false
	}
	deadline, ok := c.fixed(7, 10, 8)
	if !ok {
		return v, nil, false
	}
	v.Deadline = int64(binary.BigEndian.Uint64(deadline))
	if v.Topic, ok = c.blob(8); !ok {
		return v, nil, false
	}
	partition, ok := c.fixed(9, 8, 4)
	if !ok {
		return v, nil, false
	}
	v.Partition = int32(binary.BigEndian.Uint32(partition))
	if v.Payload, ok = c.blob(10); !ok {
		return v, nil, false
	}
	if c.at < len(data) && data[c.at] != 0 {
		if v.ErrorCode, ok = c.blob(11); !ok {
			return v, nil, false
		}
		if v.ErrorMessage, ok = c.blob(12); !ok {
			return v, nil, false
		}
		v.HasError = true
	}
	if len(data)-c.at != 1 || data[c.at] != 0 {
		return v, nil, false
	}
	if int32(binary.BigEndian.Uint32(version)) != ProtocolVersion || !bytes.Equal(kind, []byte("response")) || len(v.Service) == 0 || len(v.Method) == 0 || len(v.ID) == 0 || len(v.Session) == 0 {
		return ReplyEnvelopeView{}, fmt.Errorf("courier: invalid reply fields or version"), true
	}
	return v, nil, true
}
