package courier

import (
	"bytes"
	"fmt"
	"github.com/nextpkg/courier/internal/binread"
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
	c := binread.Cursor{Data: data, At: len(binaryEnvelopePrefix)}
	var v ReplyEnvelopeView
	version := c.Int32(envelopeFieldVersion)
	kind := c.Blob(envelopeFieldKind)
	v.Service = c.Blob(envelopeFieldService)
	v.Method = c.Blob(envelopeFieldMethod)
	v.ID = c.Blob(envelopeFieldID)
	v.Session = c.Blob(envelopeFieldSession)
	v.Deadline = c.Int64(envelopeFieldDeadline)
	v.Topic = c.Blob(envelopeFieldReplyTopic)
	v.Partition = c.Int32(envelopeFieldReplyPartition)
	v.Payload = c.Blob(envelopeFieldPayload)
	if c.Failed {
		return v, nil, false
	}
	if c.At < len(data) && data[c.At] != 0 {
		v.ErrorCode = c.Blob(envelopeFieldErrorCode)
		v.ErrorMessage = c.Blob(envelopeFieldErrorMessage)
		v.HasError = true
	}
	if !c.Done() {
		return v, nil, false
	}
	if version != ProtocolVersion || !bytes.Equal(kind, []byte("response")) || len(v.Service) == 0 || len(v.Method) == 0 || len(v.ID) == 0 || len(v.Session) == 0 {
		return ReplyEnvelopeView{}, fmt.Errorf("courier: invalid reply fields or version"), true
	}
	return v, nil, true
}
