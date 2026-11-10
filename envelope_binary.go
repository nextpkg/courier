package courier

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nextpkg/courier/internal/binwrite"
)

var binaryEnvelopePrefix = []byte{0x80, 'C', 'R', 1}

func hasBinaryEnvelopePrefix(b []byte) bool { return bytes.HasPrefix(b, binaryEnvelopePrefix) }

func encodeBinaryEnvelope(e Envelope, limit int) ([]byte, error) {
	if int64(e.Version) < -2147483648 || int64(e.Version) > 2147483647 {
		return nil, fmt.Errorf("courier: version out of range")
	}
	b, p := binaryProtocol(nil, limit)
	defer releaseBinaryProtocol(b)
	if _, err := b.Write(binaryEnvelopePrefix); err != nil {
		return nil, err
	}
	w := binwrite.Writer{Protocol: p, Context: context.Background()}
	w.Int32Field(envelopeFieldVersion, int32(e.Version))
	w.StringField(envelopeFieldKind, e.Kind)
	w.StringField(envelopeFieldService, e.Service)
	w.StringField(envelopeFieldMethod, e.Method)
	w.StringField(envelopeFieldID, e.ID)
	w.StringField(envelopeFieldSession, e.Session)
	w.Int64Field(envelopeFieldDeadline, e.Deadline)
	w.StringField(envelopeFieldReplyTopic, e.Reply.Topic)
	w.Int32Field(envelopeFieldReplyPartition, e.Reply.Partition)
	w.BinaryField(envelopeFieldPayload, e.Payload)
	if e.Error != nil {
		w.StringField(envelopeFieldErrorCode, e.Error.Code)
		w.StringField(envelopeFieldErrorMessage, e.Error.Message)
	}
	w.Stop()
	if w.Err != nil {
		return nil, w.Err
	}
	if err := checkErrorReplyBudget(e, limit); err != nil {
		return nil, err
	}
	// The codec lease ends on return; the encoded frame must outlive that lease.
	return bytes.Clone(b.Bytes()), nil
}
func decodeBinaryEnvelope(data []byte, limit int) (Envelope, error) {
	e := Envelope{}
	b, p := binaryProtocol(data[len(binaryEnvelopePrefix):], limit)
	defer releaseBinaryProtocol(b)
	ctx := context.Background()
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return Envelope{}, err
	}
	var seen uint16
	for {
		_, ty, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return Envelope{}, err
		}
		if ty == thrift.STOP {
			break
		}
		if id >= envelopeFieldVersion && id <= envelopeFieldErrorMessage {
			mask := uint16(1) << uint(id-envelopeFieldVersion)
			if seen&mask != 0 {
				return Envelope{}, fmt.Errorf("courier: duplicate envelope field")
			}
			seen |= mask
		}
		switch id {
		case envelopeFieldVersion:
			if ty != thrift.I32 {
				return Envelope{}, fmt.Errorf("courier: invalid type for Version")
			}
			v, err := p.ReadI32(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Version = int(v)
		case envelopeFieldKind:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Kind")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Kind = v
		case envelopeFieldService:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Service")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Service = v
		case envelopeFieldMethod:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Method")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Method = v
		case envelopeFieldID:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for ID")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.ID = v
		case envelopeFieldSession:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Session")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Session = v
		case envelopeFieldDeadline:
			if ty != thrift.I64 {
				return Envelope{}, fmt.Errorf("courier: invalid type for Deadline")
			}
			v, err := p.ReadI64(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Deadline = v
		case envelopeFieldReplyTopic:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for ReplyTopic")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Reply.Topic = v
		case envelopeFieldReplyPartition:
			if ty != thrift.I32 {
				return Envelope{}, fmt.Errorf("courier: invalid type for ReplyPartition")
			}
			v, err := p.ReadI32(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Reply.Partition = v
		case envelopeFieldPayload:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Payload")
			}
			v, err := p.ReadBinary(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Payload = v
		case envelopeFieldErrorCode:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for ErrorCode")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			if e.Error == nil {
				e.Error = &RPCError{}
			}
			e.Error.Code = v
		case envelopeFieldErrorMessage:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for ErrorMessage")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			if e.Error == nil {
				e.Error = &RPCError{}
			}
			e.Error.Message = v
		default:
			if err := p.Skip(ctx, ty); err != nil {
				return Envelope{}, err
			}
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return Envelope{}, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return Envelope{}, err
	}
	if b.Len() != 0 || seen&envelopeRequiredFields != envelopeRequiredFields || (seen&envelopeErrorFields != 0 && seen&envelopeErrorFields != envelopeErrorFields) {
		return Envelope{}, fmt.Errorf("courier: incomplete or trailing binary envelope")
	}
	if e.Version != ProtocolVersion || e.ID == "" || e.Session == "" || e.Service == "" || e.Method == "" || (e.Kind != "request" && e.Kind != "response") {
		return Envelope{}, fmt.Errorf("courier: invalid envelope fields or version")
	}
	if err := checkErrorReplyBudget(e, limit); err != nil {
		return Envelope{}, err
	}
	return e, nil
}
