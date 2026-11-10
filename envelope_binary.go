package courier

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
)

var binaryEnvelopePrefix = []byte{0x80, 'C', 'R', 1}

func hasBinaryEnvelopePrefix(b []byte) bool { return bytes.HasPrefix(b, binaryEnvelopePrefix) }

// EncodeThriftEnvelope is an explicit alias for the binary-only EncodeEnvelope.
func EncodeThriftEnvelope(e Envelope, limit int) ([]byte, error) {
	return EncodeEnvelope(e, limit)
}
func encodeBinaryEnvelope(e Envelope, limit int) ([]byte, error) {
	if int64(e.Version) < -2147483648 || int64(e.Version) > 2147483647 {
		return nil, fmt.Errorf("courier: version out of range")
	}
	b, p := binaryProtocol(nil, limit)
	defer releaseBinaryProtocol(b)
	if _, err := b.Write(binaryEnvelopePrefix); err != nil {
		return nil, err
	}
	ctx := context.Background()
	if err := p.WriteStructBegin(ctx, "Envelope"); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Version", thrift.I32, 1); err != nil {
		return nil, err
	}
	if err := p.WriteI32(ctx, int32(e.Version)); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Kind", thrift.STRING, 2); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, e.Kind); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Service", thrift.STRING, 3); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, e.Service); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Method", thrift.STRING, 4); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, e.Method); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "ID", thrift.STRING, 5); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, e.ID); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Session", thrift.STRING, 6); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, e.Session); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Deadline", thrift.I64, 7); err != nil {
		return nil, err
	}
	if err := p.WriteI64(ctx, e.Deadline); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "ReplyTopic", thrift.STRING, 8); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, e.Reply.Topic); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "ReplyPartition", thrift.I32, 9); err != nil {
		return nil, err
	}
	if err := p.WriteI32(ctx, e.Reply.Partition); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Payload", thrift.STRING, 10); err != nil {
		return nil, err
	}
	if err := p.WriteBinary(ctx, e.Payload); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if e.Error != nil {
		if err := p.WriteFieldBegin(ctx, "ErrorCode", thrift.STRING, 11); err != nil {
			return nil, err
		}
		if err := p.WriteString(ctx, e.Error.Code); err != nil {
			return nil, err
		}
		if err := p.WriteFieldEnd(ctx); err != nil {
			return nil, err
		}
		if err := p.WriteFieldBegin(ctx, "ErrorMessage", thrift.STRING, 12); err != nil {
			return nil, err
		}
		if err := p.WriteString(ctx, e.Error.Message); err != nil {
			return nil, err
		}
		if err := p.WriteFieldEnd(ctx); err != nil {
			return nil, err
		}
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return nil, err
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
		if id >= 1 && id <= 12 {
			mask := uint16(1) << uint(id-1)
			if seen&mask != 0 {
				return Envelope{}, fmt.Errorf("courier: duplicate envelope field")
			}
			seen |= mask
		}
		switch id {
		case 1:
			if ty != thrift.I32 {
				return Envelope{}, fmt.Errorf("courier: invalid type for Version")
			}
			v, err := p.ReadI32(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Version = int(v)
		case 2:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Kind")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Kind = v
		case 3:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Service")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Service = v
		case 4:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Method")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Method = v
		case 5:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for ID")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.ID = v
		case 6:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Session")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Session = v
		case 7:
			if ty != thrift.I64 {
				return Envelope{}, fmt.Errorf("courier: invalid type for Deadline")
			}
			v, err := p.ReadI64(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Deadline = v
		case 8:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for ReplyTopic")
			}
			v, err := p.ReadString(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Reply.Topic = v
		case 9:
			if ty != thrift.I32 {
				return Envelope{}, fmt.Errorf("courier: invalid type for ReplyPartition")
			}
			v, err := p.ReadI32(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Reply.Partition = v
		case 10:
			if ty != thrift.STRING {
				return Envelope{}, fmt.Errorf("courier: invalid type for Payload")
			}
			v, err := p.ReadBinary(ctx)
			if err != nil {
				return Envelope{}, err
			}
			e.Payload = v
		case 11:
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
		case 12:
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
	if b.Len() != 0 || seen&0x1ff != 0x1ff || (seen&(3<<10) != 0 && seen&(3<<10) != (3<<10)) {
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
