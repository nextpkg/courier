package wire

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nextpkg/courier"
	"sync"
)

var RoutePrefix = []byte{0x80, 'R', 'T', 1}

type routeCodec struct {
	b *thrift.TMemoryBuffer
	p *thrift.TBinaryProtocol
}

var routeCodecs sync.Pool

func getRouteCodec() *routeCodec {
	c, _ := routeCodecs.Get().(*routeCodec)
	if c == nil {
		b := thrift.NewTMemoryBufferLen(256)
		c = &routeCodec{b: b, p: thrift.NewTBinaryProtocolConf(b, &thrift.TConfiguration{MaxMessageSize: MaxRouteBytes, MaxFrameSize: MaxRouteBytes})}
	}
	c.b.Reset()
	return c
}
func putRouteCodec(c *routeCodec) {
	if c.b.Cap() <= 4096 {
		c.b.Reset()
		routeCodecs.Put(c)
	}
}

// readString reads from the complete in-memory route, avoiding Thrift's
// intermediate byte slice. The returned string owns its bytes across pool reuse.
func (c *routeCodec) readString(ctx context.Context) (string, error) {
	size, err := c.p.ReadI32(ctx)
	if err != nil {
		return "", err
	}
	if size < 0 || size > MaxRouteBytes || int(size) > c.b.Len() {
		return "", fmt.Errorf("invalid route string length")
	}
	return string(c.b.Next(int(size))), nil
}
func EncodeRoute(f Route) ([]byte, error) {
	c := getRouteCodec()
	defer putRouteCodec(c)
	b, err := encodeBinaryRoute(c, f)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(b), nil
}

// The returned view is valid only while the caller owns c.
func encodeBinaryRoute(c *routeCodec, f Route) ([]byte, error) {
	p := c.p
	ctx := context.Background()
	c.b.Write(RoutePrefix)
	if err := p.WriteStructBegin(ctx, "ReplyRoute"); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Endpoint", thrift.STRING, 1); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, f.Endpoint); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Instance", thrift.STRING, 2); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, f.Instance); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Session", thrift.STRING, 3); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, f.Session); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "ID", thrift.STRING, 4); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, f.ID); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Topic", thrift.STRING, 5); err != nil {
		return nil, err
	}
	if err := p.WriteString(ctx, f.Topic); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Partition", thrift.I32, 6); err != nil {
		return nil, err
	}
	if err := p.WriteI32(ctx, f.Partition); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldBegin(ctx, "Expires", thrift.I64, 7); err != nil {
		return nil, err
	}
	if err := p.WriteI64(ctx, f.Expires); err != nil {
		return nil, err
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return nil, err
	}
	if err := p.WriteStructEnd(ctx); err != nil {
		return nil, err
	}
	if c.b.Len() > MaxRouteBytes {
		return nil, courier.ErrTooLarge
	}
	return c.b.Bytes(), nil
}
func DecodeRoute(data []byte) (Route, error) {
	if len(data) > MaxRouteBytes {
		return Route{}, fmt.Errorf("routing header too large")
	}
	if !bytes.HasPrefix(data, RoutePrefix) {
		return Route{}, fmt.Errorf("invalid binary route prefix")
	}
	f := Route{}
	c := getRouteCodec()
	defer putRouteCodec(c)
	p := c.p
	ctx := context.Background()
	c.b.Write(data[len(RoutePrefix):])
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return f, err
	}
	var seen uint16
	for {
		_, ty, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return f, err
		}
		if ty == thrift.STOP {
			break
		}
		if id >= 1 && id <= 7 {
			mask := uint16(1) << uint(id-1)
			if seen&mask != 0 {
				return f, fmt.Errorf("duplicate route field")
			}
			seen |= mask
		}
		switch id {
		case 1:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := c.readString(ctx)
			if err != nil {
				return f, err
			}
			f.Endpoint = v
		case 2:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := c.readString(ctx)
			if err != nil {
				return f, err
			}
			f.Instance = v
		case 3:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := c.readString(ctx)
			if err != nil {
				return f, err
			}
			f.Session = v
		case 4:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := c.readString(ctx)
			if err != nil {
				return f, err
			}
			f.ID = v
		case 5:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := c.readString(ctx)
			if err != nil {
				return f, err
			}
			f.Topic = v
		case 6:
			if ty != thrift.I32 {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := p.ReadI32(ctx)
			if err != nil {
				return f, err
			}
			f.Partition = v
		case 7:
			if ty != thrift.I64 {
				return f, fmt.Errorf("invalid route field type")
			}
			v, err := p.ReadI64(ctx)
			if err != nil {
				return f, err
			}
			f.Expires = v
		default:
			if err := p.Skip(ctx, ty); err != nil {
				return f, err
			}
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return f, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return f, err
	}
	if c.b.Len() != 0 || seen != 127 {
		return f, fmt.Errorf("incomplete or trailing route")
	}
	return f, nil
}

const MaxRouteBytes = 768

type Route struct {
	Endpoint  string
	Instance  string
	Session   string
	ID        string
	Topic     string
	Partition int32
	Expires   int64
}
