package wire

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/internal/binread"
	"github.com/nextpkg/courier/internal/binwrite"
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
func (c *routeCodec) readValue(ctx context.Context, err *error) (v string) {
	if *err == nil {
		v, *err = c.readString(ctx)
	}
	return
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
	c.b.Write(RoutePrefix)
	w := binwrite.Writer{Protocol: c.p, Context: context.Background()}
	w.StringField(RouteFieldEndpoint, f.Endpoint)
	w.StringField(RouteFieldInstance, f.Instance)
	w.StringField(RouteFieldSession, f.Session)
	w.StringField(RouteFieldID, f.ID)
	w.StringField(RouteFieldTopic, f.Topic)
	w.Int32Field(RouteFieldPartition, f.Partition)
	w.Int64Field(RouteFieldExpires, f.Expires)
	w.Stop()
	if w.Err != nil {
		return nil, w.Err
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
	r := binread.Reader{Protocol: p, Context: ctx}
	var seen uint16
	for {
		_, ty, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return f, err
		}
		if ty == thrift.STOP {
			break
		}
		if id >= RouteFieldEndpoint && id <= RouteFieldExpires {
			mask := uint16(1) << uint(id-RouteFieldEndpoint)
			if seen&mask != 0 {
				return f, fmt.Errorf("duplicate route field")
			}
			seen |= mask
		}
		switch id {
		case RouteFieldEndpoint:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			f.Endpoint = c.readValue(ctx, &r.Err)
		case RouteFieldInstance:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			f.Instance = c.readValue(ctx, &r.Err)
		case RouteFieldSession:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			f.Session = c.readValue(ctx, &r.Err)
		case RouteFieldID:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			f.ID = c.readValue(ctx, &r.Err)
		case RouteFieldTopic:
			if ty != thrift.STRING {
				return f, fmt.Errorf("invalid route field type")
			}
			f.Topic = c.readValue(ctx, &r.Err)
		case RouteFieldPartition:
			if ty != thrift.I32 {
				return f, fmt.Errorf("invalid route field type")
			}
			v := r.Int32()
			if r.Err != nil {
				return f, r.Err
			}
			f.Partition = v
		case RouteFieldExpires:
			if ty != thrift.I64 {
				return f, fmt.Errorf("invalid route field type")
			}
			v := r.Int64()
			if r.Err != nil {
				return f, r.Err
			}
			f.Expires = v
		default:
			if err := p.Skip(ctx, ty); err != nil {
				return f, err
			}
		}
		if r.Err != nil {
			return f, r.Err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return f, err
		}
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return f, err
	}
	if c.b.Len() != 0 || seen != routeRequiredFields {
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
