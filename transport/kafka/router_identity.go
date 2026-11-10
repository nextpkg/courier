package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/nextpkg/courier/transport/kafka/internal/wire"
	"net/url"
)

const maxRouteIdentities = 64

type routeIdentity struct{ endpoint, instance, session string }
type routeIdentityTable map[string]*routeIdentity

// Snapshots are immutable after publication, so hits need no shared read lock.
// Keys contain only the stable v1 prefix (Endpoint/Instance/Session), never ID
// or expiry. A full cache starts a new generation; it is not an unbounded intern
// table. Router options are fixed for the lifetime of this per-router cache.
func (r *replyRouter) routeIdentity(prefix, endpoint, instance, session []byte) (*routeIdentity, error) {
	if table := r.identities.Load(); table != nil {
		if hit := (*table)[string(prefix)]; hit != nil {
			return hit, nil
		}
	}
	value := &routeIdentity{string(endpoint), string(instance), string(session)}
	if err := r.validateRouteEndpoint(value.endpoint); err != nil {
		return nil, err
	}
	r.identityMu.Lock()
	defer r.identityMu.Unlock()
	current := r.identities.Load()
	if current != nil {
		if hit := (*current)[string(prefix)]; hit != nil {
			return hit, nil
		}
	}
	next := make(routeIdentityTable)
	if current != nil && len(*current) < maxRouteIdentities {
		for key, value := range *current {
			next[key] = value
		}
	}
	next[string(prefix)] = value
	r.identities.Store(&next)
	return value, nil
}

func (r *replyRouter) validateRouteEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.Hostname() == "" || u.Port() == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && r.opts.AllowInsecureHTTP)) {
		return fmt.Errorf("invalid routing endpoint")
	}
	return nil
}

// The writer emits fields 1..7 in order. This cursor recognizes exactly that
// encoding without copying it through a Thrift scratch buffer. Any other shape
// falls back to the full decoder, retaining reordered/unknown-field support and
// malformed-frame checks. Prefix and total-size checks run in decodeRoute first.
func (r *replyRouter) decodeCanonicalRoute(data []byte) (wire.Route, error, bool) {
	at := len(wire.RoutePrefix)
	takeString := func(id byte) ([]byte, bool) {
		if len(data)-at < 7 || data[at] != 11 || data[at+1] != 0 || data[at+2] != id {
			return nil, false
		}
		n := uint64(binary.BigEndian.Uint32(data[at+3 : at+7]))
		at += 7
		if n > uint64(len(data)-at) {
			return nil, false
		}
		value := data[at : at+int(n)]
		at += int(n)
		return value, true
	}
	endpoint, ok := takeString(1)
	if !ok {
		return wire.Route{}, nil, false
	}
	instance, ok := takeString(2)
	if !ok {
		return wire.Route{}, nil, false
	}
	session, ok := takeString(3)
	if !ok {
		return wire.Route{}, nil, false
	}
	prefix := data[:at]
	id, ok := takeString(4)
	if !ok {
		return wire.Route{}, nil, false
	}
	topic, ok := takeString(5)
	if !ok {
		return wire.Route{}, nil, false
	}
	// I32 Partition, I64 Expires, and STOP must consume the complete frame.
	if len(data)-at != 19 || data[at] != 8 || data[at+1] != 0 || data[at+2] != 6 || data[at+7] != 10 || data[at+8] != 0 || data[at+9] != 7 || data[at+18] != 0 {
		return wire.Route{}, nil, false
	}
	partition := int32(binary.BigEndian.Uint32(data[at+3 : at+7]))
	expires := int64(binary.BigEndian.Uint64(data[at+10 : at+18]))
	if len(instance) == 0 || len(session) == 0 || len(id) == 0 || !bytes.Equal(topic, []byte(r.t.opts.ReplyTopic)) || partition < 0 || expires <= 0 {
		return wire.Route{}, fmt.Errorf("invalid routing fields"), true
	}
	identity, err := r.routeIdentity(prefix, endpoint, instance, session)
	if err != nil {
		return wire.Route{}, err, true
	}
	// ID has independent storage: the dedup ring may retain it after this route
	// completes. Stable strings are cache-owned and never alias receive buffers.
	return wire.Route{Endpoint: identity.endpoint, Instance: identity.instance, Session: identity.session, ID: string(id), Topic: r.t.opts.ReplyTopic, Partition: partition, Expires: expires}, nil, true
}
