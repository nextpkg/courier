// Package brokeradmin is deployment/test tooling, isolated from RPC runtimes.
package brokeradmin

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func Create(ctx context.Context, c *kgo.Client, topics map[string]int32) error {
	q := kmsg.NewPtrCreateTopicsRequest()
	q.TimeoutMillis = 10000
	for name, n := range topics {
		q.Topics = append(q.Topics, kmsg.CreateTopicsRequestTopic{Topic: name, NumPartitions: n, ReplicationFactor: 1})
	}
	r, err := q.RequestWith(ctx, c)
	if err != nil {
		return err
	}
	for _, t := range r.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil && err != kerr.TopicAlreadyExists {
			return fmt.Errorf("create %s: %w", t.Topic, err)
		}
	}
	return nil
}
func Delete(ctx context.Context, c *kgo.Client, names []string) error {
	q := kmsg.NewPtrDeleteTopicsRequest()
	q.TimeoutMillis = 10000
	for _, name := range names {
		q.Topics = append(q.Topics, kmsg.DeleteTopicsRequestTopic{Topic: &name})
	}
	r, err := q.RequestWith(ctx, c)
	if err != nil {
		return err
	}
	for _, t := range r.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil && err != kerr.UnknownTopicOrPartition {
			return err
		}
	}
	return nil
}
func Names(ctx context.Context, c *kgo.Client) ([]string, error) {
	q := kmsg.NewPtrMetadataRequest()
	q.AllowAutoTopicCreation = false
	r, err := q.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, t := range r.Topics {
		if t.Topic != nil && !t.IsInternal {
			names = append(names, *t.Topic)
		}
	}
	sort.Strings(names)
	return names, nil
}
func Expand(ctx context.Context, c *kgo.Client, topic string, n int32) error {
	q := kmsg.NewPtrCreatePartitionsRequest()
	q.TimeoutMillis = 10000
	q.Topics = []kmsg.CreatePartitionsRequestTopic{{Topic: topic, Count: n}}
	r, err := q.RequestWith(ctx, c)
	if err != nil {
		return err
	}
	for _, t := range r.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return err
		}
	}
	return nil
}

// Committed returns the next offset to consume, or -1 if none was committed.
// Topic IDs keep this compatible with OffsetFetch v10 in current Kafka.
func Committed(ctx context.Context, c *kgo.Client, group, topic string, partition int32) (int64, error) {
	meta := kmsg.NewPtrMetadataRequest()
	meta.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	m, err := meta.RequestWith(ctx, c)
	if err != nil {
		return -1, err
	}
	var id [16]byte
	for _, t := range m.Topics {
		if t.Topic != nil && *t.Topic == topic {
			id = t.TopicID
		}
	}
	q := kmsg.NewPtrOffsetFetchRequest()
	q.Group = group
	q.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{partition}}}
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, TopicID: id, Partitions: []int32{partition}}}
	q.Groups = []kmsg.OffsetFetchRequestGroup{g}
	r, err := q.RequestWith(ctx, c)
	if err != nil {
		return -1, err
	}
	for _, g := range r.Groups {
		if g.Group != group {
			continue
		}
		if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
			return -1, err
		}
		for _, t := range g.Topics {
			if t.Topic != topic && t.TopicID != id {
				continue
			}
			for _, p := range t.Partitions {
				if p.Partition == partition {
					return p.Offset, kerr.ErrorForCode(p.ErrorCode)
				}
			}
		}
	}
	for _, t := range r.Topics {
		if t.Topic == topic {
			for _, p := range t.Partitions {
				if p.Partition == partition {
					return p.Offset, kerr.ErrorForCode(p.ErrorCode)
				}
			}
		}
	}
	return -1, nil
}
