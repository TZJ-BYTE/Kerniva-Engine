package server

import (
	"errors"
	"path"
	"sort"
	"sync"

	"github.com/TZJ-BYTE/Kerniva/internal/protocol"
)

var errPubSubWriterUnavailable = errors.New("ERR pubsub writer unavailable")

type PubSubWriter interface {
	WriteAsync(payload []byte) error
}

type PubSubAck struct {
	Kind  string
	Name  *string
	Count int64
}

type PubSubHub struct {
	mu       sync.RWMutex
	channels map[string]map[*ConnectionContext]struct{}
	patterns map[string]map[*ConnectionContext]struct{}
}

func NewPubSubHub() *PubSubHub {
	return &PubSubHub{
		channels: make(map[string]map[*ConnectionContext]struct{}),
		patterns: make(map[string]map[*ConnectionContext]struct{}),
	}
}

func (h *PubSubHub) Subscribe(ctx *ConnectionContext, channels [][]byte) []PubSubAck {
	if h == nil || ctx == nil {
		return nil
	}

	acks := make([]PubSubAck, 0, len(channels))

	h.mu.Lock()
	defer h.mu.Unlock()

	ctx.pubsubMu.Lock()
	defer ctx.pubsubMu.Unlock()

	for _, raw := range channels {
		channel := string(raw)
		if _, ok := ctx.pubsubChannels[channel]; !ok {
			ctx.pubsubChannels[channel] = struct{}{}
			subscribers := h.channels[channel]
			if subscribers == nil {
				subscribers = make(map[*ConnectionContext]struct{})
				h.channels[channel] = subscribers
			}
			subscribers[ctx] = struct{}{}
		}
		channelCopy := channel
		acks = append(acks, PubSubAck{
			Kind:  "subscribe",
			Name:  &channelCopy,
			Count: ctx.pubsubSubscriptionCountLocked(),
		})
	}

	return acks
}

func (h *PubSubHub) PSubscribe(ctx *ConnectionContext, patterns [][]byte) []PubSubAck {
	if h == nil || ctx == nil {
		return nil
	}

	acks := make([]PubSubAck, 0, len(patterns))

	h.mu.Lock()
	defer h.mu.Unlock()

	ctx.pubsubMu.Lock()
	defer ctx.pubsubMu.Unlock()

	for _, raw := range patterns {
		pattern := string(raw)
		if _, ok := ctx.pubsubPatterns[pattern]; !ok {
			ctx.pubsubPatterns[pattern] = struct{}{}
			subscribers := h.patterns[pattern]
			if subscribers == nil {
				subscribers = make(map[*ConnectionContext]struct{})
				h.patterns[pattern] = subscribers
			}
			subscribers[ctx] = struct{}{}
		}
		patternCopy := pattern
		acks = append(acks, PubSubAck{
			Kind:  "psubscribe",
			Name:  &patternCopy,
			Count: ctx.pubsubSubscriptionCountLocked(),
		})
	}

	return acks
}

func (h *PubSubHub) Unsubscribe(ctx *ConnectionContext, channels [][]byte) []PubSubAck {
	if h == nil || ctx == nil {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	ctx.pubsubMu.Lock()
	defer ctx.pubsubMu.Unlock()

	targets := make([]string, 0, len(channels))
	if len(channels) == 0 {
		for channel := range ctx.pubsubChannels {
			targets = append(targets, channel)
		}
		sort.Strings(targets)
	} else {
		for _, raw := range channels {
			targets = append(targets, string(raw))
		}
	}

	if len(targets) == 0 {
		return []PubSubAck{{Kind: "unsubscribe", Name: nil, Count: ctx.pubsubSubscriptionCountLocked()}}
	}

	acks := make([]PubSubAck, 0, len(targets))
	for _, channel := range targets {
		if _, ok := ctx.pubsubChannels[channel]; ok {
			delete(ctx.pubsubChannels, channel)
			if subscribers := h.channels[channel]; subscribers != nil {
				delete(subscribers, ctx)
				if len(subscribers) == 0 {
					delete(h.channels, channel)
				}
			}
		}

		channelCopy := channel
		acks = append(acks, PubSubAck{
			Kind:  "unsubscribe",
			Name:  &channelCopy,
			Count: ctx.pubsubSubscriptionCountLocked(),
		})
	}

	return acks
}

func (h *PubSubHub) PUnsubscribe(ctx *ConnectionContext, patterns [][]byte) []PubSubAck {
	if h == nil || ctx == nil {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	ctx.pubsubMu.Lock()
	defer ctx.pubsubMu.Unlock()

	targets := make([]string, 0, len(patterns))
	if len(patterns) == 0 {
		for pattern := range ctx.pubsubPatterns {
			targets = append(targets, pattern)
		}
		sort.Strings(targets)
	} else {
		for _, raw := range patterns {
			targets = append(targets, string(raw))
		}
	}

	if len(targets) == 0 {
		return []PubSubAck{{Kind: "punsubscribe", Name: nil, Count: ctx.pubsubSubscriptionCountLocked()}}
	}

	acks := make([]PubSubAck, 0, len(targets))
	for _, pattern := range targets {
		if _, ok := ctx.pubsubPatterns[pattern]; ok {
			delete(ctx.pubsubPatterns, pattern)
			if subscribers := h.patterns[pattern]; subscribers != nil {
				delete(subscribers, ctx)
				if len(subscribers) == 0 {
					delete(h.patterns, pattern)
				}
			}
		}

		patternCopy := pattern
		acks = append(acks, PubSubAck{
			Kind:  "punsubscribe",
			Name:  &patternCopy,
			Count: ctx.pubsubSubscriptionCountLocked(),
		})
	}

	return acks
}

func (h *PubSubHub) CleanupConnection(ctx *ConnectionContext) {
	if h == nil || ctx == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	ctx.pubsubMu.Lock()
	defer ctx.pubsubMu.Unlock()

	for channel := range ctx.pubsubChannels {
		if subscribers := h.channels[channel]; subscribers != nil {
			delete(subscribers, ctx)
			if len(subscribers) == 0 {
				delete(h.channels, channel)
			}
		}
		delete(ctx.pubsubChannels, channel)
	}
	for pattern := range ctx.pubsubPatterns {
		if subscribers := h.patterns[pattern]; subscribers != nil {
			delete(subscribers, ctx)
			if len(subscribers) == 0 {
				delete(h.patterns, pattern)
			}
		}
		delete(ctx.pubsubPatterns, pattern)
	}
}

func (h *PubSubHub) Publish(channel string, message []byte) int64 {
	if h == nil {
		return 0
	}

	h.mu.RLock()
	subscribersMap := h.channels[channel]
	subscribers := make([]*ConnectionContext, 0, len(subscribersMap))
	for subscriber := range subscribersMap {
		subscribers = append(subscribers, subscriber)
	}
	type patternSubscriber struct {
		pattern string
		ctx     *ConnectionContext
	}
	patternSubscribers := make([]patternSubscriber, 0)
	for pattern, matchedSubscribers := range h.patterns {
		matched, err := path.Match(pattern, channel)
		if err != nil || !matched {
			continue
		}
		for subscriber := range matchedSubscribers {
			patternSubscribers = append(patternSubscribers, patternSubscriber{
				pattern: pattern,
				ctx:     subscriber,
			})
		}
	}
	h.mu.RUnlock()

	if len(subscribers) == 0 && len(patternSubscribers) == 0 {
		return 0
	}

	payload := encodePubSubMessage(channel, message)
	deliveredSet := make(map[*ConnectionContext]struct{}, len(subscribers)+len(patternSubscribers))
	for _, subscriber := range subscribers {
		if subscriber == nil || subscriber.pubsubWriter == nil {
			continue
		}
		if err := subscriber.pubsubWriter.WriteAsync(payload); err != nil {
			h.CleanupConnection(subscriber)
			continue
		}
		deliveredSet[subscriber] = struct{}{}
	}

	for _, patternSubscriber := range patternSubscribers {
		if patternSubscriber.ctx == nil || patternSubscriber.ctx.pubsubWriter == nil {
			continue
		}
		if err := patternSubscriber.ctx.pubsubWriter.WriteAsync(encodePubSubPatternMessage(patternSubscriber.pattern, channel, message)); err != nil {
			h.CleanupConnection(patternSubscriber.ctx)
			continue
		}
		deliveredSet[patternSubscriber.ctx] = struct{}{}
	}

	return int64(len(deliveredSet))
}

func (h *PubSubHub) Channels(pattern string) ([]string, error) {
	if h == nil {
		return nil, nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	channels := make([]string, 0, len(h.channels))
	for channel := range h.channels {
		if pattern == "" || pattern == "*" {
			channels = append(channels, channel)
			continue
		}
		matched, err := path.Match(pattern, channel)
		if err != nil {
			return nil, err
		}
		if matched {
			channels = append(channels, channel)
		}
	}
	sort.Strings(channels)
	return channels, nil
}

func (h *PubSubHub) NumSub(channels [][]byte) []int64 {
	if h == nil {
		return make([]int64, len(channels))
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	counts := make([]int64, len(channels))
	for i, raw := range channels {
		counts[i] = int64(len(h.channels[string(raw)]))
	}
	return counts
}

func appendPubSubAck(dst []byte, ack PubSubAck) []byte {
	items := []*protocol.Response{
		protocol.MakeBulkString(ack.Kind),
		nil,
		protocol.MakeInteger(ack.Count),
	}
	if ack.Name == nil {
		items[1] = protocol.MakeNull()
	} else {
		items[1] = protocol.MakeBulkString(*ack.Name)
	}

	resp := protocol.MakeArrayResponses(items)
	dst = protocol.AppendResponse(dst, resp)
	for _, item := range items {
		protocol.ReleaseResponse(item)
	}
	protocol.ReleaseResponse(resp)
	return dst
}

func encodePubSubMessage(channel string, message []byte) []byte {
	items := []*protocol.Response{
		protocol.MakeBulkString("message"),
		protocol.MakeBulkString(channel),
		{Type: protocol.BulkString, Value: message},
	}

	resp := protocol.MakeArrayResponses(items)
	payload := protocol.EncodeResponse(resp)
	protocol.ReleaseResponse(items[0])
	protocol.ReleaseResponse(items[1])
	protocol.ReleaseResponse(resp)
	return payload
}

func encodePubSubPatternMessage(pattern string, channel string, message []byte) []byte {
	items := []*protocol.Response{
		protocol.MakeBulkString("pmessage"),
		protocol.MakeBulkString(pattern),
		protocol.MakeBulkString(channel),
		{Type: protocol.BulkString, Value: message},
	}

	resp := protocol.MakeArrayResponses(items)
	payload := protocol.EncodeResponse(resp)
	protocol.ReleaseResponse(items[0])
	protocol.ReleaseResponse(items[1])
	protocol.ReleaseResponse(items[2])
	protocol.ReleaseResponse(resp)
	return payload
}
