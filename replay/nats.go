// Package replay provides replay queue implementations for failed write reconciliation.
package replay

import (
	"context"
	"encoding"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/inf.v0"

	"github.com/arloliu/helix/types"
)

// NATSReplayerConfig configures the NATS JetStream replayer.
type NATSReplayerConfig struct {
	// StreamName is the JetStream stream name for storing replay messages.
	// Default: "helix-replay"
	StreamName string

	// SubjectPrefix is the prefix for subjects. Messages are published to
	// "{SubjectPrefix}.{priority}.{cluster}" (e.g., "helix.replay.high.A").
	// Default: "helix.replay"
	SubjectPrefix string

	// MaxAge is the maximum age of messages in the stream.
	// Default: 24 hours
	MaxAge time.Duration

	// MaxMsgs is the maximum number of messages in the stream.
	// Default: 1,000,000
	MaxMsgs int64

	// MaxBytes is the maximum total size of the stream in bytes.
	// Default: 1GB
	MaxBytes int64

	// DiscardPolicy controls what JetStream does when MaxMsgs or MaxBytes
	// is reached. Default: DiscardOld, preserving write availability by
	// retaining the newest replay window and evicting older messages.
	DiscardPolicy jetstream.DiscardPolicy

	// Replicas is the number of stream replicas (for fault tolerance).
	// Default: 1 (use 3 for production clusters)
	Replicas int

	// PublishTimeout is the timeout for publishing messages.
	// Default: 5 seconds
	PublishTimeout time.Duration

	// MaxAckPending is the maximum number of outstanding unacknowledged messages
	// per consumer. Once reached, the server suspends message delivery until
	// acknowledgments are received. This provides backpressure to prevent
	// slow consumers from causing unbounded memory growth.
	// Default: 1000
	MaxAckPending int

	// MaxRequestBatch is the maximum batch size for a single pull request.
	// This limits memory consumption per fetch operation.
	// Default: 100
	MaxRequestBatch int

	// AckWait is how long the server waits for acknowledgment before
	// redelivering a message. Should be longer than your expected
	// processing time.
	// Default: 30 seconds
	AckWait time.Duration

	// MaxDeliver is the maximum number of delivery attempts for a message.
	// After this many failed attempts, the message is dropped by NATS.
	// The OnDrop callback will be called when this occurs.
	// Default: 5
	MaxDeliver int

	// OnCorruptMessage is called when a message cannot be decoded and is
	// permanently terminated from the queue. Use it for alerting or metrics.
	// Unlike MaxDeliver exhaustion, corrupt messages are terminated immediately
	// on the first decode attempt — retrying would never succeed.
	OnCorruptMessage func(err error)
}

// DefaultNATSReplayerConfig returns the default configuration.
//
// Returns:
//   - NATSReplayerConfig: Default configuration with reasonable defaults
func DefaultNATSReplayerConfig() NATSReplayerConfig {
	return NATSReplayerConfig{
		StreamName:      "helix-replay",
		SubjectPrefix:   "helix.replay",
		MaxAge:          24 * time.Hour,
		MaxMsgs:         1_000_000,
		MaxBytes:        1 << 30, // 1GB
		DiscardPolicy:   jetstream.DiscardOld,
		Replicas:        1,
		PublishTimeout:  5 * time.Second,
		MaxAckPending:   1000,
		MaxRequestBatch: 100,
		AckWait:         30 * time.Second,
		MaxDeliver:      5,
	}
}

// NATSReplayer implements a durable replay queue using NATS JetStream.
//
// Unlike MemoryReplayer, messages persisted to JetStream survive process crashes.
// This is the recommended replayer for production use.
type NATSReplayer struct {
	js        jetstream.JetStream
	stream    jetstream.Stream
	config    NATSReplayerConfig
	consumers map[string]jetstream.Consumer
	closed    bool
	mu        sync.RWMutex

	// redeliveryBackoff is set by a worker running RetryWhileRetained before
	// it starts: consumers created afterwards allow unlimited deliveries and
	// use this server-side redelivery schedule for unacknowledged messages.
	redeliveryBackoff []time.Duration
}

// NATSReplayerOption configures a NATSReplayer.
type NATSReplayerOption func(*NATSReplayerConfig)

// WithStreamName sets the JetStream stream name.
//
// Parameters:
//   - name: Stream name
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithStreamName(name string) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.StreamName = name
	}
}

// WithSubjectPrefix sets the subject prefix for replay messages.
//
// Parameters:
//   - prefix: Subject prefix
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithSubjectPrefix(prefix string) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.SubjectPrefix = prefix
	}
}

// WithMaxAge sets the maximum age of messages in the stream.
//
// Parameters:
//   - d: Maximum age duration
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithMaxAge(d time.Duration) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.MaxAge = d
	}
}

// WithMaxMsgs sets the maximum number of messages in the stream.
//
// Parameters:
//   - n: Maximum number of messages
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithMaxMsgs(n int64) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.MaxMsgs = n
	}
}

// WithMaxBytes sets the maximum total size of the stream.
//
// Parameters:
//   - n: Maximum bytes
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithMaxBytes(n int64) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.MaxBytes = n
	}
}

// WithDiscardPolicy sets the JetStream discard policy used when stream limits are reached.
//
// The default is [jetstream.DiscardOld], an availability-first policy that
// keeps accepting new replay messages and retains the newest repair window.
// Use [jetstream.DiscardNew] when preserving already-admitted replay work is
// more important than keeping enqueue calls successful during a prolonged outage.
//
// Parameters:
//   - policy: JetStream discard policy for MaxMsgs/MaxBytes pressure
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithDiscardPolicy(policy jetstream.DiscardPolicy) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.DiscardPolicy = policy
	}
}

// WithRejectNewOnLimit configures JetStream to reject new replay messages when full.
//
// This is a backpressure-first mode. Existing replay messages are preserved
// when MaxMsgs or MaxBytes is reached, and new Enqueue calls fail instead of
// evicting older replay work.
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithRejectNewOnLimit() NATSReplayerOption {
	return WithDiscardPolicy(jetstream.DiscardNew)
}

// WithReplicas sets the number of stream replicas.
//
// Parameters:
//   - n: Number of replicas (1 for dev, 3 for production)
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithReplicas(n int) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.Replicas = n
	}
}

// WithPublishTimeout sets the timeout for publishing messages.
//
// Parameters:
//   - d: Publish timeout duration
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithPublishTimeout(d time.Duration) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.PublishTimeout = d
	}
}

// WithMaxAckPending sets the maximum number of unacknowledged messages per consumer.
//
// This provides backpressure to prevent slow consumers from causing unbounded
// memory growth on the NATS server. Once the limit is reached, message delivery
// is suspended until acknowledgments are received.
//
// Parameters:
//   - n: Maximum pending acknowledgments (default: 1000)
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithMaxAckPending(n int) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.MaxAckPending = n
	}
}

// WithMaxRequestBatch sets the maximum batch size for pull requests.
//
// This limits memory consumption per fetch operation on both the client
// and server side.
//
// Parameters:
//   - n: Maximum batch size (default: 100)
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithMaxRequestBatch(n int) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.MaxRequestBatch = n
	}
}

// WithAckWait sets how long the server waits for acknowledgment before redelivery.
//
// This should be set longer than your expected message processing time to avoid
// unnecessary redeliveries. If processing takes longer than AckWait, the message
// will be redelivered even if still being processed.
//
// Parameters:
//   - d: Acknowledgment wait duration (default: 30 seconds)
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithAckWait(d time.Duration) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.AckWait = d
	}
}

// WithMaxDeliver sets the maximum number of delivery attempts for a message.
//
// After this many failed delivery attempts (Nak's), the message is dropped by NATS.
// The OnDrop callback in WorkerConfig will be called when this occurs.
//
// Parameters:
//   - n: Maximum delivery attempts (default: 5)
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithMaxDeliver(n int) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.MaxDeliver = n
	}
}

// WithOnCorruptMessage sets a callback invoked when a message cannot be decoded
// and is permanently terminated from the queue.
//
// Corrupt messages are terminated immediately on the first decode failure —
// retrying would never succeed since the payload bytes are irrecoverably malformed.
//
// Parameters:
//   - fn: Callback receiving the decode error
//
// Returns:
//   - NATSReplayerOption: Configuration option
func WithOnCorruptMessage(fn func(err error)) NATSReplayerOption {
	return func(c *NATSReplayerConfig) {
		c.OnCorruptMessage = fn
	}
}

// NewNATSReplayer creates a new NATS JetStream replayer.
//
// This function creates or updates a JetStream stream for storing replay messages.
// The caller is responsible for creating the JetStream context from their NATS connection.
//
// Parameters:
//   - js: A JetStream context (created via jetstream.New(conn))
//   - opts: Optional configuration options
//
// Returns:
//   - *NATSReplayer: A new NATS replayer
//   - error: Error if stream creation fails
//
// Example:
//
//	nc, _ := nats.Connect("nats://localhost:4222")
//	js, _ := jetstream.New(nc)
//	replayer, _ := replay.NewNATSReplayer(js)
func NewNATSReplayer(js jetstream.JetStream, opts ...NATSReplayerOption) (*NATSReplayer, error) {
	if js == nil {
		return nil, errors.New("helix: JetStream context is nil")
	}

	config := DefaultNATSReplayerConfig()
	for _, opt := range opts {
		opt(&config)
	}
	if err := validateNATSReplayerConfigForChecked(config); err != nil {
		return nil, err
	}

	// Create or update the stream
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streamConfig := jetstream.StreamConfig{
		Name:        config.StreamName,
		Description: "Helix dual-cluster replay queue",
		Subjects:    []string{config.SubjectPrefix + ".*.*"}, // {prefix}.{priority}.{cluster}
		Retention:   jetstream.WorkQueuePolicy,
		MaxAge:      config.MaxAge,
		MaxMsgs:     config.MaxMsgs,
		MaxBytes:    config.MaxBytes,
		Replicas:    config.Replicas,
		Storage:     jetstream.FileStorage,
		Discard:     config.DiscardPolicy,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf("helix: failed to create/update stream: %w", err)
	}

	return &NATSReplayer{
		js:        js,
		stream:    stream,
		config:    config,
		consumers: make(map[string]jetstream.Consumer),
	}, nil
}

// Enqueue adds a failed write to the NATS JetStream replay queue.
//
// The message is published with subject "{prefix}.{priority}.{cluster}".
// JetStream provides at-least-once delivery guarantees.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - payload: The write operation to replay
//
// Returns:
//   - error: nil on success, error on publish failure
func (n *NATSReplayer) Enqueue(ctx context.Context, payload types.ReplayPayload) error {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()

		return types.ErrSessionClosed
	}
	n.mu.RUnlock()

	if err := validatePayloadArgs(payload); err != nil {
		return err
	}

	// Serialize Args to msgp.Raw
	argsRaw, err := encodeArgs(payload.Args)
	if err != nil {
		return fmt.Errorf("helix: failed to encode args: %w", err)
	}

	// Build batch statements if this is a batch
	var batchStmts []batchStatement
	if payload.IsBatch {
		batchStmts = make([]batchStatement, len(payload.BatchStatements))
		for i, stmt := range payload.BatchStatements {
			stmtArgsRaw, err := encodeArgs(stmt.Args)
			if err != nil {
				return fmt.Errorf("helix: failed to encode batch args: %w", err)
			}
			batchStmts[i] = batchStatement{
				Query: stmt.Query,
				Args:  stmtArgsRaw,
			}
		}
	}

	// Build the message
	msg := natsReplayMessage{
		TargetCluster:   string(payload.TargetCluster),
		Query:           payload.Query,
		Args:            argsRaw,
		Timestamp:       payload.Timestamp,
		Priority:        int(payload.Priority),
		IsBatch:         payload.IsBatch,
		BatchType:       uint8(payload.BatchType),
		BatchStatements: batchStmts,
	}

	// Use msgp for efficient serialization
	data, err := msg.MarshalMsg(nil)
	if err != nil {
		return fmt.Errorf("helix: failed to marshal replay message: %w", err)
	}

	// Build subject: {prefix}.{priority}.{cluster}
	priorityStr := "low"
	if payload.Priority == types.PriorityHigh {
		priorityStr = "high"
	}
	subject := n.config.SubjectPrefix + "." + priorityStr + "." + string(payload.TargetCluster)

	// Publish with timeout
	pubCtx, cancel := context.WithTimeout(ctx, n.config.PublishTimeout)
	defer cancel()

	_, err = n.js.Publish(pubCtx, subject, data)
	if err != nil {
		return fmt.Errorf("helix: failed to publish replay message: %w", err)
	}

	return nil
}

// Dequeue retrieves a batch of replay messages for processing.
//
// The required pull consumer is created lazily on first use and reused for
// subsequent fetches. The returned messages must be acknowledged after
// successful processing using the Ack method on each message.
//
// Parameters:
//   - ctx: Context for cancellation
//   - cluster: Target cluster to get messages for (ClusterA or ClusterB)
//   - batchSize: Maximum number of messages to fetch
//
// Returns:
//   - []ReplayMessage: Batch of messages to process
//   - error: Error if fetch fails
func (n *NATSReplayer) Dequeue(ctx context.Context, cluster types.ClusterID, batchSize int) ([]ReplayMessage, error) {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()

		return nil, types.ErrSessionClosed
	}
	n.mu.RUnlock()

	consumerName := "helix-worker-" + string(cluster)
	filterSubject := n.config.SubjectPrefix + ".*." + string(cluster)

	consumer, err := n.getOrCreateConsumer(ctx, consumerName, filterSubject)
	if err != nil {
		return nil, err
	}

	return n.fetchReplayMessages(ctx, consumerName, consumer, batchSize)
}

// DequeueByPriority retrieves a batch of replay messages for a specific priority.
//
// The required pull consumer is created lazily on first use and reused for
// subsequent fetches. The returned messages must be acknowledged after
// successful processing using the Ack method on each message.
//
// Use this method for priority-aware processing where you want to control
// the order of high vs low priority message processing.
//
// Parameters:
//   - ctx: Context for cancellation
//   - cluster: Target cluster to get messages for (ClusterA or ClusterB)
//   - priority: Priority level to fetch (PriorityHigh or PriorityLow)
//   - batchSize: Maximum number of messages to fetch
//
// Returns:
//   - []ReplayMessage: Batch of messages to process
//   - error: Error if fetch fails
func (n *NATSReplayer) DequeueByPriority(ctx context.Context, cluster types.ClusterID, priority types.PriorityLevel, batchSize int) ([]ReplayMessage, error) {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()

		return nil, types.ErrSessionClosed
	}
	n.mu.RUnlock()

	// Build priority-specific subject
	priorityStr := "low"
	if priority == types.PriorityHigh {
		priorityStr = "high"
	}

	consumerName := workerConsumerName(priorityStr, cluster)
	filterSubject := n.config.SubjectPrefix + "." + priorityStr + "." + string(cluster)

	consumer, err := n.getOrCreateConsumer(ctx, consumerName, filterSubject)
	if err != nil {
		return nil, err
	}

	return n.fetchReplayMessages(ctx, consumerName, consumer, batchSize)
}

func (n *NATSReplayer) getOrCreateConsumer(
	ctx context.Context,
	consumerName string,
	filterSubject string,
) (jetstream.Consumer, error) {
	n.mu.RLock()
	consumer, ok := n.consumers[consumerName]
	n.mu.RUnlock()
	if ok {
		return consumer, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil, types.ErrSessionClosed
	}
	if consumer, ok = n.consumers[consumerName]; ok {
		return consumer, nil
	}

	consumerConfig := jetstream.ConsumerConfig{
		Name:            consumerName,
		Durable:         consumerName,
		FilterSubject:   filterSubject,
		AckPolicy:       jetstream.AckExplicitPolicy,
		DeliverPolicy:   jetstream.DeliverAllPolicy,
		MaxDeliver:      n.config.MaxDeliver,
		MaxAckPending:   n.config.MaxAckPending,
		MaxRequestBatch: n.config.MaxRequestBatch,
		AckWait:         n.config.AckWait,
	}
	if n.redeliveryBackoff != nil {
		// The worker owns the poison budget; the server must never drop a
		// message on its own. BackOff only governs redelivery of messages
		// that were never acknowledged (worker crash mid-batch); explicit
		// delayed NAKs carry their own delay.
		consumerConfig.MaxDeliver = -1
		consumerConfig.BackOff = n.redeliveryBackoff
	}

	consumer, err := n.stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("helix: failed to create consumer: %w", err)
	}

	n.consumers[consumerName] = consumer

	return consumer, nil
}

func (n *NATSReplayer) fetchReplayMessages(
	ctx context.Context,
	consumerName string,
	consumer jetstream.Consumer,
	batchSize int,
) ([]ReplayMessage, error) {
	// Fetch messages
	fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	msgs, err := consumer.Fetch(batchSize, jetstream.FetchContext(fetchCtx))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, jetstream.ErrNoMessages) {
			return nil, nil // No messages available
		}
		// Out-of-band consumer deletion (admin removed it, server reset,
		// stream recreated) leaves a stale entry in n.consumers that would
		// fail every subsequent fetch. Evict so the next call rebuilds.
		if isConsumerGoneErr(err) {
			n.invalidateConsumer(consumerName)
		}

		return nil, fmt.Errorf("helix: failed to fetch messages: %w", err)
	}

	maxDeliver := n.config.MaxDeliver
	result := make([]ReplayMessage, 0, batchSize)
msgLoop:
	for msg := range msgs.Messages() {
		// Extract delivery metadata
		meta, metaErr := msg.Metadata()
		var deliveryCount uint64 = 1
		var streamSeq uint64
		if metaErr == nil {
			deliveryCount = meta.NumDelivered
			streamSeq = meta.Sequence.Stream
		}

		var natsMsg natsReplayMessage
		if _, err := natsMsg.UnmarshalMsg(msg.Data()); err != nil {
			// Permanently corrupt — Term immediately; no retry will fix bad bytes.
			n.handleCorrupt(msg, err)

			continue
		}

		// A target no client can resolve is as unprocessable as bad bytes.
		if err := validateTargetCluster(types.ClusterID(natsMsg.TargetCluster)); err != nil {
			n.handleCorrupt(msg, err)

			continue
		}

		// Decode Args from msgp.Raw
		args, err := decodeArgs(natsMsg.Args)
		if err != nil {
			n.handleCorrupt(msg, err)

			continue
		}

		// Decode batch statements if this is a batch
		var batchStmts []types.BatchStatement
		if natsMsg.IsBatch {
			batchStmts = make([]types.BatchStatement, len(natsMsg.BatchStatements))
			for i, stmt := range natsMsg.BatchStatements {
				stmtArgs, err := decodeArgs(stmt.Args)
				if err != nil {
					n.handleCorrupt(msg, err)

					continue msgLoop
				}
				batchStmts[i] = types.BatchStatement{
					Query: stmt.Query,
					Args:  stmtArgs,
				}
			}
		}

		result = append(result, ReplayMessage{
			Payload: types.ReplayPayload{
				TargetCluster:   types.ClusterID(natsMsg.TargetCluster),
				Query:           natsMsg.Query,
				Args:            args,
				Timestamp:       natsMsg.Timestamp,
				Priority:        types.PriorityLevel(natsMsg.Priority),
				IsBatch:         natsMsg.IsBatch,
				BatchType:       types.BatchType(natsMsg.BatchType),
				BatchStatements: batchStmts,
			},
			ackFunc:          msg.Ack,
			nakFunc:          msg.Nak,
			nakWithDelayFunc: msg.NakWithDelay,
			inProgressFunc:   msg.InProgress,
			termFunc:         msg.Term,
			DeliveryCount:    deliveryCount,
			MaxDeliver:       maxDeliver,
			StreamSequence:   streamSeq,
		})
	}

	// Check for errors during iteration
	if err := msgs.Error(); err != nil {
		if !errors.Is(err, jetstream.ErrNoMessages) {
			if isConsumerGoneErr(err) {
				n.invalidateConsumer(consumerName)
			}

			return result, fmt.Errorf("helix: error during message fetch: %w", err)
		}
	}

	return result, nil
}

// isConsumerGoneErr reports whether err indicates the cached JetStream
// consumer reference is stale and must be rebuilt.
//
// jetstream.ErrConsumerNotFound is returned by management calls when the
// consumer was already missing at request time. jetstream.ErrConsumerDeleted
// is surfaced via MessageBatch.Error when a delete races with an in-flight
// pull. nats.ErrNoResponders surfaces in practice from consumer.Fetch when
// the durable consumer was deleted out of band — the subject the pull
// request targets has no subscriber. Treating ErrNoResponders as "evict
// and retry" is also safe under transient JetStream unreachability: the
// next call simply re-creates the consumer via CreateOrUpdateConsumer.
func isConsumerGoneErr(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, nats.ErrNoResponders)
}

// invalidateConsumer removes a stale consumer entry from the cache so the
// next Dequeue/DequeueByPriority for that name re-creates it via
// CreateOrUpdateConsumer. Safe to call when the entry is already absent.
func (n *NATSReplayer) invalidateConsumer(consumerName string) {
	n.mu.Lock()
	delete(n.consumers, consumerName)
	n.mu.Unlock()
}

// avgArgEncodedSize is a rough per-argument capacity hint for encodeArgs'
// output buffer, covering common CQL argument encodings (short strings,
// numbers, UUID extensions) without inspecting each argument's concrete
// type. A per-type estimate (e.g. msgp.GuessSize) was benchmarked and
// rejected: msgp.GuessSize doesn't recognize UUID-like values (this
// package's UUID extension methods use a pointer receiver, and driver
// UUID types aren't msgp builtins) and falls back to a 512-byte default
// per UUID, and even a UUID-aware estimate doubles the per-argument
// type-switch cost paid again moments later by appendArg. A flat hint
// avoids both problems while still cutting reallocations for the common
// case.
const avgArgEncodedSize = 24

// encodeArgs encodes []any arguments to msgp.Raw.
//
// msgp doesn't directly support []any, so we use msgp's AppendIntf which
// handles interface{} values by encoding them according to their underlying type.
//
// Special handling is provided for UUID types ([16]byte arrays) which are
// encoded as MessagePack extensions to preserve their type through serialization.
//
// Parameters:
//   - args: Slice of arguments to encode
//
// Returns:
//   - msgp.Raw: Encoded arguments as raw MessagePack bytes
//   - error: Encoding error if any argument type is not supported
func encodeArgs(args []any) (msgp.Raw, error) {
	if len(args) == 0 {
		return nil, nil
	}

	// Encode as a MessagePack array
	// Note: the queries typically have < 100 parameters, so overflow is not a concern
	// in practice, but we add a check for safety.
	if len(args) > int(^uint32(0)) {
		return nil, errors.New("helix: too many arguments to encode")
	}

	buf := make([]byte, 0, msgp.ArrayHeaderSize+len(args)*avgArgEncodedSize)
	//nolint:gosec // overflow checked above
	buf = msgp.AppendArrayHeader(buf, uint32(len(args)))

	for _, arg := range args {
		var err error
		buf, err = appendArg(buf, arg)
		if err != nil {
			return nil, fmt.Errorf("helix: failed to encode argument: %w", err)
		}
	}

	return buf, nil
}

// appendArg encodes a single argument to the buffer.
//
// UUID-shaped values, big integers, decimals, IP addresses, and CQL
// durations travel as MessagePack extensions so they decode back to the
// value the driver expects. A nil byte slice is encoded as nil and an empty
// one as an empty binary, so a replayed empty blob stays an empty blob.
func appendArg(buf []byte, arg any) ([]byte, error) {
	if uuid, ok := tryConvertToUUID(arg); ok {
		return msgp.AppendExtension(buf, &uuid)
	}
	if d, ok := durationFromValue(arg); ok {
		return msgp.AppendExtension(buf, &durationExt{value: d})
	}

	switch v := arg.(type) {
	case *big.Int:
		if v == nil {
			return msgp.AppendNil(buf), nil
		}
		ext := &varintExt{}
		ext.value.Set(v)

		return msgp.AppendExtension(buf, ext)
	case *inf.Dec:
		if v == nil {
			return msgp.AppendNil(buf), nil
		}
		ext := &decimalExt{}
		ext.value.Set(v)

		return msgp.AppendExtension(buf, ext)
	case net.IP:
		if v == nil {
			return msgp.AppendNil(buf), nil
		}

		return msgp.AppendExtension(buf, &inetExt{value: v})
	case []byte:
		if v == nil {
			return msgp.AppendNil(buf), nil
		}

		return msgp.AppendBytes(buf, v), nil
	}

	// Fall back to standard msgp encoding
	return msgp.AppendIntf(buf, arg)
}

// tryConvertToUUID attempts to convert an argument to a UUID extension.
// It handles gocql.UUID, google/uuid.UUID, and any other [16]byte array types.
func tryConvertToUUID(arg any) (UUID, bool) {
	switch v := arg.(type) {
	case [16]byte:
		// Direct [16]byte array
		return UUID(v), true
	case *[16]byte:
		// Pointer to [16]byte array
		if v != nil {
			return UUID(*v), true
		}
		return UUID{}, false
	case UUID:
		// Already our UUID type
		return v, true
	case *UUID:
		// Pointer to our UUID type
		if v != nil {
			return *v, true
		}
		return UUID{}, false
	default:
		// Optimization: Check for encoding.BinaryMarshaler (implemented by google/uuid)
		if bm, ok := arg.(encoding.BinaryMarshaler); ok {
			if data, err := bm.MarshalBinary(); err == nil && len(data) == 16 {
				var u UUID
				copy(u[:], data)
				return u, true
			}
		}

		// Fallback: Use reflection to handle named types that are underlying [16]byte
		// This covers gocql.UUID (which doesn't implement BinaryMarshaler) without importing it
		rv := reflect.ValueOf(arg)

		// Handle pointers by dereferencing
		if rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				return UUID{}, false
			}
			rv = rv.Elem()
		}

		// Check if it's an array of 16 bytes
		if rv.Kind() == reflect.Array && rv.Type().Len() == 16 && rv.Type().Elem().Kind() == reflect.Uint8 {
			// Create a new UUID and copy bytes
			var u UUID
			byteType := reflect.TypeFor[byte]()
			// We can't directly cast, so we copy
			// reflect.Copy requires both to be slices or arrays, but we can iterate
			// or use Unsafe, but iteration is safer and fast enough for 16 bytes
			for i := range 16 {
				converted, ok := rv.Index(i).Convert(byteType).Interface().(byte)
				if !ok {
					return UUID{}, false
				}
				u[i] = converted
			}

			return u, true
		}

		return UUID{}, false
	}
}

// decodeArgs decodes msgp.Raw back to []any arguments.
//
// Parameters:
//   - raw: Raw MessagePack bytes to decode
//
// Returns:
//   - []any: Decoded arguments
//   - error: Decoding error if the data is malformed
func decodeArgs(raw msgp.Raw) ([]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// Read array header
	sz, buf, err := msgp.ReadArrayHeaderBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("helix: failed to read array header: %w", err)
	}

	args := make([]any, sz)
	for i := range sz {
		var val any
		val, buf, err = msgp.ReadIntfBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("helix: failed to decode argument %d: %w", i, err)
		}

		args[i] = unwrapArg(val)
	}

	return args, nil
}

// unwrapArg turns a decoded MessagePack value into the value handed to the
// driver: extensions become their Go types and an empty binary stays an
// empty, non-nil byte slice.
func unwrapArg(val any) any {
	switch v := val.(type) {
	case *UUID:
		// []byte is universally accepted by CQL drivers for both UUID and
		// blob columns.
		return v.Bytes()
	case *varintExt:
		return &v.value
	case *decimalExt:
		return &v.value
	case *inetExt:
		return v.value
	case *durationExt:
		return v.value
	case []byte:
		if v == nil {
			return []byte{}
		}

		return v
	}

	return val
}

// ReplayMessage wraps a replay payload with acknowledgment functions.
type ReplayMessage struct {
	Payload          types.ReplayPayload
	ackFunc          func() error
	nakFunc          func() error
	nakWithDelayFunc func(time.Duration) error
	inProgressFunc   func() error
	termFunc         func() error

	// DeliveryCount is the number of times this message has been delivered.
	// Starts at 1 for the first delivery.
	DeliveryCount uint64

	// MaxDeliver is the maximum delivery attempts configured for this consumer.
	// When DeliveryCount equals MaxDeliver and the message is Nak'd, it will be dropped.
	// Under RetryWhileRetained the consumer allows unlimited deliveries and
	// the worker's MaxAttempts bounds dead-letter attempts instead.
	MaxDeliver int

	// StreamSequence is the message's sequence number in the stream.
	// It identifies the message across redeliveries.
	StreamSequence uint64
}

// Ack acknowledges successful processing of the message.
//
// Returns:
//   - error: Error if acknowledgment fails
func (m *ReplayMessage) Ack() error {
	if m.ackFunc != nil {
		return m.ackFunc()
	}

	return nil
}

// Nak negatively acknowledges the message for redelivery.
//
// Returns:
//   - error: Error if negative acknowledgment fails
func (m *ReplayMessage) Nak() error {
	if m.nakFunc != nil {
		return m.nakFunc()
	}

	return nil
}

// NakWithDelay negatively acknowledges the message and asks the server to
// redeliver it no sooner than delay from now.
//
// Parameters:
//   - delay: Minimum time before redelivery
//
// Returns:
//   - error: Error if negative acknowledgment fails
func (m *ReplayMessage) NakWithDelay(delay time.Duration) error {
	if m.nakWithDelayFunc != nil {
		return m.nakWithDelayFunc(delay)
	}

	return nil
}

// InProgress tells the server the message is still being processed,
// resetting its AckWait timer so a long batch is not redelivered mid-flight.
//
// Returns:
//   - error: Error if the notification fails
func (m *ReplayMessage) InProgress() error {
	if m.inProgressFunc != nil {
		return m.inProgressFunc()
	}

	return nil
}

// Term terminates the message, preventing any further redelivery.
//
// Use this when you want to permanently stop processing a message,
// regardless of remaining delivery attempts. This is called automatically
// when MaxDeliver is reached and the message execution fails.
//
// Returns:
//   - error: Error if termination fails
func (m *ReplayMessage) Term() error {
	if m.termFunc != nil {
		return m.termFunc()
	}

	return nil
}

// Pending returns the number of pending messages in the stream.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - int: Number of pending messages
//   - error: Error if unable to get stream info, or if the stream's message
//     count overflows the platform int width (only possible on 32-bit
//     builds with a very large stream)
func (n *NATSReplayer) Pending(ctx context.Context) (int, error) {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()

		return 0, types.ErrSessionClosed
	}
	n.mu.RUnlock()

	info, err := n.stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("helix: failed to get stream info: %w", err)
	}

	return msgsToInt(info.State.Msgs)
}

// msgsToInt converts a JetStream stream's uint64 message count to a
// platform int, returning an error instead of a fabricated sentinel value
// when the count overflows the local int width (only reachable on 32-bit
// builds with a very large stream, or when Msgs exceeds math.MaxInt64).
func msgsToInt(msgs uint64) (int, error) {
	if msgs > math.MaxInt {
		return 0, fmt.Errorf("helix: pending count %d overflows platform int", msgs)
	}

	return int(msgs), nil
}

// handleCorrupt terminates a message that cannot be decoded and invokes the
// OnCorruptMessage callback if configured.
//
// Corrupt messages are terminated immediately rather than Nak'd: since the
// payload bytes are irrecoverably malformed, no number of redeliveries would
// allow the message to be processed successfully. Calling Term() removes the
// message from the queue without waiting for MaxDeliver exhaustion.
func (n *NATSReplayer) handleCorrupt(msg jetstream.Msg, err error) {
	if termErr := msg.Term(); termErr != nil {
		err = errors.Join(err, fmt.Errorf("helix: failed to terminate corrupt replay message: %w", termErr))
	}
	if n.config.OnCorruptMessage != nil {
		n.config.OnCorruptMessage(err)
	}
}

// Close closes the replayer.
//
// Note: This does NOT close the NATS connection - that is the caller's responsibility.
func (n *NATSReplayer) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.closed = true
	// Drop cached consumer references; the durable consumers themselves
	// remain on the server and are owned by the caller's NATS connection.
	n.consumers = nil
	// We don't close the NATS connection - caller owns it

	return nil
}

// StreamName returns the JetStream stream name.
//
// Returns:
//   - string: The stream name
func (n *NATSReplayer) StreamName() string {
	return n.config.StreamName
}

// Config returns a copy of the effective replayer configuration.
//
// Returns:
//   - NATSReplayerConfig: The configuration after defaults and options
func (n *NATSReplayer) Config() NATSReplayerConfig {
	return n.config
}

// PendingByCluster returns the number of messages for one cluster that have
// not yet been replayed successfully: not yet delivered, delivered but not
// acknowledged, and waiting for redelivery, across both priority consumers.
//
// Consumers that this replayer has not created yet contribute 0.
//
// Parameters:
//   - ctx: Context for cancellation
//   - cluster: The target cluster to count
//
// Returns:
//   - int: Outstanding messages for that cluster
//   - error: Error if consumer info cannot be fetched
func (n *NATSReplayer) PendingByCluster(ctx context.Context, cluster types.ClusterID) (int, error) {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()

		return 0, types.ErrSessionClosed
	}
	consumers := make([]jetstream.Consumer, 0, 2)
	for _, priority := range []string{"high", "low"} {
		if consumer, ok := n.consumers[workerConsumerName(priority, cluster)]; ok {
			consumers = append(consumers, consumer)
		}
	}
	n.mu.RUnlock()

	total := uint64(0)
	for _, consumer := range consumers {
		info, err := consumer.Info(ctx)
		if err != nil {
			return 0, fmt.Errorf("helix: failed to get consumer info: %w", err)
		}
		total += info.NumPending + uint64(info.NumAckPending) //nolint:gosec // NumAckPending is a non-negative count
	}

	return msgsToInt(total)
}

// enableRetainedDelivery makes every consumer created from now on allow
// unlimited deliveries and use backoff as its redelivery schedule for
// unacknowledged messages.
// A worker running RetryWhileRetained calls it before Start.
func (n *NATSReplayer) enableRetainedDelivery(backoff []time.Duration) {
	n.mu.Lock()
	n.redeliveryBackoff = backoff
	n.mu.Unlock()
}

// workerConsumerName is the durable consumer a worker uses for one
// priority level of one cluster.
func workerConsumerName(priority string, cluster types.ClusterID) string {
	return "helix-worker-" + priority + "-" + string(cluster)
}
