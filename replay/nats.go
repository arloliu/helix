// Package replay provides replay queue implementations for failed write reconciliation.
package replay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/tinylib/msgp/msgp"

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

	// Replicas is the number of stream replicas (for fault tolerance).
	// Default: 1 (use 3 for production clusters)
	Replicas int

	// PublishTimeout is the timeout for publishing messages.
	// Default: 5 seconds
	PublishTimeout time.Duration
}

// DefaultNATSReplayerConfig returns the default configuration.
//
// Returns:
//   - NATSReplayerConfig: Default configuration with reasonable defaults
func DefaultNATSReplayerConfig() NATSReplayerConfig {
	return NATSReplayerConfig{
		StreamName:     "helix-replay",
		SubjectPrefix:  "helix.replay",
		MaxAge:         24 * time.Hour,
		MaxMsgs:        1_000_000,
		MaxBytes:       1 << 30, // 1GB
		Replicas:       1,
		PublishTimeout: 5 * time.Second,
	}
}

// NATSReplayer implements a durable replay queue using NATS JetStream.
//
// Unlike MemoryReplayer, messages persisted to JetStream survive process crashes.
// This is the recommended replayer for production use.
type NATSReplayer struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	config NATSReplayerConfig
	closed bool
	mu     sync.RWMutex
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
		Discard:     jetstream.DiscardOld,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf("helix: failed to create/update stream: %w", err)
	}

	return &NATSReplayer{
		js:     js,
		stream: stream,
		config: config,
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
	subject := fmt.Sprintf("%s.%s.%s", n.config.SubjectPrefix, priorityStr, payload.TargetCluster)

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
// This creates a pull consumer if it doesn't exist and fetches messages.
// The returned messages must be acknowledged after successful processing
// using the Ack method on each message.
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

	// Create a consumer for this cluster (both high and low priority)
	consumerName := fmt.Sprintf("helix-worker-%s", cluster)
	filterSubject := fmt.Sprintf("%s.*.%s", n.config.SubjectPrefix, cluster)

	consumer, err := n.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    5, // Retry up to 5 times
	})
	if err != nil {
		return nil, fmt.Errorf("helix: failed to create consumer: %w", err)
	}

	// Fetch messages
	msgs, err := consumer.Fetch(batchSize, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, jetstream.ErrNoMessages) {
			return nil, nil // No messages available
		}

		return nil, fmt.Errorf("helix: failed to fetch messages: %w", err)
	}

	result := make([]ReplayMessage, 0, batchSize)
	for msg := range msgs.Messages() {
		var natsMsg natsReplayMessage
		if _, err := natsMsg.UnmarshalMsg(msg.Data()); err != nil {
			// Skip malformed messages but nak them for retry
			_ = msg.Nak()

			continue
		}

		// Decode Args from msgp.Raw
		args, err := decodeArgs(natsMsg.Args)
		if err != nil {
			_ = msg.Nak()

			continue
		}

		// Decode batch statements if this is a batch
		var batchStmts []types.BatchStatement
		if natsMsg.IsBatch {
			batchStmts = make([]types.BatchStatement, len(natsMsg.BatchStatements))
			for i, stmt := range natsMsg.BatchStatements {
				stmtArgs, err := decodeArgs(stmt.Args)
				if err != nil {
					_ = msg.Nak()

					continue
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
			ackFunc: msg.Ack,
			nakFunc: msg.Nak,
		})
	}

	// Check for errors during iteration
	if err := msgs.Error(); err != nil {
		if !errors.Is(err, jetstream.ErrNoMessages) {
			return result, fmt.Errorf("helix: error during message fetch: %w", err)
		}
	}

	return result, nil
}

// DequeueByPriority retrieves a batch of replay messages for a specific priority.
//
// This creates a pull consumer for the specific priority/cluster combination
// and fetches messages. The returned messages must be acknowledged after
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

	// Create a consumer for this specific priority/cluster combination
	consumerName := fmt.Sprintf("helix-worker-%s-%s", priorityStr, cluster)
	filterSubject := fmt.Sprintf("%s.%s.%s", n.config.SubjectPrefix, priorityStr, cluster)

	consumer, err := n.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    5, // Retry up to 5 times
	})
	if err != nil {
		return nil, fmt.Errorf("helix: failed to create consumer: %w", err)
	}

	// Fetch messages
	msgs, err := consumer.Fetch(batchSize, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, jetstream.ErrNoMessages) {
			return nil, nil // No messages available
		}

		return nil, fmt.Errorf("helix: failed to fetch messages: %w", err)
	}

	result := make([]ReplayMessage, 0, batchSize)
	for msg := range msgs.Messages() {
		var natsMsg natsReplayMessage
		if _, err := natsMsg.UnmarshalMsg(msg.Data()); err != nil {
			// Skip malformed messages but nak them for retry
			_ = msg.Nak()

			continue
		}

		// Decode Args from msgp.Raw
		args, err := decodeArgs(natsMsg.Args)
		if err != nil {
			_ = msg.Nak()

			continue
		}

		// Decode batch statements if this is a batch
		var batchStmts []types.BatchStatement
		if natsMsg.IsBatch {
			batchStmts = make([]types.BatchStatement, len(natsMsg.BatchStatements))
			for i, stmt := range natsMsg.BatchStatements {
				stmtArgs, err := decodeArgs(stmt.Args)
				if err != nil {
					_ = msg.Nak()

					continue
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
			ackFunc: msg.Ack,
			nakFunc: msg.Nak,
		})
	}

	// Check for errors during iteration
	if err := msgs.Error(); err != nil {
		if !errors.Is(err, jetstream.ErrNoMessages) {
			return result, fmt.Errorf("helix: error during message fetch: %w", err)
		}
	}

	return result, nil
}

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
	// Note: CQL/SQL queries typically have < 100 parameters, so overflow is not a concern
	// in practice, but we add a check for safety.
	if len(args) > int(^uint32(0)) {
		return nil, errors.New("helix: too many arguments to encode")
	}

	var buf []byte
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
// It provides special handling for UUID types ([16]byte arrays).
func appendArg(buf []byte, arg any) ([]byte, error) {
	// Check for UUID-like types ([16]byte arrays)
	if uuid, ok := tryConvertToUUID(arg); ok {
		return msgp.AppendExtension(buf, &uuid)
	}

	// Fall back to standard msgp encoding
	return msgp.AppendIntf(buf, arg)
}

// tryConvertToUUID attempts to convert an argument to a UUID extension.
// It handles gocql.UUID and any other [16]byte array types.
func tryConvertToUUID(arg any) (UUID, bool) {
	switch v := arg.(type) {
	case [16]byte:
		// Direct [16]byte array (covers gocql.UUID which is type UUID [16]byte)
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
	for i := uint32(0); i < sz; i++ {
		var val any
		val, buf, err = msgp.ReadIntfBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("helix: failed to decode argument %d: %w", i, err)
		}

		// Unwrap UUID extension to []byte for compatibility with drivers (e.g. gocql)
		if u, ok := val.(*UUID); ok {
			// Return as []byte which is universally accepted by CQL drivers
			// for both UUID and Blob columns
			val = u.Bytes()
		}

		args[i] = val
	}

	return args, nil
}

// ReplayMessage wraps a replay payload with acknowledgment functions.
type ReplayMessage struct {
	Payload types.ReplayPayload
	ackFunc func() error
	nakFunc func() error
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

// Pending returns the number of pending messages in the stream.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - int: Number of pending messages
//   - error: Error if unable to get stream info
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

	// Cap at max int to prevent overflow
	msgs := info.State.Msgs
	if msgs > uint64(^uint(0)>>1) {
		msgs = uint64(^uint(0) >> 1)
	}

	//nolint:gosec // overflow is handled by the cap above
	return int(msgs), nil
}

// Close closes the replayer.
//
// Note: This does NOT close the NATS connection - that is the caller's responsibility.
func (n *NATSReplayer) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.closed = true
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
