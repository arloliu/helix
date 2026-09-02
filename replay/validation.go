package replay

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/inf.v0"

	"github.com/arloliu/helix/types"
)

const (
	memoryReplayerComponent = "replay.MemoryReplayer"
	natsReplayerComponent   = "replay.NATSReplayer"
	memoryWorkerComponent   = "replay.MemoryWorker"
	natsWorkerComponent     = "replay.NATSWorker"
)

var natsTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func newOptionError(component, option, reason string) error {
	return &types.OptionError{Component: component, Option: option, Reason: reason}
}

func joinValidationErrors(errs ...error) error {
	nonNil := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}

	return errors.Join(nonNil...)
}

func optionErrPositiveInt(component, option string) error {
	return newOptionError(component, option, "must be > 0")
}

func optionErrNonNegativeInt(component, option string) error {
	return newOptionError(component, option, "must be >= 0")
}

func optionErrPositiveDuration(component, option string) error {
	return newOptionError(component, option, "must be > 0")
}

func optionErrClusterNames(component, option string, err error) error {
	return newOptionError(component, option, strings.TrimSpace(fmt.Sprintf("%v", err)))
}

func validateWorkerConfigForChecked(config WorkerConfig, component string) error {
	errList := make([]error, 0, 9)

	if config.BatchSize <= 0 {
		errList = append(errList, optionErrPositiveInt(component, "WithBatchSize"))
	}
	if config.PollInterval <= 0 {
		errList = append(errList, optionErrPositiveDuration(component, "WithPollInterval"))
	}
	if config.RetryDelay <= 0 {
		errList = append(errList, optionErrPositiveDuration(component, "WithRetryDelay"))
	}
	if config.MaxRetryDelay <= 0 {
		errList = append(errList, optionErrPositiveDuration(component, "WithMaxRetryDelay"))
	}
	if config.RetryDelay > 0 && config.MaxRetryDelay > 0 && config.MaxRetryDelay < config.RetryDelay {
		errList = append(errList,
			newOptionError(component, "WithMaxRetryDelay",
				"must be >= retry delay configured by WithRetryDelay"),
		)
	}
	if config.ExecuteTimeout <= 0 {
		errList = append(errList, optionErrPositiveDuration(component, "WithExecuteTimeout"))
	}
	if config.MaxAttempts <= 0 {
		errList = append(errList, optionErrPositiveInt(component, "WithMaxAttempts"))
	}
	if config.RetryWindow <= 0 {
		errList = append(errList, optionErrPositiveDuration(component, "WithRetryWindow"))
	}
	if !config.RetryPolicy.valid() {
		errList = append(errList, newOptionError(component, "WithRetryPolicy",
			"must be RetryBounded or RetryWhileRetained"))
	}
	if config.HighPriorityRatio < 0 {
		errList = append(errList, optionErrNonNegativeInt(component, "WithHighPriorityRatio"))
	}
	if err := config.ClusterNames.Validate(); err != nil {
		errList = append(errList, optionErrClusterNames(component, "WithWorkerClusterNames", err))
	}

	return joinValidationErrors(errList...)
}

func validateWorkerInputsForChecked(component string, hasReplayer bool, execute ExecuteFunc) error {
	errList := make([]error, 0, 2)

	if !hasReplayer {
		errList = append(errList, newOptionError(component, "replayer", "must be non-nil"))
	}
	if execute == nil {
		errList = append(errList, newOptionError(component, "execute", "must be non-nil"))
	}

	return joinValidationErrors(errList...)
}

func validateNATSReplayerConfigForChecked(config NATSReplayerConfig) error {
	errList := make([]error, 0, 7)

	if err := validateNATSStreamName(config.StreamName); err != nil {
		errList = append(errList,
			newOptionError(natsReplayerComponent, "WithStreamName", err.Error()),
		)
	}
	if err := validateNATSSubjectPrefix(config.SubjectPrefix); err != nil {
		errList = append(errList,
			newOptionError(natsReplayerComponent, "WithSubjectPrefix", err.Error()),
		)
	}
	if config.Replicas <= 0 {
		errList = append(errList, optionErrPositiveInt(natsReplayerComponent, "WithReplicas"))
	}
	if config.PublishTimeout <= 0 {
		errList = append(errList, optionErrPositiveDuration(natsReplayerComponent, "WithPublishTimeout"))
	}
	if config.MaxAckPending <= 0 {
		errList = append(errList, optionErrPositiveInt(natsReplayerComponent, "WithMaxAckPending"))
	}
	if config.MaxRequestBatch <= 0 {
		errList = append(errList, optionErrPositiveInt(natsReplayerComponent, "WithMaxRequestBatch"))
	}
	if config.AckWait <= 0 {
		errList = append(errList, optionErrPositiveDuration(natsReplayerComponent, "WithAckWait"))
	}
	if config.MaxDeliver <= 0 {
		errList = append(errList, optionErrPositiveInt(natsReplayerComponent, "WithMaxDeliver"))
	}
	if !isValidDiscardPolicy(config.DiscardPolicy) {
		errList = append(errList,
			newOptionError(natsReplayerComponent, "WithDiscardPolicy", "must be DiscardOld or DiscardNew"),
		)
	}

	return joinValidationErrors(errList...)
}

func isValidDiscardPolicy(policy jetstream.DiscardPolicy) bool {
	return policy == jetstream.DiscardOld || policy == jetstream.DiscardNew
}

func validateNATSStreamName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return errors.New("must be non-empty")
	}
	if trimmed != name {
		return errors.New("must not have leading or trailing whitespace")
	}
	if !natsTokenPattern.MatchString(trimmed) {
		return errors.New("must contain only letters, numbers, underscores, or hyphens")
	}

	return nil
}

func validateNATSSubjectPrefix(prefix string) error {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		return errors.New("must be non-empty")
	}
	if trimmed != prefix {
		return errors.New("must not have leading or trailing whitespace")
	}
	tokens := strings.SplitSeq(trimmed, ".")
	for token := range tokens {
		if token == "" {
			return errors.New("must not contain empty subject tokens")
		}
		if !natsTokenPattern.MatchString(token) {
			return errors.New("tokens must contain only letters, numbers, underscores, or hyphens")
		}
	}

	return nil
}

// validatePayloadArgs rejects a payload whose target cluster is unknown or
// whose arguments no backend can carry, so both replayers give the same
// answer at enqueue regardless of which one is configured.
func validatePayloadArgs(payload types.ReplayPayload) error {
	if err := validateTargetCluster(payload.TargetCluster); err != nil {
		return err
	}
	if err := validateArgs(payload.Args); err != nil {
		return err
	}
	for _, stmt := range payload.BatchStatements {
		if err := validateArgs(stmt.Args); err != nil {
			return err
		}
	}

	return nil
}

// validateArgs reports the first argument the replay wire format cannot
// carry as [types.ErrUnsupportedReplayArg].
func validateArgs(args []any) error {
	for i, arg := range args {
		if !supportedArg(reflect.ValueOf(arg)) {
			return fmt.Errorf("%w: argument %d is %T", types.ErrUnsupportedReplayArg, i, arg)
		}
	}

	return nil
}

// supportedArg reports whether v can be encoded by appendArg: the scalar
// kinds msgp handles, byte slices, time.Time, UUID-shaped values, the
// extension types, and slices, arrays, and string-keyed maps of supported
// values.
func supportedArg(v reflect.Value) bool {
	if !v.IsValid() {
		return true // nil
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return true
		}
		if v.Kind() == reflect.Pointer {
			switch v.Interface().(type) {
			case *big.Int, *inf.Dec:
				return true
			}
		}

		return supportedArg(v.Elem())
	}

	switch v.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return true // []byte, net.IP, [16]byte
		}
		for i := range v.Len() {
			if !supportedArg(v.Index(i)) {
				return false
			}
		}

		return true
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return false
		}
		for _, key := range v.MapKeys() {
			if !supportedArg(v.MapIndex(key)) {
				return false
			}
		}

		return true
	case reflect.Struct:
		if _, ok := v.Interface().(time.Time); ok {
			return true
		}
		if _, ok := durationFromValue(v.Interface()); ok {
			return true
		}
		_, ok := tryConvertToUUID(v.Interface())

		return ok
	case reflect.Invalid, reflect.Uintptr, reflect.Complex64, reflect.Complex128,
		reflect.Chan, reflect.Func, reflect.Interface, reflect.Pointer, reflect.UnsafePointer:
		return false
	}

	return false
}

// validateTargetCluster rejects a replay payload whose TargetCluster is
// neither ClusterA nor ClusterB.
// Such a payload cannot be routed by any client,
// so it is refused before it can occupy queue capacity.
func validateTargetCluster(cluster types.ClusterID) error {
	if cluster == types.ClusterA || cluster == types.ClusterB {
		return nil
	}

	return fmt.Errorf("%w: replay target %q", types.ErrInvalidCluster, cluster)
}
