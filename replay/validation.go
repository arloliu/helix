package replay

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

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
