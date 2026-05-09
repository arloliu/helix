package replay

import (
	"errors"
	"fmt"
	"strings"

	"github.com/arloliu/helix/types"
)

const (
	memoryReplayerComponent = "replay.MemoryReplayer"
	memoryWorkerComponent   = "replay.MemoryWorker"
	natsWorkerComponent     = "replay.NATSWorker"
)

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
