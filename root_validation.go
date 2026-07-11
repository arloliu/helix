package helix

import (
	"errors"
	"math"

	"github.com/arloliu/helix/types"
)

const cqlClientComponent = "helix.CQLClient"

func newRootOptionError(option, reason string) error {
	return &types.OptionError{Component: cqlClientComponent, Option: option, Reason: reason}
}

func joinRootValidationErrors(errs ...error) error {
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

func validateNewCQLClientConfig(config *ClientConfig) error {
	return joinRootValidationErrors(
		validateRootClusterNames(config),
		validateRootAutoRefresh(config),
		validateRootMirrorMode(config),
		validateRootDefaultMaxRows(config),
		validateRootRecoveryProbe(config),
	)
}

func validateRootClusterNames(config *ClientConfig) error {
	if err := config.ClusterNames.Validate(); err != nil {
		return newRootOptionError("WithClusterNames", err.Error())
	}

	return nil
}

func validateRootAutoRefresh(config *ClientConfig) error {
	if !config.AutoRefresh.Enabled {
		return nil
	}

	errList := make([]error, 0, 5)
	if config.AutoRefresh.FailureThreshold <= 0 {
		errList = append(errList, newRootOptionError("WithAutoRefreshFailureThreshold", "must be > 0"))
	}
	if config.AutoRefresh.SustainedFailureWindow <= 0 {
		errList = append(errList, newRootOptionError("WithAutoRefreshSustainedFailureWindow", "must be > 0"))
	}
	if config.AutoRefresh.MinRetryInterval <= 0 {
		errList = append(errList, newRootOptionError("WithAutoRefreshMinRetryInterval", "must be > 0"))
	}
	if config.AutoRefresh.CheckInterval <= 0 {
		errList = append(errList, newRootOptionError("WithAutoRefreshCheckInterval", "must be > 0"))
	}
	if config.AutoRefresh.RefreshTimeout <= 0 {
		errList = append(errList, newRootOptionError("WithAutoRefreshRefreshTimeout", "must be > 0"))
	}

	return joinRootValidationErrors(errList...)
}

func validateRootDefaultMaxRows(config *ClientConfig) error {
	if config.DefaultMaxRows < 0 {
		return newRootOptionError("WithDefaultMaxRows", "must be >= 0")
	}
	if config.DefaultMaxRows >= math.MaxInt32 {
		return newRootOptionError("WithDefaultMaxRows", "must be < math.MaxInt32")
	}
	return nil
}

// validateRootRecoveryProbe rejects a negative Interval or Timeout on
// [RecoveryProbe]. [WithRecoveryProbe] only substitutes defaults for the
// zero value; negative values reach here untouched so they surface as a
// types.OptionError instead of being silently clamped to the default.
func validateRootRecoveryProbe(config *ClientConfig) error {
	if config.RecoveryProbe == nil {
		return nil
	}

	errList := make([]error, 0, 2)
	if config.RecoveryProbe.Interval < 0 {
		errList = append(errList, newRootOptionError("WithRecoveryProbe", "Interval must be >= 0"))
	}
	if config.RecoveryProbe.Timeout < 0 {
		errList = append(errList, newRootOptionError("WithRecoveryProbe", "Timeout must be >= 0"))
	}

	return joinRootValidationErrors(errList...)
}

func validateRootMirrorMode(config *ClientConfig) error {
	errList := make([]error, 0, 3)

	if config.mirrorTargetSet && config.mirrorPublisherSet {
		errList = append(errList,
			errors.Join(
				types.ErrMirrorModeConflict,
				newRootOptionError("WithMirrorPublisher", "cannot be used with WithMirror"),
			),
		)
	}
	if config.mirrorTargetSet && config.MirrorTarget == nil {
		errList = append(errList,
			errors.Join(
				types.ErrNilMirrorTarget,
				newRootOptionError("WithMirror", "target cannot be nil"),
			),
		)
	}
	if config.mirrorPublisherSet && config.MirrorPublisher == nil {
		errList = append(errList,
			errors.Join(
				types.ErrNilMirrorPublisher,
				newRootOptionError("WithMirrorPublisher", "publisher cannot be nil"),
			),
		)
	}

	return joinRootValidationErrors(errList...)
}
