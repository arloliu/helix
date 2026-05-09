package helix

import (
	"errors"

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
