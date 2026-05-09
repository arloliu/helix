package policy

import (
	"errors"
	"fmt"

	"github.com/arloliu/helix/types"
)

const (
	adaptiveDualWriteComponent  = "policy.AdaptiveDualWrite"
	circuitBreakerComponent     = "policy.CircuitBreaker"
	latencyCircuitComponent     = "policy.LatencyCircuitBreaker"
	maxInt32OptionBoundaryError = "must be between 1 and 2147483647"
)

func isKnownCluster(cluster types.ClusterID) bool {
	return cluster == types.ClusterA || cluster == types.ClusterB
}

func newOptionError(component, option, reason string) error {
	return &types.OptionError{Component: component, Option: option, Reason: reason}
}

func joinValidationErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func optionErrPositiveDuration(component, option string) error {
	return newOptionError(component, option, "must be > 0")
}

func optionErrNonNegativeDuration(component, option string) error {
	return newOptionError(component, option, "must be >= 0")
}

func optionErrInt32Range(component, option string) error {
	return newOptionError(component, option, maxInt32OptionBoundaryError)
}

func optionErrReasonFromErr(component, option string, err error) error {
	return newOptionError(component, option, fmt.Sprintf("%v", err))
}
