/*
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package adapter

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/spanner/adapter/apiv1/adapterpb"

	"github.com/googleapis/gax-go/v2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultRetryBackoff is used for retryers as a fallback value when the server
// did not return any retry information.
var DefaultRetryBackoff = gax.Backoff{
	Initial:    20 * time.Millisecond,
	Max:        32 * time.Second,
	Multiplier: 1.3,
}

// spannerRetryer extends the generic gax Retryer, but also checks for any
// retry info returned by Cloud Spanner and uses that if present.
type adapterRetryer struct {
	gax.Retryer
}

// onCodes returns a adapterRetryer that will retry on the specified error
// codes. For Internal errors, only errors that have one of a list of known
// descriptions should be retried.
func onCodes(bo gax.Backoff, cc ...codes.Code) gax.Retryer {
	return &adapterRetryer{
		Retryer: gax.OnCodes(cc, bo),
	}
}

// ExtractRetryDelay extracts retry backoff from a grpc error if present.
func ExtractRetryDelay(err error) (time.Duration, bool) {
	s := status.Convert(err)
	if s == nil {
		return 0, false
	}
	for _, detail := range s.Details() {
		if retryInfo, ok := detail.(*errdetails.RetryInfo); ok {
			if !retryInfo.GetRetryDelay().IsValid() {
				return 0, false
			}
			return retryInfo.GetRetryDelay().AsDuration(), true
		}
	}
	return 0, false
}

// Retry returns the retry delay returned by Cloud Spanner if that is present.
// Otherwise it returns the retry delay calculated by the generic gax Retryer.
func (r *adapterRetryer) Retry(err error) (time.Duration, bool) {
	if status.Code(err) == codes.Internal &&
		!strings.Contains(err.Error(), "stream terminated by RST_STREAM") &&
		!strings.Contains(err.Error(), "HTTP/2 error code: INTERNAL_ERROR") &&
		!strings.Contains(err.Error(), "Connection closed with unknown cause") &&
		!strings.Contains(
			err.Error(),
			"Received unexpected EOS on DATA frame from server",
		) {
		return 0, false
	}

	delay, shouldRetry := r.Retryer.Retry(err)
	if !shouldRetry {
		return 0, false
	}
	if serverDelay, hasServerDelay := ExtractRetryDelay(err); hasServerDelay {
		delay = serverDelay
	}
	return delay, true
}

// RunFuncWithRetry executes the provided function with a retry mechanism based
// on the given policy.
func RunCreateAdapterSessionWithRetry(
	ctx context.Context,
	f func(context.Context) error,
) error {
	retryer := onCodes(
		DefaultRetryBackoff,
		codes.ResourceExhausted,
		codes.Internal,
		codes.Unavailable,
	)
	funcWithRetry := func(ctx context.Context) error {
		for {
			err := f(ctx)
			if err == nil {
				return nil
			}
			_, ok := status.FromError(err)
			// Only retry on valid grpc status errors
			if !ok {
				return err
			}

			delay, shouldRetry := retryer.Retry(err)
			if !shouldRetry {
				return err
			}
			if err := gax.Sleep(ctx, delay); err != nil {
				return err
			}
		}
	}
	return funcWithRetry(ctx)
}

// RunAdaptMessageWithRetry executes the provided function with a retry
// mechanism based
// on the given policy.
func RunAdaptMessageWithRetry(
	ctx context.Context,
	disableRetry bool,
	f func(ctx context.Context) (adapterpb.Adapter_AdaptMessageClient, error),
) (adapterpb.Adapter_AdaptMessageClient, error) {
	retryer := onCodes(
		DefaultRetryBackoff,
		codes.ResourceExhausted,
		codes.Internal,
		codes.Unavailable,
	)
	funcWithRetry := func(ctx context.Context) (adapterpb.Adapter_AdaptMessageClient, error) {
		for {
			resp, err := f(ctx)
			if err == nil {
				return resp, nil
			}
			_, ok := status.FromError(err)
			// Only retry on valid grpc status errors
			if !ok || disableRetry {
				return nil, err
			}
			delay, shouldRetry := retryer.Retry(err)
			if !shouldRetry {
				return nil, err
			}
			if err := gax.Sleep(ctx, delay); err != nil {
				return nil, err
			}
		}
	}
	return funcWithRetry(ctx)
}
