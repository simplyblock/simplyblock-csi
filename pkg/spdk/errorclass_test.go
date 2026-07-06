package spdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/spdk/spdk-csi/pkg/util"
)

func TestClassifyControlPlaneError_HTTPStatuses(t *testing.T) {
	tests := []struct {
		status      int
		wantCode    codes.Code
		retryable   bool
		rpcSpecific bool
	}{
		// Retryable server errors + request timeout.
		{500, codes.Unavailable, true, false},
		{502, codes.Unavailable, true, false},
		{503, codes.Unavailable, true, false},
		{504, codes.Unavailable, true, false},
		{408, codes.Unavailable, true, false},

		// Backpressure / capacity.
		{429, codes.ResourceExhausted, true, false},
		{507, codes.ResourceExhausted, true, false},

		// Operation-specific — the RPC must handle these, not generic classification.
		{404, codes.Internal, false, true},
		{409, codes.Internal, false, true},

		// Permanent client errors — never retryable.
		{400, codes.InvalidArgument, false, false},
		{401, codes.Unauthenticated, false, false},
		{403, codes.PermissionDenied, false, false},
		{405, codes.FailedPrecondition, false, false}, // method not allowed
		{406, codes.FailedPrecondition, false, false}, // not acceptable
		{410, codes.FailedPrecondition, false, false}, // gone
		{411, codes.FailedPrecondition, false, false}, // length required
		{412, codes.FailedPrecondition, false, false}, // precondition failed
		{413, codes.FailedPrecondition, false, false}, // payload too large
		{414, codes.FailedPrecondition, false, false}, // URI too long
		{415, codes.FailedPrecondition, false, false}, // unsupported media type
		{422, codes.InvalidArgument, false, false},    // unprocessable entity

		// Permanent server errors — never retryable.
		{501, codes.Internal, false, false}, // not implemented
		{505, codes.Internal, false, false}, // HTTP version not supported
		{508, codes.Internal, false, false}, // loop detected
		{511, codes.Internal, false, false}, // network auth required
		{599, codes.Internal, false, false}, // unlisted 5xx → permanent
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("HTTP_%d", tc.status), func(t *testing.T) {
			err := &util.HTTPError{Method: "POST", StatusCode: tc.status, Message: "boom"}
			got := classifyControlPlaneError(err)
			if got.Code != tc.wantCode {
				t.Errorf("HTTP %d: code = %s, want %s", tc.status, got.Code, tc.wantCode)
			}
			if got.Retryable != tc.retryable {
				t.Errorf("HTTP %d: retryable = %v, want %v", tc.status, got.Retryable, tc.retryable)
			}
			if got.RPCSpecific != tc.rpcSpecific {
				t.Errorf("HTTP %d: rpcSpecific = %v, want %v", tc.status, got.RPCSpecific, tc.rpcSpecific)
			}
		})
	}
}

func TestClassifyControlPlaneError_NoClientErrorIsRetryable(t *testing.T) {
	// Guard rail: no 4xx except 408/429 may be classified as retryable. (404/409
	// are RPCSpecific and non-retryable at the generic layer.)
	retryable4xx := map[int]bool{408: true, 429: true}
	for status := 400; status < 500; status++ {
		got := classifyControlPlaneError(&util.HTTPError{StatusCode: status})
		if got.Retryable && !retryable4xx[status] {
			t.Errorf("HTTP %d must not be retryable (code %s)", status, got.Code)
		}
	}
}

func TestClassifyControlPlaneError_TransportErrors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCode  codes.Code
		retryable bool
	}{
		{
			// The exact failure mode from the incident logs.
			name:      "context deadline exceeded (wrapped, as jsonrpc do() returns it)",
			err:       fmt.Errorf("POST: %w", context.DeadlineExceeded),
			wantCode:  codes.DeadlineExceeded,
			retryable: true,
		},
		{
			name:      "context canceled",
			err:       fmt.Errorf("POST: %w", context.Canceled),
			wantCode:  codes.Canceled,
			retryable: true,
		},
		{
			name:      "connection refused (real net.Error)",
			err:       fmt.Errorf("POST: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}),
			wantCode:  codes.Unavailable,
			retryable: true,
		},
		{
			// An unknown, non-transport error (e.g. secret parse / unmarshal bug)
			// must NOT be mistaken for a retryable transport failure.
			name:      "unknown non-transport error",
			err:       fmt.Errorf("failed to parse secret file: %w", errors.New("unexpected end of JSON input")),
			wantCode:  codes.Internal,
			retryable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyControlPlaneError(tc.err)
			if got.Code != tc.wantCode {
				t.Errorf("code = %s, want %s", got.Code, tc.wantCode)
			}
			if got.Retryable != tc.retryable {
				t.Errorf("retryable = %v, want %v", got.Retryable, tc.retryable)
			}
		})
	}
}

func TestClassifyControlPlaneError_Nil(t *testing.T) {
	got := classifyControlPlaneError(nil)
	if got.Code != codes.OK || got.Retryable {
		t.Errorf("nil error: got %+v, want {OK false}", got)
	}
}