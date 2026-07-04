package spdk

import (
	"context"
	"errors"
	"net"
	"net/http"

	"google.golang.org/grpc/codes"

	"github.com/spdk/spdk-csi/pkg/util"
)

// controlPlaneErrorClass is the CSI-level classification of a control-plane
// error: the gRPC status code the RPC should return, whether the operation is
// worth retrying, and whether the status is operation-specific.
type controlPlaneErrorClass struct {
	Code      codes.Code
	Retryable bool

	// RPCSpecific marks a control-plane status whose CSI meaning depends on the
	// calling RPC and therefore cannot be classified generically — namely 404
	// and 409. Example: a 404 means "already deleted, success" for DeleteVolume
	// but "source not found, NotFound" for CreateVolume-from-snapshot; a 409
	// means "idempotent, return the existing object" for a create with the same
	// source but "AlreadyExists" for a different one. The generic classifier
	// cannot resolve these — a per-RPC classifier (see errorclass_rpc.go) does.
	// If one reaches generic classification unresolved, it is a driver bug: Code
	// is codes.Internal so it surfaces rather than being silently mislabeled
	// (e.g. as a retryable Unavailable).
	RPCSpecific bool

	// Success means the control-plane error is a no-op for this RPC and it should
	// return success — e.g. a 404 on a delete: the object is already gone.
	Success bool

	// Idempotent means the RPC must resolve a conflict by looking up the existing
	// object before deciding — e.g. a 409 on a create: same source/params →
	// return the existing object as success, otherwise AlreadyExists. Pure
	// classification cannot decide; the RPC performs the lookup.
	Idempotent bool
}

// classifyControlPlaneError maps a simplyblock control-plane error to the gRPC
// status a CSI RPC should return, and whether retrying can help. It handles only
// the operation-INDEPENDENT ("generic") failures; operation-specific statuses
// (404, 409) are flagged RPCSpecific and must be handled by the RPC itself.
//
// Generic policy:
//   - Transport failures: timeout → DeadlineExceeded, cancel → Canceled,
//     connection refused/reset/TLS/DNS → Unavailable (all retryable).
//   - 500/502/503/504 and 408 → Unavailable (retryable).
//   - 429 and 507 → ResourceExhausted (retryable backpressure/capacity).
//   - 400/422 → InvalidArgument, 401 → Unauthenticated, 403 → PermissionDenied,
//     412 → FailedPrecondition; other 4xx → FailedPrecondition (all permanent).
//   - 501/505/508/511 and any other 5xx → Internal (permanent).
//   - An unrecognized non-HTTP error (e.g. a secret-parse or unmarshal bug) is
//     NOT a transient control-plane failure → Internal, not retryable. Mapping
//     such errors to the retryable Unavailable is what let the sidecars retry
//     permanent failures forever and collapse etcd.
func classifyControlPlaneError(err error) controlPlaneErrorClass {
	if err == nil {
		return controlPlaneErrorClass{Code: codes.OK}
	}

	var httpErr *util.HTTPError
	if errors.As(err, &httpErr) {
		return classifyHTTPStatus(httpErr.StatusCode)
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return controlPlaneErrorClass{Code: codes.DeadlineExceeded, Retryable: true}
	case errors.Is(err, context.Canceled):
		return controlPlaneErrorClass{Code: codes.Canceled, Retryable: true}
	}

	// Recognized transport failure (connection refused/reset, TLS, DNS) → transient.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return controlPlaneErrorClass{Code: codes.Unavailable, Retryable: true}
	}

	// Unknown, non-transport error — treat as an internal fault, not a retry.
	return controlPlaneErrorClass{Code: codes.Internal}
}

// classifyHTTPStatus applies the generic policy to a raw HTTP status code.
func classifyHTTPStatus(status int) controlPlaneErrorClass {
	switch status {
	// Operation-specific — must be handled by the RPC, never generically.
	case http.StatusNotFound, // 404
		http.StatusConflict: // 409
		return controlPlaneErrorClass{Code: codes.Internal, RPCSpecific: true}

	// Retryable server errors + request timeout.
	case http.StatusInternalServerError, // 500
		http.StatusBadGateway,         // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout,     // 504
		http.StatusRequestTimeout:     // 408
		return controlPlaneErrorClass{Code: codes.Unavailable, Retryable: true}

	// Backpressure / capacity — retry with backoff.
	case http.StatusTooManyRequests, // 429
		http.StatusInsufficientStorage: // 507
		return controlPlaneErrorClass{Code: codes.ResourceExhausted, Retryable: true}

	// Permanent server errors.
	case http.StatusNotImplemented, // 501
		http.StatusHTTPVersionNotSupported,       // 505
		http.StatusLoopDetected,                  // 508
		http.StatusNetworkAuthenticationRequired: // 511
		return controlPlaneErrorClass{Code: codes.Internal}

	// Permanent client errors with specific gRPC codes.
	case http.StatusBadRequest, http.StatusUnprocessableEntity: // 400, 422
		return controlPlaneErrorClass{Code: codes.InvalidArgument}
	case http.StatusUnauthorized: // 401
		return controlPlaneErrorClass{Code: codes.Unauthenticated}
	case http.StatusForbidden: // 403
		return controlPlaneErrorClass{Code: codes.PermissionDenied}
	case http.StatusPreconditionFailed: // 412
		return controlPlaneErrorClass{Code: codes.FailedPrecondition}
	}

	switch {
	case status >= 400 && status < 500:
		return controlPlaneErrorClass{Code: codes.FailedPrecondition} // unlisted 4xx → permanent
	default:
		return controlPlaneErrorClass{Code: codes.Internal} // unlisted 5xx (and anything else) → permanent
	}
}
