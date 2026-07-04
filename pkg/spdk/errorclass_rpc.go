package spdk

import (
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/spdk/spdk-csi/pkg/util"
)

// errorClassifier is a per-RPC control-plane response policy. Any HTTP status in
// its overrides map is dispositioned RPC-specifically; every other status — and
// every non-HTTP transport error — falls through to classifyControlPlaneError.
//
// In principle any status may be overridden; in practice only the
// operation-specific ones (404, 409) are, because those are the statuses whose
// CSI meaning depends on the operation. Each RPC has a preconfigured instance
// below, so an operation's full response→disposition table lives in one
// declarative place instead of being scattered through the RPC body.
type errorClassifier struct {
	overrides map[int]controlPlaneErrorClass
}

// Classify maps a control-plane error to this RPC's disposition.
func (c errorClassifier) Classify(err error) controlPlaneErrorClass {
	var httpErr *util.HTTPError
	if errors.As(err, &httpErr) {
		if d, ok := c.overrides[httpErr.StatusCode]; ok {
			return d
		}
	}
	return classifyControlPlaneError(err)
}

// classifiedError is a control-plane error already classified for a specific RPC
// (produced by the classify*Error functions). It IS an error and carries a gRPC
// status, so a handler can return it directly and the gRPC layer unwraps it to
// the right code via status.FromError:
//
//	if err := sbclient.DeleteSnapshot(ctx, id); err != nil {
//	    ce := classifyDeleteSnapshotError(err)
//	    if ce.IsSuccess() { return &csi.DeleteSnapshotResponse{}, nil } // 404 → gone
//	    return nil, ce                                                  // 500 → Unavailable, …
//	}
//
// Handlers MUST branch on IsSuccess()/IsIdempotent() before returning it: those
// dispositions are not terminal errors (return the success response, or perform
// the idempotency lookup, respectively).
type classifiedError struct {
	class controlPlaneErrorClass
	err   error
}

var _ interface {
	error
	GRPCStatus() *status.Status
} = classifiedError{}

// IsIdempotent reports that the handler must resolve a conflict by looking up the
// existing object (e.g. a 409 on create) before returning.
func (c classifiedError) IsIdempotent() bool { return c.class.Idempotent }

// IsSuccess reports that the error is a no-op for this RPC and it should return
// success (e.g. a 404 on delete).
func (c classifiedError) IsSuccess() bool { return c.class.Success }

// Retryable reports whether retrying the operation can help.
func (c classifiedError) Retryable() bool { return c.class.Retryable }

// Error implements error.
func (c classifiedError) Error() string {
	if c.err != nil {
		return c.err.Error()
	}
	return c.class.Code.String()
}

// GRPCStatus lets the gRPC layer (status.FromError) map this error to the correct
// status code with no interceptor. An unresolved idempotent disposition reaching
// here is a caller bug and surfaces as Internal rather than a silent OK.
func (c classifiedError) GRPCStatus() *status.Status {
	if c.class.Idempotent {
		return status.Newf(codes.Internal, "idempotent conflict not resolved by caller: %v", c.err)
	}
	return status.New(c.class.Code, c.Error())
}

// Per-RPC convenience: classify a control-plane error for one operation.
func classifyCreateVolumeError(err error) classifiedError {
	return classifiedError{CreateVolumeErrorClassifier.Classify(err), err}
}
func classifyDeleteVolumeError(err error) classifiedError {
	return classifiedError{DeleteVolumeErrorClassifier.Classify(err), err}
}
func classifyCreateSnapshotError(err error) classifiedError {
	return classifiedError{CreateSnapshotErrorClassifier.Classify(err), err}
}
func classifyDeleteSnapshotError(err error) classifiedError {
	return classifiedError{DeleteSnapshotErrorClassifier.Classify(err), err}
}
func classifyControllerExpandVolumeError(err error) classifiedError {
	return classifiedError{ControllerExpandVolumeErrorClassifier.Classify(err), err}
}
func classifyControllerGetVolumeError(err error) classifiedError {
	return classifiedError{ControllerGetVolumeErrorClassifier.Classify(err), err}
}
func classifyValidateVolumeCapabilitiesError(err error) classifiedError {
	return classifiedError{ValidateVolumeCapabilitiesErrorClassifier.Classify(err), err}
}
func classifyListSnapshotsError(err error) classifiedError {
	return classifiedError{ListSnapshotsErrorClassifier.Classify(err), err}
}

// Dispositions reused across RPCs.
var (
	// alreadyGone: a 404 means the object no longer exists → the operation is a
	// no-op and succeeds (idempotent delete).
	alreadyGone = controlPlaneErrorClass{Code: codes.OK, Success: true}
	// sourceNotFound: a 404 means a referenced object (source volume/snapshot,
	// or the target being expanded/inspected) does not exist → NotFound.
	sourceNotFound = controlPlaneErrorClass{Code: codes.NotFound}
	// resolveConflict: a 409 must be resolved by looking up the existing object
	// (same source/params → return it as success; otherwise AlreadyExists).
	resolveConflict = controlPlaneErrorClass{Idempotent: true}
)

// Preconfigured per-RPC classifiers. Every RPC that talks to the control plane
// has one; generic statuses (5xx, timeout, 429, 4xx…) come from the shared
// classifier, and only the operation-specific statuses are overridden here.
var (
	CreateVolumeErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: sourceNotFound,
		http.StatusConflict: resolveConflict,
	}}
	DeleteVolumeErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: alreadyGone,
	}}
	CreateSnapshotErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: sourceNotFound,
		http.StatusConflict: resolveConflict,
	}}
	DeleteSnapshotErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: alreadyGone,
	}}

	ControllerExpandVolumeErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: sourceNotFound,
	}}
	ControllerGetVolumeErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: sourceNotFound,
	}}
	ValidateVolumeCapabilitiesErrorClassifier = errorClassifier{overrides: map[int]controlPlaneErrorClass{
		http.StatusNotFound: sourceNotFound,
	}}

	// ListSnapshotsErrorClassifier has no operation-specific statuses — every
	// status is handled generically.
	ListSnapshotsErrorClassifier = errorClassifier{}
)