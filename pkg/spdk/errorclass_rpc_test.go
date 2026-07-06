package spdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/spdk/spdk-csi/pkg/util"
)

// outcome is what a handler observes from a classifiedError: the gRPC code the
// framework would transmit (via status.FromError) plus the branch flags.
type outcome struct {
	code       codes.Code
	success    bool
	idempotent bool
	retryable  bool
}

// observe derives the handler-observable outcome from a disposition, mirroring
// classifiedError.GRPCStatus (an unresolved idempotent conflict surfaces as
// Internal) and the IsSuccess/IsIdempotent/Retryable accessors.
func observe(d controlPlaneErrorClass) outcome {
	code := d.Code
	if d.Idempotent {
		code = codes.Internal
	}
	return outcome{code: code, success: d.Success, idempotent: d.Idempotent, retryable: d.Retryable}
}

// TestRPCErrorClassifiers pins the complete response→disposition table for every
// RPC, grouped by RPC. For every status + transport case it asserts BOTH:
//
//   - the raw disposition from classifier.Classify (the internal contract), and
//   - the handler-observable outcome from classify*Error → classifiedError (the
//     gRPC code the framework transmits, plus the branch flags).
//
// Each RPC inherits the shared generic table and overrides only the statuses
// whose meaning is operation-specific. The generic table is pinned as literals
// (not derived from classifyControlPlaneError), so changing a generic mapping,
// an operation-specific override, or adding a new RPC that breaks the contract
// all fail the test.
func TestRPCErrorClassifiers(t *testing.T) {
	rpcSpecificBug := controlPlaneErrorClass{Code: codes.Internal, RPCSpecific: true}

	// Generic dispositions every RPC must inherit for non-404/409 statuses.
	generic := map[int]controlPlaneErrorClass{
		400: {Code: codes.InvalidArgument},
		401: {Code: codes.Unauthenticated},
		403: {Code: codes.PermissionDenied},
		405: {Code: codes.FailedPrecondition},
		406: {Code: codes.FailedPrecondition},
		408: {Code: codes.Unavailable, Retryable: true},
		410: {Code: codes.FailedPrecondition},
		411: {Code: codes.FailedPrecondition},
		412: {Code: codes.FailedPrecondition},
		413: {Code: codes.FailedPrecondition},
		414: {Code: codes.FailedPrecondition},
		415: {Code: codes.FailedPrecondition},
		422: {Code: codes.InvalidArgument},
		429: {Code: codes.ResourceExhausted, Retryable: true},
		500: {Code: codes.Unavailable, Retryable: true},
		501: {Code: codes.Internal},
		502: {Code: codes.Unavailable, Retryable: true},
		503: {Code: codes.Unavailable, Retryable: true},
		504: {Code: codes.Unavailable, Retryable: true},
		505: {Code: codes.Internal},
		507: {Code: codes.ResourceExhausted, Retryable: true},
		508: {Code: codes.Internal},
		511: {Code: codes.Internal},
		599: {Code: codes.Internal},
	}

	// Generic transport failures (no HTTP status) every RPC must inherit.
	genericTransport := []struct {
		label string
		err   error
		want  controlPlaneErrorClass
	}{
		{"timeout", fmt.Errorf("POST: %w", context.DeadlineExceeded), controlPlaneErrorClass{Code: codes.DeadlineExceeded, Retryable: true}},
		{"canceled", fmt.Errorf("POST: %w", context.Canceled), controlPlaneErrorClass{Code: codes.Canceled, Retryable: true}},
		{"conn_refused", fmt.Errorf("POST: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}), controlPlaneErrorClass{Code: codes.Unavailable, Retryable: true}},
		{"unknown", errors.New("secret parse failed"), controlPlaneErrorClass{Code: codes.Internal}},
	}

	// Per-RPC overrides — the only statuses allowed to differ from generic. Any
	// HTTP status may be overridden in principle; today only 404/409 are. A status
	// that is neither overridden nor generic (an unhandled 404/409) must surface
	// as an Internal (RPCSpecific) bug.
	rpcs := []struct {
		name       string
		classifier errorClassifier
		classify   func(error) classifiedError
		overrides  map[int]controlPlaneErrorClass
	}{
		{"CreateVolume", CreateVolumeErrorClassifier, classifyCreateVolumeError, map[int]controlPlaneErrorClass{
			404: {Code: codes.NotFound}, 409: {Idempotent: true}}},
		{"DeleteVolume", DeleteVolumeErrorClassifier, classifyDeleteVolumeError, map[int]controlPlaneErrorClass{
			404: {Code: codes.OK, Success: true}}},
		{"CreateSnapshot", CreateSnapshotErrorClassifier, classifyCreateSnapshotError, map[int]controlPlaneErrorClass{
			404: {Code: codes.NotFound}, 409: {Idempotent: true}}},
		{"DeleteSnapshot", DeleteSnapshotErrorClassifier, classifyDeleteSnapshotError, map[int]controlPlaneErrorClass{
			404: {Code: codes.OK, Success: true}}},
		{"ControllerExpandVolume", ControllerExpandVolumeErrorClassifier, classifyControllerExpandVolumeError, map[int]controlPlaneErrorClass{
			404: {Code: codes.NotFound}}},
		{"ControllerGetVolume", ControllerGetVolumeErrorClassifier, classifyControllerGetVolumeError, map[int]controlPlaneErrorClass{
			404: {Code: codes.NotFound}}},
		{"ValidateVolumeCapabilities", ValidateVolumeCapabilitiesErrorClassifier, classifyValidateVolumeCapabilitiesError, map[int]controlPlaneErrorClass{
			404: {Code: codes.NotFound}}},
		{"ListSnapshots", ListSnapshotsErrorClassifier, classifyListSnapshotsError, nil},
	}

	for _, rpc := range rpcs {
		// assert both the raw disposition and the observable handler outcome.
		assert := func(t *testing.T, err error, want controlPlaneErrorClass, label string) {
			t.Helper()
			if got := rpc.classifier.Classify(err); got != want {
				t.Errorf("%s: Classify = %+v, want %+v", label, got, want)
			}
			ce := rpc.classify(err)
			got := outcome{code: status.Code(ce), success: ce.IsSuccess(), idempotent: ce.IsIdempotent(), retryable: ce.Retryable()}
			if want := observe(want); got != want {
				t.Errorf("%s: handler outcome = %+v, want %+v", label, got, want)
			}
		}

		t.Run(rpc.name, func(t *testing.T) {
			// Every HTTP status: override wins; else generic; else unhandled 404/409 → bug.
			statuses := map[int]bool{404: true, 409: true}
			for s := range generic {
				statuses[s] = true
			}
			for s := range rpc.overrides {
				statuses[s] = true
			}
			for code := range statuses {
				want, ok := rpc.overrides[code]
				if !ok {
					if want, ok = generic[code]; !ok {
						want = rpcSpecificBug
					}
				}
				assert(t, &util.HTTPError{StatusCode: code}, want, fmt.Sprintf("HTTP %d", code))
			}

			// Every generic transport failure, inherited unchanged.
			for _, tr := range genericTransport {
				assert(t, tr.err, tr.want, tr.label)
			}
		})
	}
}

// TestClassifyRPCErrorFunctions checks every per-RPC classify function delegates
// to its classifier (and thereby exercises all of them).
func TestClassifyRPCErrorFunctions(t *testing.T) {
	err := &util.HTTPError{StatusCode: 404}
	cases := []struct {
		name       string
		fn         func(error) classifiedError
		classifier errorClassifier
	}{
		{"CreateVolume", classifyCreateVolumeError, CreateVolumeErrorClassifier},
		{"DeleteVolume", classifyDeleteVolumeError, DeleteVolumeErrorClassifier},
		{"CreateSnapshot", classifyCreateSnapshotError, CreateSnapshotErrorClassifier},
		{"DeleteSnapshot", classifyDeleteSnapshotError, DeleteSnapshotErrorClassifier},
		{"ControllerExpandVolume", classifyControllerExpandVolumeError, ControllerExpandVolumeErrorClassifier},
		{"ControllerGetVolume", classifyControllerGetVolumeError, ControllerGetVolumeErrorClassifier},
		{"ValidateVolumeCapabilities", classifyValidateVolumeCapabilitiesError, ValidateVolumeCapabilitiesErrorClassifier},
		{"ListSnapshots", classifyListSnapshotsError, ListSnapshotsErrorClassifier},
	}
	for _, tc := range cases {
		got := tc.fn(err)
		if got.class != tc.classifier.Classify(err) || got.err != err {
			t.Errorf("%s: classify function does not delegate to its classifier", tc.name)
		}
	}
}

// TestClassifiedError_MessagePreserved checks the underlying message survives for
// diagnostics.
func TestClassifiedError_MessagePreserved(t *testing.T) {
	underlying := &util.HTTPError{Method: "POST", StatusCode: 400, Message: "bad thing"}
	if d := classifyCreateVolumeError(underlying); d.Error() != underlying.Error() {
		t.Errorf("Error() = %q, want %q", d.Error(), underlying.Error())
	}
}