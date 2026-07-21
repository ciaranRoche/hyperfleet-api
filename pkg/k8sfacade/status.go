package k8sfacade

import (
	"context"
	"encoding/json"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
)

// writeJSON writes a Kubernetes API JSON response.
func writeJSON(ctx context.Context, w http.ResponseWriter, code int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.WithError(ctx, err).Error("k8sfacade: failed to encode response")
	}
}

// writeStatusError writes a metav1.Status error the way a kube-apiserver
// would, so client-go error helpers (IsNotFound, IsGone, ...) work.
func writeStatusError(
	ctx context.Context, w http.ResponseWriter,
	code int, reason metav1.StatusReason, message string, details *metav1.StatusDetails,
) {
	status := metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Code:     int32(code),
		Reason:   reason,
		Message:  message,
		Details:  details,
	}
	writeJSON(ctx, w, code, status)
}

func writeNotFound(ctx context.Context, w http.ResponseWriter, kind, name string) {
	writeStatusError(ctx, w, http.StatusNotFound, metav1.StatusReasonNotFound,
		kind+" \""+name+"\" not found",
		&metav1.StatusDetails{Group: Group, Kind: kind, Name: name})
}

func writeInternalError(ctx context.Context, w http.ResponseWriter, message string) {
	writeStatusError(ctx, w, http.StatusInternalServerError,
		metav1.StatusReasonInternalError, message, nil)
}

func writeBadRequest(ctx context.Context, w http.ResponseWriter, message string) {
	writeStatusError(ctx, w, http.StatusBadRequest, metav1.StatusReasonBadRequest, message, nil)
}

func writeExpired(ctx context.Context, w http.ResponseWriter, message string) {
	writeStatusError(ctx, w, http.StatusGone, metav1.StatusReasonExpired, message, nil)
}
