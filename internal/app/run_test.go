package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gateway "github.com/loynet/ptchan-gateway/clients/go"
)

func TestMetricThreadKind(t *testing.T) {
	for input, want := range map[gateway.EventKind]string{
		gateway.ThreadCreated:            "thread.created",
		gateway.PostCreated:              "post.created",
		gateway.EventKind("future.kind"): "unknown",
	} {
		if got := metricThreadKind(input); got != want {
			t.Errorf("metricThreadKind(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestGatewayServerRejectsOversizedBody(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/internal/ptchan/events", strings.NewReader(strings.Repeat("x", gateway.DefaultWebhookMaxBodyBytes+1)))
	response := httptest.NewRecorder()
	gatewayHandler("secret", nil, newMetrics()).ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}
