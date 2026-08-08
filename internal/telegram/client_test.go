package telegram

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSendExposesTelegramRetryAfter(t *testing.T) {
	client := &Client{baseURL: "https://api.telegram.org/bottoken", httpClient: &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests", Body: io.NopCloser(strings.NewReader(`{"ok":false,"description":"Too Many Requests","parameters":{"retry_after":42}}`))}, nil
	})}}
	err := client.Send(context.Background(), 1, "hello")
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.RetryAfter != 42*time.Second {
		t.Fatalf("error = %#v, want retry_after 42s", err)
	}
	if !apiErr.Retryable() {
		t.Fatal("rate limit error is not retryable")
	}
}

func TestSendClassifiesTelegramRejectionAsPermanent(t *testing.T) {
	client := &Client{baseURL: "https://api.telegram.org/bottoken", httpClient: &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request", Body: io.NopCloser(strings.NewReader(`{"ok":false,"description":"chat not found"}`))}, nil
	})}}
	err := client.Send(context.Background(), 1, "hello")
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Retryable() {
		t.Fatalf("error = %#v, want permanent APIError", err)
	}
}

func TestSendRejectsOversizedResponse(t *testing.T) {
	client := &Client{baseURL: "https://api.telegram.org/bottoken", httpClient: &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", maxResponseBytes+1)))}, nil
	})}}
	if err := client.Send(context.Background(), 1, "hello"); err == nil {
		t.Fatal("oversized response accepted")
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
