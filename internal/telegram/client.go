package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

type Client struct {
	baseURL    string
	httpClient *http.Client
}

type APIError struct {
	StatusCode  int
	Description string
	RetryAfter  time.Duration
}

func (e *APIError) Error() string             { return "telegram api error: " + e.Description }
func (e *APIError) RetryDelay() time.Duration { return e.RetryAfter }
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= http.StatusInternalServerError
}

func New(token string) *Client {
	return &Client{
		baseURL:    "https://api.telegram.org/bot" + token,
		httpClient: &http.Client{Timeout: 40 * time.Second},
	}
}

func (c *Client) Send(ctx context.Context, chatID int64, text string) error {
	form := url.Values{}
	form.Set("chat_id", fmt.Sprintf("%d", chatID))
	form.Set("text", text)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/sendMessage", strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create telegram request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := c.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		var urlError *url.Error
		if errors.As(err, &urlError) {
			err = urlError.Err
		}
		return fmt.Errorf("send telegram request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read telegram response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("telegram response exceeds %d bytes", maxResponseBytes)
	}
	var result struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("telegram unexpected status: %s", response.Status)
		}
		return fmt.Errorf("decode telegram response: %w", err)
	}
	if !result.OK {
		return &APIError{StatusCode: response.StatusCode, Description: result.Description, RetryAfter: time.Duration(result.Parameters.RetryAfter) * time.Second}
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram unexpected status: %s", response.Status)
	}
	return nil
}
