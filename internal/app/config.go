package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"

	"noor/internal/watch/threads"
)

type Config struct {
	SQLitePath         string
	HTTPAddr           string
	WebhookAddr        string
	TelegramToken      string
	NotificationChatID int64
	Retention          time.Duration
	Threads            ThreadsConfig
	Streams            StreamsConfig
}

type ThreadsConfig struct {
	Enabled bool
	Secret  string
	Config  threads.Config
}

type StreamsConfig struct {
	Enabled  bool
	Interval time.Duration
}

type fileConfig struct {
	Telegram struct {
		NotificationChatID int64  `toml:"notification_chat_id"`
		Locale             string `toml:"locale"`
	} `toml:"telegram"`
	Ptchan struct {
		BaseURL string `toml:"base_url"`
	} `toml:"ptchan"`
	Gateway struct {
		IntegrationName string `toml:"integration_name"`
		Webhook         struct {
			Addr string `toml:"addr"`
		} `toml:"webhook"`
	} `toml:"gateway"`
	Threads struct {
		Enabled         bool     `toml:"enabled"`
		MinReplyPosts   int      `toml:"min_reply_posts"`
		KeywordDenylist []string `toml:"keyword_denylist"`
		MaxThreadAge    string   `toml:"max_thread_age"`
	} `toml:"threads"`
	Streams struct {
		Enabled      bool   `toml:"enabled"`
		PollInterval string `toml:"poll_interval"`
	} `toml:"streams"`
	Runtime struct {
		HTTPAddr string `toml:"http_addr"`
	} `toml:"runtime"`
	Storage struct {
		SQLitePath string `toml:"sqlite_path"`
	} `toml:"storage"`
	Retention struct {
		CompletedAfter string `toml:"completed_after"`
	} `toml:"retention"`
}

func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config %q: %w", path, err)
	}
	defer file.Close()
	var raw fileConfig
	decoder := toml.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		var strict *toml.StrictMissingError
		if errors.As(err, &strict) {
			return Config{}, fmt.Errorf("decode config %q:\n%s", path, strict.String())
		}
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	cfg := Config{
		SQLitePath:         cleanPath(raw.Storage.SQLitePath),
		HTTPAddr:           strings.TrimSpace(raw.Runtime.HTTPAddr),
		WebhookAddr:        strings.TrimSpace(raw.Gateway.Webhook.Addr),
		TelegramToken:      strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		NotificationChatID: raw.Telegram.NotificationChatID,
		Threads: ThreadsConfig{Enabled: raw.Threads.Enabled, Config: threads.Config{
			Target: raw.Telegram.NotificationChatID, BaseURL: strings.TrimSpace(raw.Ptchan.BaseURL),
			MinReplyPosts:   raw.Threads.MinReplyPosts,
			KeywordDenylist: raw.Threads.KeywordDenylist,
			Locale:          strings.TrimSpace(raw.Telegram.Locale),
		}},
		Streams: StreamsConfig{Enabled: raw.Streams.Enabled, Interval: 5 * time.Minute},
	}
	cfg.Retention, err = time.ParseDuration(raw.Retention.CompletedAfter)
	if err != nil || cfg.Retention <= 0 {
		return Config{}, fmt.Errorf("retention.completed_after must be a positive duration")
	}
	cfg.Threads.Config.EventRetention = cfg.Retention
	if cfg.Threads.Config.Locale == "" {
		cfg.Threads.Config.Locale = "en"
	}
	if cfg.Threads.Config.Locale != "en" && cfg.Threads.Config.Locale != "pt-PT" {
		return Config{}, fmt.Errorf("telegram.locale must be en or pt-PT")
	}
	if raw.Threads.MaxThreadAge != "" {
		cfg.Threads.Config.MaxThreadAge, err = time.ParseDuration(raw.Threads.MaxThreadAge)
		if err != nil || cfg.Threads.Config.MaxThreadAge < 0 {
			return Config{}, fmt.Errorf("threads.max_thread_age must be a non-negative duration")
		}
	}
	if raw.Streams.PollInterval != "" {
		cfg.Streams.Interval, err = time.ParseDuration(raw.Streams.PollInterval)
		if err != nil || cfg.Streams.Interval <= 0 {
			return Config{}, fmt.Errorf("streams.poll_interval must be a positive duration")
		}
	}
	if cfg.Threads.Enabled {
		name := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(strings.TrimSpace(raw.Gateway.IntegrationName)))
		cfg.Threads.Secret = strings.TrimSpace(os.Getenv("PTCHAN_INTEGRATION_" + name + "_SECRET"))
		if cfg.Threads.Secret == "" || cfg.WebhookAddr == "" || cfg.Threads.Config.MinReplyPosts < 1 || cfg.Threads.Config.BaseURL == "" {
			return Config{}, fmt.Errorf("enabled threads watcher requires ptchan.base_url, webhook address, integration secret, and min_reply_posts")
		}
	}
	if (cfg.Threads.Enabled || cfg.Streams.Enabled) && (cfg.SQLitePath == "" || cfg.TelegramToken == "" || raw.Telegram.NotificationChatID == 0) {
		return Config{}, fmt.Errorf("enabled watchers require storage.sqlite_path, TELEGRAM_BOT_TOKEN, and telegram.notification_chat_id")
	}
	if cfg.HTTPAddr == "" {
		return Config{}, fmt.Errorf("runtime.http_addr is required")
	}
	if !cfg.Threads.Enabled && !cfg.Streams.Enabled {
		return Config{}, fmt.Errorf("enable at least one watcher")
	}
	return cfg, nil
}

func cleanPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return filepath.Clean(value)
}
