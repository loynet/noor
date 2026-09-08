package localization

import (
	"strings"
	"testing"
	"time"
)

func TestThreadEnglish(t *testing.T) {
	got := Thread("en", ThreadNotification{Board: "test", ThreadID: 123, Threshold: 10, Elapsed: 38 * time.Minute, URL: "https://ptchan.org/test/thread/123.html", Highlight: Highlight{Kind: SpeedHighlight, Percent: 4}})
	if !strings.Contains(got, "/test/ #123 - Reached 10 replies in 38 minutes") || !strings.Contains(got, "top 4% fastest this month") {
		t.Fatalf("rendered notification: %q", got)
	}
}

func TestThreadPortuguese(t *testing.T) {
	got := Thread("pt-PT", ThreadNotification{Board: "test", ThreadID: 123, Threshold: 10, Elapsed: 38 * time.Minute, URL: "https://ptchan.org/test/thread/123.html", Highlight: Highlight{Kind: CapcodeHighlight, Percent: 4, Count: 4}})
	if !strings.Contains(got, "/test/ #123 - Atingiu 10 respostas em 38 minutos") || !strings.Contains(got, "Íman de celebridades") {
		t.Fatalf("rendered notification: %q", got)
	}
}

func TestThreadUsesHumanDuration(t *testing.T) {
	got := Thread("en", ThreadNotification{Elapsed: 8*24*time.Hour + 2*time.Hour + 34*time.Minute})
	if !strings.Contains(got, "1 week, 1 day, 2 hours, 34 minutes") {
		t.Fatalf("rendered notification: %q", got)
	}
	got = Thread("pt-PT", ThreadNotification{Elapsed: 30 * time.Second})
	if !strings.Contains(got, "menos de um minuto") {
		t.Fatalf("rendered notification: %q", got)
	}
}
