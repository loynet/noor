package localization

import (
	"strings"
	"testing"
)

func TestThreadEnglish(t *testing.T) {
	got := Thread("en", ThreadNotification{Board: "test", ThreadID: 123, Threshold: 10, Elapsed: "38m", URL: "https://ptchan.org/test/thread/123.html", Highlight: Highlight{Kind: SpeedHighlight, Percent: 4}})
	if !strings.Contains(got, "/test/ #123 - Reached 10 replies in 38m") || !strings.Contains(got, "top 4% fastest this month") {
		t.Fatalf("rendered notification: %q", got)
	}
}

func TestThreadPortuguese(t *testing.T) {
	got := Thread("pt-PT", ThreadNotification{Board: "test", ThreadID: 123, Threshold: 10, Elapsed: "38m", URL: "https://ptchan.org/test/thread/123.html", Highlight: Highlight{Kind: CapcodeHighlight, Percent: 4, Count: 4}})
	if !strings.Contains(got, "/test/ #123 - Atingiu 10 respostas em 38m") || !strings.Contains(got, "Íman de celebridades") {
		t.Fatalf("rendered notification: %q", got)
	}
}
