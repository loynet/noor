package localization

import "fmt"

type HighlightKind string

const (
	SpeedHighlight        HighlightKind = "speed"
	AccelerationHighlight HighlightKind = "acceleration"
	CapcodeHighlight      HighlightKind = "capcode"
	MartaHighlight        HighlightKind = "marta"
	LongFormHighlight     HighlightKind = "long_form"
	MediaHighlight        HighlightKind = "media"
)

type Highlight struct {
	Kind           HighlightKind
	Percent, Count int
}
type ThreadNotification struct {
	Board        string
	ThreadID     int64
	Threshold    int
	Elapsed, URL string
	Highlight    Highlight
}

func Thread(locale string, n ThreadNotification) string {
	var text string
	if locale == "pt-PT" {
		text = fmt.Sprintf("/%s/ #%d - Atingiu %d respostas em %s", n.Board, n.ThreadID, n.Threshold, n.Elapsed)
		if line := portuguese(n.Highlight); line != "" {
			text += "\n" + line
		}
	} else {
		text = fmt.Sprintf("/%s/ #%d - Reached %d replies in %s", n.Board, n.ThreadID, n.Threshold, n.Elapsed)
		if line := english(n.Highlight); line != "" {
			text += "\n" + line
		}
	}
	return text + "\n" + n.URL
}
func english(h Highlight) string {
	switch h.Kind {
	case SpeedHighlight:
		return fmt.Sprintf("⚡ Blink and you missed it: top %d%% fastest this month", h.Percent)
	case AccelerationHighlight:
		return fmt.Sprintf("🔥 It caught fire: top %d%% fastest acceleration this month", h.Percent)
	case CapcodeHighlight:
		return fmt.Sprintf("👑 Celebrity magnet: top %d%% with %d Capcode replies", h.Percent, h.Count)
	case MartaHighlight:
		return fmt.Sprintf("🤖 Loved by robots: top %d%% with %d Marta replies", h.Percent, h.Count)
	case LongFormHighlight:
		return fmt.Sprintf("📝 Yapfest: top %d%% longest replies this month", h.Percent)
	case MediaHighlight:
		return fmt.Sprintf("🖼️ Server storage felt it: top %d%% most media-heavy this month", h.Percent)
	}
	return ""
}
func portuguese(h Highlight) string {
	switch h.Kind {
	case SpeedHighlight:
		return fmt.Sprintf("⚡ Foi num instante: entre os %d%% mais rápidos deste mês", h.Percent)
	case AccelerationHighlight:
		return fmt.Sprintf("🔥 Pegou fogo: entre os %d%% que mais aceleraram este mês", h.Percent)
	case CapcodeHighlight:
		return fmt.Sprintf("👑 Íman de celebridades: entre os %d%% com %d respostas com Capcode", h.Percent, h.Count)
	case MartaHighlight:
		return fmt.Sprintf("🤖 Os robôs adoraram: entre os %d%% com %d respostas da Marta", h.Percent, h.Count)
	case LongFormHighlight:
		return fmt.Sprintf("📝 Tagarelice a sério: entre os %d%% com as respostas mais longas deste mês", h.Percent)
	case MediaHighlight:
		return fmt.Sprintf("🖼️ Encheu o disco: entre os %d%% com mais ficheiros deste mês", h.Percent)
	}
	return ""
}
