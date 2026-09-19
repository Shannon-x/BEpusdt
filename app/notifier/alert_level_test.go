package notifier

import (
	"testing"

	"github.com/v03413/bepusdt/app/model"
)

// notifier_alerts 决定推送范围：off 全不推，important（默认）只推重要，all 全推
func TestAlertLevelGate(t *testing.T) {
	old := GetC
	t.Cleanup(func() { GetC = old })

	cases := []struct {
		level           string
		wantImportant   bool
		wantInformative bool
	}{
		{AlertsOff, false, false},
		{AlertsImportant, true, false},
		{"", false, false}, // 未配置时按 off
		{AlertsAll, true, true},
	}

	for _, c := range cases {
		GetC = func(k model.ConfKey) string {
			if k == model.NotifierAlerts {
				return c.level
			}

			return ""
		}

		if got := shouldSend(true, c.level); got != c.wantImportant {
			t.Errorf("level=%q important=%v，期望 %v", c.level, got, c.wantImportant)
		}
		if got := shouldSend(false, c.level); got != c.wantInformative {
			t.Errorf("level=%q notice=%v，期望 %v", c.level, got, c.wantInformative)
		}
	}
}
