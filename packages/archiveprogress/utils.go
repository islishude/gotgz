package archiveprogress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/islishude/gotgz/packages/cli"
)

// shouldEnableProgress decides whether progress output is active for this run.
func shouldEnableProgress(mode cli.ProgressMode, writer io.Writer, interactive bool) bool {
	if writer == nil {
		return false
	}
	switch mode {
	case cli.ProgressNever:
		return false
	case cli.ProgressAlways:
		return true
	case "", cli.ProgressAuto:
		return interactive
	default:
		return interactive
	}
}

// isInteractiveTTY checks whether the writer is an interactive terminal.
// Only the "dumb" TERM value disables detection; an empty TERM (common in
// containers) is accepted as long as the fd is a character device.
func isInteractiveTTY(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	return true
}

// FormatBytes formats bytes using IEC units.
func FormatBytes(v int64) string {
	if v < 0 {
		v = 0
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	value := float64(v)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%dB", v)
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}

// FormatRate formats byte-per-second throughput values.
func FormatRate(v float64) string {
	if v <= 0 {
		return "0B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	value := v
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0fB", value)
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}

// FormatClock formats a duration as MM:SS or HH:MM:SS for user-facing output.
func FormatClock(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d / time.Hour)
	m := int((d % time.Hour) / time.Minute)
	s := int((d % time.Minute) / time.Second)
	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}
