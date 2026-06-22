package mtr

import (
	"fmt"
	"strings"
)

// Render formats a Result as an mtr-report-style text table for display.
func Render(r Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "MTR report to %s", r.Target)
	if !r.DestinationReached {
		b.WriteString(" (destination not reached)")
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "%-3s %-39s %6s %4s %8s %8s %8s %8s\n",
		"Hop", "Host", "Loss%", "Snt", "Last", "Avg", "Best", "Wrst")
	for _, h := range r.Hops {
		host := h.IP
		if host == "" {
			host = "???"
		}
		fmt.Fprintf(&b, "%-3d %-39s %5.1f%% %4d %7.1f %7.1f %7.1f %7.1f\n",
			h.Number, host, h.LossPct, h.Sent, h.LastMs, h.AvgMs, h.BestMs, h.WorstMs)
	}
	return b.String()
}
