// Package pbconv converts between domain types and their protobuf wire
// representations for the AgentLink stream.
package pbconv

import (
	"time"

	pb "github.com/slothnetworks/artemis/internal/pb/artemis/v1"
	"github.com/slothnetworks/artemis/internal/stats"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotToPB converts a stats.Snapshot to its protobuf form.
func SnapshotToPB(s stats.Snapshot) *pb.Snapshot {
	return &pb.Snapshot{
		Target:       s.Target,
		Sent:         s.Sent,
		Received:     s.Received,
		Dropped:      s.Dropped,
		Timeout:      s.Timeout,
		LossPct:      s.LossPct,
		MinNs:        int64(s.Min),
		AvgNs:        int64(s.Avg),
		P50Ns:        int64(s.P50),
		P95Ns:        int64(s.P95),
		P99Ns:        int64(s.P99),
		MaxNs:        int64(s.Max),
		JitterNs:     int64(s.Jitter),
		Bytes:        s.Bytes,
		Throughput:   s.Throughput,
		PathMtu:      int32(s.PathMTU),
		EffectiveMss: int32(s.EffectiveMSS),
		WindowNs:     int64(s.Window),
		StartedAt:    timestamppb.New(s.StartedAt),
		EndedAt:      timestamppb.New(s.EndedAt),
	}
}

// SnapshotFromPB converts a protobuf Snapshot to a stats.Snapshot.
func SnapshotFromPB(p *pb.Snapshot) stats.Snapshot {
	if p == nil {
		return stats.Snapshot{}
	}
	return stats.Snapshot{
		Target:       p.GetTarget(),
		Sent:         p.GetSent(),
		Received:     p.GetReceived(),
		Dropped:      p.GetDropped(),
		Timeout:      p.GetTimeout(),
		LossPct:      p.GetLossPct(),
		Min:          time.Duration(p.GetMinNs()),
		Avg:          time.Duration(p.GetAvgNs()),
		P50:          time.Duration(p.GetP50Ns()),
		P95:          time.Duration(p.GetP95Ns()),
		P99:          time.Duration(p.GetP99Ns()),
		Max:          time.Duration(p.GetMaxNs()),
		Jitter:       time.Duration(p.GetJitterNs()),
		Bytes:        p.GetBytes(),
		Throughput:   p.GetThroughput(),
		PathMTU:      int(p.GetPathMtu()),
		EffectiveMSS: int(p.GetEffectiveMss()),
		Window:       time.Duration(p.GetWindowNs()),
		StartedAt:    p.GetStartedAt().AsTime(),
		EndedAt:      p.GetEndedAt().AsTime(),
	}
}
