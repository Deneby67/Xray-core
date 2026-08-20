package splithttp

import (
	"testing"
	"time"

	aperCongestion "github.com/apernet/quic-go/congestion"
	quicCongestion "github.com/quic-go/quic-go/congestion"
	quicMonotime "github.com/quic-go/quic-go/monotime"
	"github.com/stretchr/testify/require"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/brutal"
)

func Test_h3CongestionAdapter_preservesMonotonicEpoch(t *testing.T) {
	adapter := newH3CongestionAdapter(func() aperCongestion.CongestionControlEx {
		return brutal.NewBrutalSender(100_000_000)
	})
	now := quicMonotime.Now()

	require.Equal(t, now, adapter.toQUICTime(adapter.toAperTime(now)))
	require.WithinDuration(t, now.ToTime(), adapter.toAperTime(now).ToTime(), time.Nanosecond)
	require.Zero(t, adapter.toAperTime(0))
	require.Zero(t, adapter.toQUICTime(0))
	require.Zero(t, adapter.TimeUntilSend(0))
}

func Test_h3CongestionAdapter_reusesEventBuffers(t *testing.T) {
	adapter := newH3CongestionAdapter(func() aperCongestion.CongestionControlEx {
		return brutal.NewBrutalSender(100_000_000)
	})
	acked := make([]quicCongestion.AckedPacketInfo, 64)
	lost := make([]quicCongestion.LostPacketInfo, 32)
	now := quicMonotime.Now()
	for index := range acked {
		acked[index] = quicCongestion.AckedPacketInfo{
			PacketNumber: quicCongestion.PacketNumber(index),
			BytesAcked:   1200,
			ReceivedTime: now,
		}
	}
	for index := range lost {
		lost[index] = quicCongestion.LostPacketInfo{
			PacketNumber: quicCongestion.PacketNumber(index + len(acked)),
			BytesLost:    1200,
		}
	}
	adapter.OnCongestionEventEx(115_200, now, acked, lost)

	allocations := testing.AllocsPerRun(100, func() {
		adapter.OnCongestionEventEx(115_200, now, acked, lost)
	})

	require.Zero(t, allocations)
	require.GreaterOrEqual(t, cap(adapter.acked), len(acked))
	require.GreaterOrEqual(t, cap(adapter.lost), len(lost))
}

func Test_h3CongestionAdapter_resetsPathSpecificState(t *testing.T) {
	created := 0
	adapter := newH3CongestionAdapter(func() aperCongestion.CongestionControlEx {
		created++
		return brutal.NewBrutalSender(100_000_000)
	})
	adapter.SetRTTStatsProvider(fixedH3RTTStatsProvider{
		minRTT:      50 * time.Millisecond,
		smoothedRTT: 100 * time.Millisecond,
	})
	adapter.SetMaxDatagramSize(1450)
	acked := make([]quicCongestion.AckedPacketInfo, 80)
	lost := make([]quicCongestion.LostPacketInfo, 20)
	adapter.OnCongestionEventEx(145_000, quicMonotime.Now(), acked, lost)
	congestionWindowBeforeMigration := adapter.GetCongestionWindow()
	innerBeforeMigration := adapter.inner

	adapter.OnPathMigration()
	adapter.SetMaxDatagramSize(1280)

	require.Equal(t, 2, created)
	require.NotSame(t, innerBeforeMigration, adapter.inner)
	require.Less(t, adapter.GetCongestionWindow(), congestionWindowBeforeMigration)
	require.GreaterOrEqual(t, cap(adapter.acked), len(acked))
	require.GreaterOrEqual(t, cap(adapter.lost), len(lost))
}

type fixedH3RTTStatsProvider struct {
	minRTT      time.Duration
	smoothedRTT time.Duration
}

func (p fixedH3RTTStatsProvider) MinRTT() time.Duration      { return p.minRTT }
func (p fixedH3RTTStatsProvider) SmoothedRTT() time.Duration { return p.smoothedRTT }
