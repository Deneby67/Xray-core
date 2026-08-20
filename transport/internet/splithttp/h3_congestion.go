package splithttp

import (
	"time"

	aperCongestion "github.com/apernet/quic-go/congestion"
	aperMonotime "github.com/apernet/quic-go/monotime"
	"github.com/quic-go/quic-go"
	quicCongestion "github.com/quic-go/quic-go/congestion"
	quicMonotime "github.com/quic-go/quic-go/monotime"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/bbr"
	"github.com/xtls/xray-core/transport/internet/hysteria/congestion/brutal"
)

type h3CongestionAdapter struct {
	inner       aperCongestion.CongestionControlEx
	newInner    func() aperCongestion.CongestionControlEx
	rttProvider quicCongestion.RTTStatsProvider
	timeOffset  int64
	acked       []aperCongestion.AckedPacketInfo
	lost        []aperCongestion.LostPacketInfo
}

var _ quicCongestion.CongestionControlEx = (*h3CongestionAdapter)(nil)
var _ quicCongestion.PathMigrationAware = (*h3CongestionAdapter)(nil)

func newH3CongestionAdapter(newInner func() aperCongestion.CongestionControlEx) *h3CongestionAdapter {
	now := quicMonotime.Now()
	return &h3CongestionAdapter{
		inner:      newInner(),
		newInner:   newInner,
		timeOffset: int64(aperMonotime.FromTime(now.ToTime())) - int64(now),
	}
}

func (a *h3CongestionAdapter) toAperTime(value quicMonotime.Time) aperMonotime.Time {
	if value.IsZero() {
		return 0
	}
	return aperMonotime.Time(int64(value) + a.timeOffset)
}

func (a *h3CongestionAdapter) toQUICTime(value aperMonotime.Time) quicMonotime.Time {
	if value.IsZero() {
		return 0
	}
	return quicMonotime.Time(int64(value) - a.timeOffset)
}

func (a *h3CongestionAdapter) SetRTTStatsProvider(provider quicCongestion.RTTStatsProvider) {
	a.rttProvider = provider
	a.inner.SetRTTStatsProvider(h3RTTStatsProvider{provider: provider})
}

func (a *h3CongestionAdapter) OnPathMigration() {
	a.inner = a.newInner()
	if a.rttProvider != nil {
		a.inner.SetRTTStatsProvider(h3RTTStatsProvider{provider: a.rttProvider})
	}
}

func (a *h3CongestionAdapter) TimeUntilSend(bytesInFlight quicCongestion.ByteCount) quicMonotime.Time {
	return a.toQUICTime(a.inner.TimeUntilSend(aperCongestion.ByteCount(bytesInFlight)))
}

func (a *h3CongestionAdapter) HasPacingBudget(now quicMonotime.Time) bool {
	return a.inner.HasPacingBudget(a.toAperTime(now))
}

func (a *h3CongestionAdapter) OnPacketSent(sentTime quicMonotime.Time, bytesInFlight quicCongestion.ByteCount, packetNumber quicCongestion.PacketNumber, size quicCongestion.ByteCount, isRetransmittable bool) {
	a.inner.OnPacketSent(
		a.toAperTime(sentTime),
		aperCongestion.ByteCount(bytesInFlight),
		aperCongestion.PacketNumber(packetNumber),
		aperCongestion.ByteCount(size),
		isRetransmittable,
	)
}

func (a *h3CongestionAdapter) CanSend(bytesInFlight quicCongestion.ByteCount) bool {
	return a.inner.CanSend(aperCongestion.ByteCount(bytesInFlight))
}

func (a *h3CongestionAdapter) MaybeExitSlowStart() {
	a.inner.MaybeExitSlowStart()
}

func (a *h3CongestionAdapter) OnPacketAcked(number quicCongestion.PacketNumber, ackedBytes quicCongestion.ByteCount, priorInFlight quicCongestion.ByteCount, eventTime quicMonotime.Time) {
	a.inner.OnPacketAcked(
		aperCongestion.PacketNumber(number),
		aperCongestion.ByteCount(ackedBytes),
		aperCongestion.ByteCount(priorInFlight),
		a.toAperTime(eventTime),
	)
}

func (a *h3CongestionAdapter) OnCongestionEvent(number quicCongestion.PacketNumber, lostBytes quicCongestion.ByteCount, priorInFlight quicCongestion.ByteCount) {
	a.inner.OnCongestionEvent(
		aperCongestion.PacketNumber(number),
		aperCongestion.ByteCount(lostBytes),
		aperCongestion.ByteCount(priorInFlight),
	)
}

func (a *h3CongestionAdapter) OnRetransmissionTimeout(packetsRetransmitted bool) {
	a.inner.OnRetransmissionTimeout(packetsRetransmitted)
}

func (a *h3CongestionAdapter) SetMaxDatagramSize(size quicCongestion.ByteCount) {
	a.inner.SetMaxDatagramSize(aperCongestion.ByteCount(size))
}

func (a *h3CongestionAdapter) InSlowStart() bool {
	return a.inner.InSlowStart()
}

func (a *h3CongestionAdapter) InRecovery() bool {
	return a.inner.InRecovery()
}

func (a *h3CongestionAdapter) GetCongestionWindow() quicCongestion.ByteCount {
	return quicCongestion.ByteCount(a.inner.GetCongestionWindow())
}

func (a *h3CongestionAdapter) OnCongestionEventEx(priorInFlight quicCongestion.ByteCount, eventTime quicMonotime.Time, ackedPackets []quicCongestion.AckedPacketInfo, lostPackets []quicCongestion.LostPacketInfo) {
	if cap(a.acked) < len(ackedPackets) {
		a.acked = make([]aperCongestion.AckedPacketInfo, len(ackedPackets))
	} else {
		a.acked = a.acked[:len(ackedPackets)]
	}
	for index, packet := range ackedPackets {
		a.acked[index] = aperCongestion.AckedPacketInfo{
			PacketNumber: aperCongestion.PacketNumber(packet.PacketNumber),
			BytesAcked:   aperCongestion.ByteCount(packet.BytesAcked),
			ReceivedTime: a.toAperTime(packet.ReceivedTime),
		}
	}
	if cap(a.lost) < len(lostPackets) {
		a.lost = make([]aperCongestion.LostPacketInfo, len(lostPackets))
	} else {
		a.lost = a.lost[:len(lostPackets)]
	}
	for index, packet := range lostPackets {
		a.lost[index] = aperCongestion.LostPacketInfo{
			PacketNumber: aperCongestion.PacketNumber(packet.PacketNumber),
			BytesLost:    aperCongestion.ByteCount(packet.BytesLost),
		}
	}
	a.inner.OnCongestionEventEx(
		aperCongestion.ByteCount(priorInFlight),
		a.toAperTime(eventTime),
		a.acked,
		a.lost,
	)
	a.acked = a.acked[:0]
	a.lost = a.lost[:0]
}

type h3RTTStatsProvider struct {
	provider quicCongestion.RTTStatsProvider
}

func (p h3RTTStatsProvider) MinRTT() time.Duration      { return p.provider.MinRTT() }
func (p h3RTTStatsProvider) LatestRTT() time.Duration   { return p.provider.SmoothedRTT() }
func (p h3RTTStatsProvider) SmoothedRTT() time.Duration { return p.provider.SmoothedRTT() }
func (h3RTTStatsProvider) MeanDeviation() time.Duration { return 0 }
func (h3RTTStatsProvider) MaxAckDelay() time.Duration   { return 0 }
func (p h3RTTStatsProvider) PTO(bool) time.Duration     { return 2 * p.provider.SmoothedRTT() }
func (h3RTTStatsProvider) UpdateRTT(time.Duration, time.Duration) {
}
func (h3RTTStatsProvider) SetMaxAckDelay(time.Duration) {}
func (h3RTTStatsProvider) SetInitialRTT(time.Duration)  {}

func configureH3Congestion(conn *quic.Conn, params *internet.QuicParams) error {
	switch params.Congestion {
	case "reno":
		return nil
	case "", "bbr":
		initialPacketSize := bbr.GetInitialPacketSize(conn.RemoteAddr())
		profile := bbr.Profile(params.BbrProfile)
		return conn.SetCongestionControl(newH3CongestionAdapter(func() aperCongestion.CongestionControlEx {
			return bbr.NewBbrSender(bbr.DefaultClock{}, initialPacketSize, profile)
		}))
	case "force-brutal":
		bitrate := params.BrutalUp
		return conn.SetCongestionControl(newH3CongestionAdapter(func() aperCongestion.CongestionControlEx {
			return brutal.NewBrutalSender(bitrate)
		}))
	default:
		panic(params.Congestion)
	}
}
