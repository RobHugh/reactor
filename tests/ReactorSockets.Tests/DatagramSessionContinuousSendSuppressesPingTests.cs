using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #342 (parent #53): the flip side of issue #340's send-idle Ping. A Session sending data more
/// often than <see cref="DatagramSessionOptions.SendIdleInterval"/> never needs a Heartbeat — every
/// data datagram already restarts <see cref="DatagramSessionReactor.PeerSlot.LastSendTimestamp"/>
/// (#340's <c>FlushPeer</c>), so <c>EnforceSendIdleTimer</c> never finds this Peer idle. Both tests
/// here drive a real <see cref="DatagramSessionReactor"/> against another one through a
/// <see cref="WireTap"/>, reading the exact bytes the tap captured off the wire, exactly as
/// <see cref="DatagramSessionSendIdlePingTests"/> does. Traffic here is <c>Unreliable</c>: continuous
/// reliable traffic would either exhaust the default 4096-byte retention budget or need the far end to
/// keep acking it, neither of which this ticket's timer question needs (#340's own known traps).
/// </summary>
public class DatagramSessionContinuousSendSuppressesPingTests
{
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ControlBit = 0b0000_1000;

    // =========================================================================================
    // AC1 — continuous traffic across at least 10 send-idle intervals puts zero control records
    // on the wire
    // =========================================================================================

    [Fact]
    public void Pump_MessageSentMoreOftenThanSendIdleInterval_PutsNoControlRecordOnTheWireAcrossTenIntervals()
    {
        // Arrange — a short SendIdleInterval, and Sends spaced at a quarter of it, so this test
        // crosses the interval boundary four times over before ever giving the timer a chance to fire.
        // Linger is pushed far out so no retransmit deadline (there is none for Unreliable traffic
        // anyway) ever adds an unrelated record to a datagram this test inspects.
        TimeSpan sendIdleInterval = TimeSpan.FromSeconds(1);
        TimeSpan sendInterval = TimeSpan.FromMilliseconds(sendIdleInterval.TotalMilliseconds / 4);
        const int sendsPerInterval = 4;
        const int intervalsToCross = 10;

        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: TimeSpan.FromHours(1), sendIdleInterval: sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Only data datagrams — never the handshake flights captured above, which are not in the
        // established record shape DatagramDecoder decodes — are in scope for this assertion.
        int capturesBeforeSends = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — Send more often than the interval, for at least ten intervals' worth of Pumps.
        for (int i = 0; i < intervalsToCross * sendsPerInterval; i++)
        {
            dialler.TimeProvider.Advance(sendInterval);
            dialler.Reactor.Send(peer, DeliveryMode.Unreliable, [1, 2, 3]).Should().Be(SendOutcome.Sent);
            dialler.Reactor.Pump();
            tap.Pump();
            accepter.Reactor.Pump();
            accepter.DrainAndDiscard();
            tap.Pump();
        }

        // Assert — not one of the data datagrams that left the dialler carries a control record.
        foreach (byte[] datagram in tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Skip(capturesBeforeSends))
        {
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
            decoded.IsSuccess.Should().BeTrue();
            decoded.Datagram!.Records.Should().NotContain(
                record => (record.Flags & ControlBit) == ControlBit,
                "a Session sending more often than SendIdleInterval never finds itself idle enough to Ping");
        }
    }

    // =========================================================================================
    // AC2 — once continuous traffic stops, the Ping arrives exactly one SendIdleInterval after
    // the last data datagram, not before
    // =========================================================================================

    [Fact]
    public void Pump_SendsStopAfterContinuousTraffic_EmitsPingExactlyOneSendIdleIntervalAfterLastDatagram()
    {
        // Arrange — same shape as AC1: continuous Unreliable traffic faster than the interval, so the
        // timer never fires while sends continue.
        TimeSpan sendIdleInterval = TimeSpan.FromSeconds(1);
        TimeSpan sendInterval = TimeSpan.FromMilliseconds(sendIdleInterval.TotalMilliseconds / 4);

        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: TimeSpan.FromHours(1), sendIdleInterval: sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        for (int i = 0; i < 10; i++)
        {
            dialler.TimeProvider.Advance(sendInterval);
            dialler.Reactor.Send(peer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
            dialler.Reactor.Pump();
            tap.Pump();
            accepter.Reactor.Pump();
            accepter.DrainAndDiscard();
            tap.Pump();
        }

        int capturesAfterLastSend = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — Sends stop. One Pump short of a full interval since the last data datagram: no Ping yet.
        dialler.TimeProvider.Advance(sendIdleInterval - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        tap.Pump();

        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesAfterLastSend, "the send-idle interval has not fully elapsed since the last data datagram yet");

        // Act — the interval fully elapses.
        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        tap.Pump();

        // Assert — exactly one further datagram left the dialler, and it is the Ping.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesAfterLastSend + 1, "exactly one Ping leaves, one SendIdleInterval after the last data datagram");

        byte[] datagram = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        decoded.IsSuccess.Should().BeTrue();
        decoded.Datagram!.Records.Should().ContainSingle();

        DecodedPacketRecord record = decoded.Datagram!.Records[0];
        record.Length.Should().Be(0, "a control record never carries a payload");
        (record.Flags & ModeReliableUnordered).Should().Be(ModeReliableUnordered, "a control record is always ReliableUnordered");
        (record.Flags & ControlBit).Should().Be(ControlBit, "the datagram that finally leaves is the Ping, not another data record");
    }
}
