using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #371 (parent #55): once a datagram Peer transitions to
/// <see cref="SessionEventKind.PeerInterrupted"/>, the Interrupted end keeps sending Pings — the
/// seven-byte <c>ReliableUnordered</c> control record <see cref="DatagramSessionSendIdlePingTests"/>
/// proves the shape of — at the send-idle interval or as the adaptive retransmission of one still
/// unacknowledged. A datagram Session has no re-dial schedule (#55): there is nothing to re-dial, and
/// the interrupted end can only resume by being heard from again, so it keeps probing the path with
/// Pings for as long as Linger holds the Peer alive. <c>EnforceSendIdleTimer</c> does not read
/// <c>PeerSlot.IsInterrupted</c>, and its XML doc records that decision.
/// </summary>
/// <remarks>
/// Drives a real <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on a
/// shared <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>, cuts both directions with
/// <see cref="LossyProxy.StopRelayingBothDirections"/> so every datagram the dialler emits afterward is
/// still captured (<see cref="LossyProxy.Captures"/> records arrivals before any drop rule runs) but
/// never reaches the accepter — matching #55's picture of a Peer resuming only by being heard from
/// again, never by a re-dial. Every Ping captured this way is never acknowledged, so its own adaptive
/// retransmission fires too; this file counts every seven-byte control record captured strictly after
/// the transition, regardless of which mechanism queued it. Every Pump drains its Events, and the whole
/// run is checked for <see cref="SessionEventKind.PeerClosed"/>: the Peer must stay Interrupted and
/// never close while it pings. Linger is set far longer than every clock advance this test makes, so
/// nothing here is ever a Linger expiry.
/// </remarks>
public class DatagramSessionInterruptedPeerPingsTests
{
    private const byte ControlBit = 0b0000_1000;

    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than every clock advance this test makes, so nothing here is a Linger expiry.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    [Fact]
    public void Pump_PeerStaysInterrupted_KeepsEmittingSevenByteControlRecordsAndNeverCloses()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allDiallerEvents = [];

        // Act — cut the path in both directions; the dialler's own datagrams are still captured by the
        // proxy (arrivals are recorded before any drop rule is applied) but never reach the accepter, so
        // this Peer can only resume by being heard from again, exactly as #55 describes.
        proxy.StopRelayingBothDirections();

        TimeSpan receiveIdleDeadline = _sendIdleInterval * MissedHeartbeats;
        allDiallerEvents.AddRange(SettleThenAdvanceAndDrain(dialler, receiveIdleDeadline));
        allDiallerEvents.AddRange(dialler.PumpAndDrainAll(1));
        proxy.Pump();

        allDiallerEvents.Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer),
            "the receive-idle deadline was crossed once while the path was cut");

        int captureCountAtInterruption = proxy.Captures.Count;

        // Act — keep Pumping across several further send-idle intervals; nothing relays anything back to
        // the dialler, so every Ping it queues is also a candidate for its own adaptive retransmission —
        // this loop proves the Interrupted end keeps probing regardless of which mechanism queued each
        // one.
        for (int i = 0; i < 6; i++)
        {
            allDiallerEvents.AddRange(SettleThenAdvanceAndDrain(dialler, _sendIdleInterval));
            allDiallerEvents.AddRange(dialler.PumpAndDrainAll(1));
            proxy.Pump();
        }

        // Assert — the Peer never closed at any point across the whole run.
        allDiallerEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed, "the Peer stays Interrupted while it pings, not closed");

        // Assert — more than one seven-byte control record left the dialler strictly after the
        // transition to Interrupted: a single Ping already in flight at the moment of interruption would
        // not prove the Interrupted end keeps probing the path.
        List<RelayCapture> controlRecordsAfterInterruption = proxy.Captures
            .Skip(captureCountAtInterruption)
            .Where(c => c.Direction == RelayDirection.ClientToUpstream && CarriesOneSevenByteControlRecord(c.Payload))
            .ToList();

        controlRecordsAfterInterruption.Count.Should().BeGreaterThan(
            1, "the Interrupted end has no re-dial schedule and keeps probing the path with Pings");
    }

    /// <summary>
    /// Runs <see cref="DatagramSessionReactorHarness.SettleThenAdvance"/>'s own settle-then-advance
    /// sequence against <paramref name="harness"/>, but drains the Pump it issues instead of leaving it
    /// undrained: <see cref="DatagramSessionReactor.Pump"/> clears any pending Events still sitting from
    /// a previous, undrained Pump the moment a new one starts, so a <see cref="SessionEventKind.PeerClosed"/>
    /// raised on this Pump would otherwise never reach an assertion.
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> SettleThenAdvanceAndDrain(
        DatagramSessionReactorHarness harness, TimeSpan interval)
    {
        harness.TimeProvider.Advance(interval - TimeSpan.FromMilliseconds(1));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = harness.DrainToList();
        harness.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        return drained;
    }

    /// <summary>
    /// Whether <paramref name="payload"/> — a datagram captured off the wire — carries exactly one
    /// record that is the seven-byte <c>ReliableUnordered</c> control record a Ping always takes
    /// (<see cref="DatagramSessionSendIdlePingTests"/>): zero-length, with the control bit set.
    /// </summary>
    private static bool CarriesOneSevenByteControlRecord(byte[] payload)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
        if (!decoded.IsSuccess || decoded.Datagram!.Records.Count != 1)
        {
            return false;
        }

        DecodedPacketRecord record = decoded.Datagram.Records[0];
        return record.Length == 0 && (record.Flags & ControlBit) == ControlBit;
    }
}
