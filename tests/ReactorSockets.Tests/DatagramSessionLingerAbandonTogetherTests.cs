using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #368 (parent #55): under the negotiated-minimum rule (#363), two Reactors whose own configured
/// Lingers disagree do not merely each individually close on their own effective value — they abandon the
/// Session at the <em>same instant</em>, so a Resume can no longer fail because the peer had a different
/// idea of how long the Session existed. Both harnesses here and the <see cref="LossyProxy"/> between them
/// share one <see cref="FakeTimeProvider"/>, and both ends are given the same send-idle interval and have
/// the path cut in both directions, so both cross their own receive-idle deadline (#352) on the same Pump
/// pair rather than merely at equal, independently-reached clock readings.
/// </summary>
public class DatagramSessionLingerAbandonTogetherTests
{
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    private static readonly TimeSpan _receiveIdleDeadline = _sendIdleInterval * MissedHeartbeats;

    /// <summary>The dialling end's own configured Linger — the shorter of the two, and so the negotiated (effective) value on both ends.</summary>
    private static readonly TimeSpan _shorterLinger = TimeSpan.FromSeconds(5);

    /// <summary>The accepting end's own configured Linger — the longer of the two. If the accepter read this instead of its negotiated minimum, it would still hold the Peer open well past every boundary this file checks.</summary>
    private static readonly TimeSpan _longerLinger = TimeSpan.FromSeconds(8);

    [Fact]
    public void Pump_BothEndsInterrupted_HoldTheInterruptedPeerJustInsideTheShorterLingerAndCloseJustOutsideItAtTheSameInstant()
    {
        // Arrange — one shared clock, one proxy cut in both directions, both ends given the same
        // send-idle interval so both cross their own receive-idle deadline on the same Pump pair.
        FakeTimeProvider clock = new();
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false,
            sendIdleInterval: _sendIdleInterval,
            linger: _shorterLinger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval,
            linger: _longerLinger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        using LossyProxy proxy = new(accepter.Endpoint, clock);

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allDiallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allAccepterEvents = [];

        // Act — cut the path in both directions so neither end hears from the other again, then advance
        // to the receive-idle deadline both ends share (same send-idle interval, same missed-heartbeats
        // count) and pump both, back to back, at the one clock reading that crosses it.
        proxy.StopRelayingBothDirections();
        SettleThenAdvanceBoth(dialler, accepter, _receiveIdleDeadline);

        DateTimeOffset interruptInstant = clock.GetUtcNow();
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DiallerInterrupt,
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AccepterInterrupt) interrupted =
            PumpBothAndDrain(dialler, accepter);
        allDiallerEvents.AddRange(interrupted.DiallerInterrupt);
        allAccepterEvents.AddRange(interrupted.AccepterInterrupt);

        // Assert — both ends interrupted on that same Pump pair, at the same clock reading: nothing
        // advanced the clock between the two Pump calls above, so this is a literal same instant, not
        // merely two harnesses that happened to reach equal readings independently.
        clock.GetUtcNow().Should().Be(interruptInstant, "no clock advance separates the Pump that interrupted the dialler from the one that interrupted the accepter");
        interrupted.DiallerInterrupt.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
        interrupted.AccepterInterrupt.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
        allDiallerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        allAccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — one millisecond short of the shorter (and so negotiated, on both ends) Linger, measured
        // from the interruption instant just captured above.
        SettleThenAdvanceBoth(dialler, accepter, _shorterLinger - TimeSpan.FromMilliseconds(1));
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DiallerJustInside,
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AccepterJustInside) justInside =
            PumpBothAndDrain(dialler, accepter);
        allDiallerEvents.AddRange(justInside.DiallerJustInside);
        allAccepterEvents.AddRange(justInside.AccepterJustInside);

        // Assert — held open on both ends across every drain so far, including the interruption Pump's
        // own drain: neither end's Peer has closed, even though the accepter's own configured Linger is
        // 8s and has not remotely elapsed either.
        allDiallerEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "the negotiated minimum of 5s has not yet elapsed on the dialling end");
        allAccepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "the negotiated minimum of 5s — not the accepter's own 8s — has not yet elapsed on the accepting end");

        // Act — the final millisecond crosses the negotiated minimum on the one shared clock, still
        // pumping both ends back to back at that one reading.
        clock.Advance(TimeSpan.FromMilliseconds(1));
        DateTimeOffset expiryInstant = clock.GetUtcNow();
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DiallerExpiry,
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AccepterExpiry) expired =
            PumpBothAndDrain(dialler, accepter);

        // Assert — both close at this same instant, naming LingerExpired, neither having waited for its
        // own configured value: the dialling end's own 5s (which happens to equal the negotiated minimum
        // here) and, more tellingly, the accepting end's own 8s, which has not elapsed and never governs.
        clock.GetUtcNow().Should().Be(expiryInstant, "no clock advance separates the Pump that closed the dialler from the one that closed the accepter");
        expired.DiallerExpiry.Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed).Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        expired.AccepterExpiry.Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed,
            "the negotiated minimum of 5s has now elapsed, even though the accepter's own configured Linger is 8s")
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    /// <summary>
    /// The two-harness counterpart of <see cref="DatagramSessionReactorHarness.SettleThenAdvance"/>:
    /// splits a large wait on the one clock both harnesses share into "advance to one millisecond short,
    /// Pump both ends once each so each observes a settled, small Pump cadence, then advance the final
    /// millisecond" — needed for the same reason the single-harness version is (see that method's own
    /// remarks): a large advance immediately before an assertion Pump would corrupt the observed-interval
    /// floor either end's <c>RetransmissionTimer</c> reads, for whichever timer that Pump is meant to
    /// exercise.
    /// </summary>
    private static void SettleThenAdvanceBoth(
        DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, TimeSpan interval)
    {
        dialler.TimeProvider.Advance(interval - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        accepter.Reactor.Pump();
        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
    }

    /// <summary>
    /// Pumps the dialler, then the accepter, back to back at whatever clock reading currently stands —
    /// no advance separates the two calls — and drains each into its own list. What every "same instant"
    /// assertion in this file pumps and drains through, so the two ends' Events are always read off the
    /// one Pump pair that produced them.
    /// </summary>
    private static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothAndDrain(DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter)
    {
        dialler.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = dialler.DrainToList();
        accepter.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = accepter.DrainToList();
        return (diallerEvents, accepterEvents);
    }
}
