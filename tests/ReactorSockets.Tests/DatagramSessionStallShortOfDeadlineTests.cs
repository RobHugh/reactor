using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #356 (parent #54): the receive-idle count (#352) guards against lateness, not a single missed
/// interval. A peer that stalls for a little over one <c>RemoteSendIdleInterval</c> — and then resumes —
/// must not be declared dead at the default <c>MissedHeartbeats</c> of 3, because the deadline is
/// <c>RemoteSendIdleInterval × MissedHeartbeats</c> from the peer's <em>last</em> received datagram, not
/// from the datagram before the stall.
/// </summary>
/// <remarks>
/// Drives two real <see cref="DatagramSessionReactor"/>s through a <see cref="LossyProxy"/> on a shared
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>: connects, drops the dialler-to-proxy
/// direction for 1.2 send-idle intervals, resumes relaying, then pumps for several more intervals so
/// that if <c>PeerSlot.LastReceiveTimestamp</c> were never advanced by the resumed traffic, the deadline
/// measured from establishment would already have fired. A Send after the stall, drained on the
/// accepter side, proves the Session still delivers.
/// </remarks>
public class DatagramSessionStallShortOfDeadlineTests
{
    /// <summary>
    /// Short enough that several multiples of it are a small clock advance, and, at
    /// <see cref="MissedHeartbeats"/> below, far under the Linger this file uses so nothing here is
    /// mistaken for a Linger expiry.
    /// </summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than any clock advance in this file so nothing here is mistaken for a Linger expiry.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    [Fact]
    public void Pump_StallJustOverOneInterval_DoesNotInterruptAndStillDeliversAfterwards()
    {
        // Arrange — one shared clock, so the accepter's own send-idle Ping (needed to keep the
        // dialler's receive-idle deadline alive across the resumed quiet phase) actually fires rather
        // than sitting frozen on a clock nothing ever advances.
        FakeTimeProvider timeProvider = new();
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats,
            timeProvider: timeProvider);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats,
            timeProvider: timeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, timeProvider);

        PeerId dialledPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — stall the accepter-to-dialler direction for 1.2 intervals: a little over one missed
        // heartbeat, well short of the MissedHeartbeats-count deadline that guards against it.
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, _ => true);

        TimeSpan stall = TimeSpan.FromMilliseconds(_sendIdleInterval.TotalMilliseconds * 1.2);
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, stall);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> duringStall =
            dialler.PumpAndDrainAll(1);
        duringStall.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted, "a stall of a little over one interval is within the default count");

        // Act — resume relaying and pump both sides through several more intervals. If
        // LastReceiveTimestamp were never advanced by the resumed traffic, the MissedHeartbeats-count
        // deadline measured from establishment would already have elapsed by now.
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, null);

        for (int i = 0; i < MissedHeartbeats * 2; i++)
        {
            DatagramSessionReactorHarness.SettleThenAdvance(dialler, _sendIdleInterval);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
        }

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> afterResuming =
            dialler.PumpAndDrainAll(1);
        afterResuming.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);

        // Assert — the Session still delivers a Message afterwards.
        SendOutcome outcome = accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, "still alive"u8);
        outcome.Should().Be(SendOutcome.Sent);

        DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
        dialler.Reactor.Pump();

        dialler.DrainPayloads().Should().ContainSingle(
            payload => System.Text.Encoding.UTF8.GetString(payload) == "still alive");
    }
}
