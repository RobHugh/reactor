using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.6.15 (issue #225): duplicate and out-of-window arrivals are counters, never Diagnostics.
/// Across one scripted exchange <see cref="PeerCounters.DuplicatePackets"/> and
/// <see cref="PeerCounters.OutOfWindowPackets"/> each rise by exactly one, at different moments, and the
/// Diagnostics Sink receives nothing at all.
/// </summary>
/// <remarks>
/// <para>
/// This is ADR 0013's record test applied. Both are per-packet volume driven by loss and reordering, so
/// a record for either would fill a lossy path's Sink with noise while telling a consumer nothing a
/// counter does not.
/// </para>
/// <para>
/// The Sink runs at <see cref="DiagnosticLevel.Trace"/>, the harness default and the most verbose level
/// there is, so "no Diagnostic" here means none at any level rather than none above some threshold.
/// </para>
/// <para>
/// The receiver's window is left at the default four fragments, because that is what makes an
/// out-of-window arrival reachable at all: four scripted drops leave the base unmoved at zero, and the
/// fifth sequence is then one past the window's end.
/// </para>
/// </remarks>
public class DatagramSessionRefusalCounterTests
{
    /// <summary>Room for every reliable record this test keeps in flight at once (1200 bytes is the default fragment size).</summary>
    private const int RetentionForSeveralInFlight = 1200 * 12;

    /// <summary>The default receive window, four fragments wide, so a Space based at zero admits [0, 4).</summary>
    private const int WindowFragments = 4;

    [Fact]
    public void Pump_AScriptedDuplicateThenAScriptedOutOfWindowArrival_RaiseOneCounterEachAndEmitNoDiagnostic()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WindowFragments);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);
        receiver.Sink.Received.Clear();

        // Act — the duplicate. The first ReliableUnordered Message is dropped so its sequence stays a
        // hole, which keeps the second one classifiable: a sequence admitted at the window's own base is
        // retired as the base advances past it, and its repeat would then read as out of window instead.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, [1]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        proxy.DuplicateNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, [2]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        receiver.DrainAndDiscard();

        // Assert — one counter moved and the other did not, which is what makes them independent rather
        // than two readings of one event.
        PeerCounters afterTheDuplicate = receiver.Reactor.CountersOf(receiverPeer);
        afterTheDuplicate.DuplicatePackets.Should().Be(1UL, "the repeated sequence was recognised exactly once");
        afterTheDuplicate.OutOfWindowPackets.Should().Be(0UL, "a duplicate is not an out-of-window arrival");
        receiver.Sink.Received.Should().BeEmpty("a duplicate is a counter, not a Diagnostic (ADR 0013)");

        // Act — the out-of-window arrival. The ReliableOrdered Space starts at zero with a four-fragment
        // window, so dropping sequences 0 through 3 leaves the base unmoved and sequence 4 lands one past
        // the window's end.
        for (int sequence = 0; sequence < WindowFragments; sequence++)
        {
            proxy.DropNext(RelayDirection.ClientToUpstream);
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [(byte)sequence]).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        }

        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [WindowFragments]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        receiver.DrainAndDiscard();

        // Assert — the other counter moved, by exactly one, and the first stayed where it was.
        PeerCounters afterTheOutOfWindowArrival = receiver.Reactor.CountersOf(receiverPeer);
        afterTheOutOfWindowArrival.OutOfWindowPackets.Should().Be(
            1UL, "the sequence past the window's end was refused and counted exactly once");
        afterTheOutOfWindowArrival.DuplicatePackets.Should().Be(
            1UL, "an out-of-window arrival is not a duplicate, so the duplicate figure is where the earlier step left it");
        receiver.Sink.Received.Should().BeEmpty(
            "neither refusal emits a Diagnostic, at Trace or at any other level (ADR 0013)");
    }
}
