using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.4.1e (issue #142): the negative case that gives Interrupted's Slot reservation (issue #122)
/// its shape. Goodbye is the one thing permitted to forbid a Resume (ADR 0010), so a Peer that leaves
/// by Goodbye holds no state and reserves nothing — its Slot is free the moment it closes, whether the
/// Goodbye was this end's own <see cref="StreamSessionReactor.Close"/> or one that arrived from the
/// counterparty, and no Linger deadline ever follows it. Mirrors <see cref="StreamSessionGoodbyeTests"/>
/// for the wire shape of a Goodbye and <see cref="StreamSessionHandshakeGuardTests"/> for
/// <see cref="StreamSessionReactorHarness.FillToCapacity"/> and
/// <see cref="StreamSessionReactorHarness.AttemptFreshConnection"/>, the house pattern for driving a
/// Reactor to capacity and observing what a fresh connection sees next (issue #138).
/// </summary>
public class StreamSessionGoodbyeSlotReleaseTests
{
    private const uint GoodbyeSentinel = uint.MaxValue;

    private static byte[] BuildGoodbye() => StreamSessionTestRecords.BuildPrefix(GoodbyeSentinel);

    // =========================================================================================
    // Slot release: a Goodbye close frees its Slot at once, with no time ever advanced (AC1, AC2)
    // =========================================================================================

    [Fact]
    public void Close_ByThisEndsOwnGoodbye_ReleasesTheSlotSoAFreshConnectionIsAdmittedWithNoTimeAdvanced()
    {
        // Arrange — the Reactor's single capacity Slot is occupied. Nothing is ever queued for the
        // occupant, so this end's own Close closes it directly, with its own Goodbye, on the very Pump
        // that sends it (mirrors StreamSessionGoodbyeTests.Close_CalledTwice_IsIdempotentAndSendsExactlyOneGoodbye).
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        StreamSessionReactorHarness.HeldPeer occupant = held[0];

        // Act — close by this end's own Goodbye, then attempt a fresh connection without ever calling
        // Advance on the FakeTimeProvider.
        accepter.Reactor.Close(occupant.Peer);
        accepter.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — closed with LocalClose (no reservation held), and the fresh connection was admitted
        // at once rather than refused for capacity.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == occupant.Peer)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
        outcome.Admitted.Should().BeTrue();
        outcome.Peer.Should().NotBeNull();

        occupant.Counterparty.Dispose();
    }

    [Fact]
    public void Close_ByTheCounterpartysGoodbye_ReleasesTheSlotSoAFreshConnectionIsAdmittedWithNoTimeAdvanced()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        StreamSessionReactorHarness.HeldPeer occupant = held[0];

        // Act — the counterparty announces its own departure. Nothing was ever queued for it either
        // way, so the Goodbye closes the Peer directly with no observable Closing in between (mirrors
        // StreamSessionGoodbyeTests.GoodbyeReceived_WithNothingOutstandingInThisEndsOwnRetention_
        // ClosesDirectlyWithNoObservableClosingState). No FakeTimeProvider.Advance call anywhere here.
        occupant.Counterparty.Send(BuildGoodbye());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — closed with PeerGoodbye (no reservation held), and the fresh connection was admitted
        // at once.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == occupant.Peer)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        outcome.Admitted.Should().BeTrue();
        outcome.Peer.Should().NotBeNull();

        occupant.Counterparty.Dispose();
    }

    // =========================================================================================
    // No Linger deadline ever follows a Peer that left by Goodbye (AC3)
    // =========================================================================================

    [Fact]
    public void Pump_WellPastTheEffectiveLingerAfterThisEndsOwnGoodbyeClose_RaisesNoFurtherEventForThatPeer()
    {
        // Arrange — a single established Peer is enough; capacity plays no role in proving the
        // absence of a Linger deadline.
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send);
        peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);

        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == id)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);

        // Act — advance well past the effective Linger and pump repeatedly: if a Linger deadline had
        // wrongly been reserved for this Peer, this is when it would fire.
        accepter.TimeProvider.Advance(TimeSpan.FromSeconds(5));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.PumpAndDrainAll(5);

        // Assert — nothing further was ever raised for the Peer that left by Goodbye.
        drained.Should().NotContain(e => e.Peer == id);
    }

    [Fact]
    public void Pump_WellPastTheEffectiveLingerAfterTheCounterpartysGoodbyeClose_RaisesNoFurtherEventForThatPeer()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send);
        peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);

        peer.Send(BuildGoodbye());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> establishDrained = [];
        for (int i = 0; i < 20 && !establishDrained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            establishDrained.AddRange(accepter.DrainToList());
        }

        establishDrained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == id)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);

        // Act — advance well past the effective Linger and pump repeatedly.
        accepter.TimeProvider.Advance(TimeSpan.FromSeconds(5));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.PumpAndDrainAll(5);

        // Assert — nothing further was ever raised for the Peer that left by Goodbye.
        drained.Should().NotContain(e => e.Peer == id);
    }
}
