using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The Frame arena observed through the surface a consumer has: the read-share escape, the arena's own
/// bound, and what a Frame does to the previous Frame's bytes. The arena itself is internal, so every
/// assertion here is on drained Events and Diagnostic records.
/// </summary>
public class RawReactorFrameTests
{
    [Fact]
    public void Drain_DatagramLargerThanTheReadShare_IsDeliveredWholeViaTheEscape()
    {
        // Arrange — at the Raw default the arena is one maximum datagram (65 507) shared across
        // Capacity share-holders, so a Capacity of 4 gives a read share of 16 376. A datagram larger
        // than that share is still delivered whole: the escape ADR 0014 requires.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.BindLoopbackLink();
        byte[] payload = new byte[20_000];
        Random.Shared.NextBytes(payload);
        harness.SendFromCounterparty(localEndPoint, payload);

        // Act
        harness.Reactor.Pump();

        // Assert
        harness.DrainToList().Should().ContainSingle()
            .Which.Payload.Should().Equal(payload);
    }

    [Fact]
    public void Drain_SeveralLinksEachSentTwoOversizedDatagrams_YieldsExactlyOnePerLinkPerPump()
    {
        // Arrange — the composed default behaviour issue #1 asks to have pinned, because it is what a
        // consumer actually observes at the defaults. Note what does *not* enforce it: the read share
        // is never reached, because a ready Link is read exactly once per Pump. One datagram per Link
        // per Pump is a property of the single read, not of the share cap — so this test would keep
        // passing if the cap were removed. The cap is observable only where a Link is read more than
        // once per Pump.
        using RawReactorHarness harness = new(capacity: 4);
        (LinkId firstLink, IPEndPoint firstEndPoint) = harness.BindLoopbackLink();
        (LinkId secondLink, IPEndPoint secondEndPoint) = harness.BindLoopbackLink();
        byte[] payload = new byte[20_000]; // exceeds the 16 376-byte read share
        foreach (IPEndPoint endPoint in new[] { firstEndPoint, secondEndPoint })
        {
            harness.SendFromCounterparty(endPoint, payload);
            harness.SendFromCounterparty(endPoint, payload);
        }

        // Act
        harness.Reactor.Pump();

        // Assert
        harness.DrainToList().Select(drained => drained.Link)
            .Should().BeEquivalentTo(new[] { firstLink, secondLink });
    }

    [Fact]
    public void Drain_ArenaWithNoRoomForASecondLinksDatagram_DeliversOnlyTheFirst()
    {
        // Arrange
        using RawReactorHarness harness = ArenaTooSmallForBothLinks();

        // Act
        harness.Reactor.Pump();

        // Assert
        harness.DrainToList().Should().ContainSingle();
    }

    [Fact]
    public void Pump_ArenaWithNoRoomForASecondLinksDatagram_ReportsArenaExhaustedWithTheArithmetic()
    {
        // Arrange
        using RawReactorHarness harness = ArenaTooSmallForBothLinks();

        // Act
        harness.Reactor.Pump();

        // Assert
        DiagnosticRecord record = harness.Sink.Received.Should()
            .ContainSingle(record => record.Id == DiagnosticId.ArenaExhausted).Subject;
        record.Value0.Should().Be(1000, "the refused request's size is reported");
        record.Value1.Should().Be(200, "so is the arena's free space at the moment it was refused");
    }

    /// <summary>
    /// Two Links both ready in one Frame, sharing an arena that cannot hold both their datagrams.
    /// ArenaSize is left unset, so it defaults to this profile's floor: one maximum-sized read, which
    /// for Raw is the scratch buffer. That gives an arena of 1 200 bytes; the first Link commits
    /// 1 000 and the second cannot fit in the 200 that remain.
    /// </summary>
    private static RawReactorHarness ArenaTooSmallForBothLinks()
    {
        RawReactorHarness harness = new(capacity: 2, scratchBufferSize: 1200);
        (_, IPEndPoint firstEndPoint) = harness.BindLoopbackLink();
        (_, IPEndPoint secondEndPoint) = harness.BindLoopbackLink();
        byte[] payload = new byte[1000];
        harness.SendFromCounterparty(firstEndPoint, payload);
        harness.SendFromCounterparty(secondEndPoint, payload);
        return harness;
    }

    [Fact]
    public void Drain_ThePumpAfterArenaExhaustion_DeliversAgainOnTheWipedArena()
    {
        // Arrange — the arena-level refusal must not corrupt later Frames: every Pump wipes the arena,
        // so the next Frame starts with the whole of it free again.
        using RawReactorHarness harness = new(capacity: 2, scratchBufferSize: 1200, arenaSize: 1200);
        (_, IPEndPoint firstEndPoint) = harness.BindLoopbackLink();
        (_, IPEndPoint secondEndPoint) = harness.BindLoopbackLink();
        byte[] payload = new byte[1000];
        harness.SendFromCounterparty(firstEndPoint, payload);
        harness.SendFromCounterparty(secondEndPoint, payload);
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle();

        // Act
        harness.SendFromCounterparty(firstEndPoint, payload);
        harness.Reactor.Pump();

        // Assert
        harness.DrainToList().Should().ContainSingle()
            .Which.Payload.Should().Equal(payload);
    }

    [Fact]
    public void Drain_AfterTheNextPump_YieldsOnlyTheCurrentFramesEvents()
    {
        // Arrange — a Frame is the span from one Pump to the next; the previous Frame's events are
        // gone, whether or not they were drained. (That the previous Frame's *payload bytes* are also
        // invalidated is enforced by the compiler, not by a test: RawEvent is a ref struct whose
        // Payload aliases the arena, so it cannot be held across a Pump at all.)
        using RawReactorHarness harness = new();
        (_, IPEndPoint localEndPoint) = harness.BindLoopbackLink();
        harness.SendFromCounterparty(localEndPoint, [1, 2, 3]);
        harness.Reactor.Pump();

        // Act — a second Pump with nothing waiting.
        harness.Reactor.Pump();

        // Assert
        harness.DrainToList().Should().BeEmpty();
    }
}
