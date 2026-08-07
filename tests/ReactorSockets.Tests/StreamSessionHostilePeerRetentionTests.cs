using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.5.6 (issue #88): confirms a Peer that never completes a clean handshake still has its
/// Slot — and, per #84's wiring in <c>ClosePeer</c>, its retention region — reclaimed, exactly as a
/// Peer that does complete the handshake and later disconnects would. <see cref="HostilePeer"/>
/// plays the hostile counterparty, built entirely from the public Raw surface, following the house
/// pattern <see cref="StreamSessionHandshakeGuardTests"/> and
/// <see cref="StreamSessionMalformedFramingTests"/> already established. Per tests/CLAUDE.md, L0 is
/// untested-through — so "the retention region was reclaimed" is observed the only way the public
/// surface allows: a subsequent legitimate connection can still use the reactor at full capacity,
/// never by reaching into <c>ShareHolderTable</c> or <c>RetentionArena</c> directly.
/// </summary>
public class StreamSessionHostilePeerRetentionTests
{
    private const int RecordSize = 41; // 1 type byte + 40-byte Hello payload; a Refusal is padded to it.
    private const byte HelloType = 1;
    private const uint ProtocolVersion = 1;

    private static byte[] BuildHello(uint version = ProtocolVersion, uint maxMessageSize = 4096, ulong sendIdleMs = 1000, ulong lingerMs = 1000)
    {
        byte[] record = new byte[RecordSize];
        record[0] = HelloType;
        int offset = 1;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), version);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), sendIdleMs);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), lingerMs);
        return record;
    }

    /// <summary>
    /// Connects a fresh legitimate dialler to <paramref name="accepter"/> and asserts it establishes
    /// a Session — the only public-surface proof available that a Slot (and its retention region)
    /// a hostile scenario occupied earlier was actually reclaimed rather than permanently lost.
    /// </summary>
    private static void AssertLegitimateConnectionStillSucceeds(StreamSessionReactorHarness accepter)
    {
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => false, maxRounds: 10);

        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    // =========================================================================================
    // Opens and sends nothing
    // =========================================================================================

    [Fact]
    public void Pump_AConnectionThatOpensAndSendsNothing_NeverThrowsAndReclaimsItsSlotAndRetentionRegion()
    {
        // Arrange — a sole Slot, so the legitimate connection below can only succeed if the hostile
        // connection's Slot (and the retention region keyed to it) was actually released.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();

        // Act
        Action act = () =>
        {
            LinkId link = hostile.ConnectAndSend(accepter.Endpoint, payload: []);
            accepter.PumpAndDrainAll(3);
            hostile.Close(link);
            accepter.PumpAndDrainAll(3);
        };

        // Assert
        act.Should().NotThrow();
        AssertLegitimateConnectionStillSucceeds(accepter);
    }

    // =========================================================================================
    // Truncated handshake
    // =========================================================================================

    [Fact]
    public void Pump_ATruncatedHandshake_NeverThrowsAndReclaimsItsSlotAndRetentionRegion()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();
        byte[] partial = BuildHello().AsSpan(0, 10).ToArray();

        // Act
        Action act = () =>
        {
            LinkId link = hostile.ConnectAndSend(accepter.Endpoint, partial);
            accepter.PumpAndDrainAll(3);
            hostile.Close(link);
            accepter.PumpAndDrainAll(3);
        };

        // Assert
        act.Should().NotThrow();
        AssertLegitimateConnectionStillSucceeds(accepter);
    }

    // =========================================================================================
    // Garbage-version handshake
    // =========================================================================================

    [Fact]
    public void Pump_AHandshakeWithAGarbageVersion_NeverThrowsAndReclaimsItsSlotAndRetentionRegionAfterRefusal()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();
        byte[] garbage = BuildHello(version: 0xDEAD_BEEF);

        // Act — the reactor refuses and closes this connection on its own; no explicit Close needed.
        Action act = () =>
        {
            hostile.ConnectAndSend(accepter.Endpoint, garbage);
            accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused), maxPumps: 10);
            accepter.PumpAndDrainAll(3);
        };

        // Assert
        act.Should().NotThrow();
        AssertLegitimateConnectionStillSucceeds(accepter);
    }

    // =========================================================================================
    // All three, back to back, on the very same Slot
    // =========================================================================================

    [Fact]
    public void Pump_AllThreeHostileScenariosInSequenceOnTheSoleSlot_NeverLeaksItAndALegitimateConnectionStillSucceeds()
    {
        // Arrange — capacity of one forces every hostile attempt below, and the legitimate connection
        // that follows, through the same Slot and the same retention region. Any one of the three
        // failing to release it would starve every attempt that comes after.
        using StreamSessionReactorHarness accepter = new(capacity: 1);

        // Act & Assert — opens and sends nothing.
        using (HostilePeer opensAndSendsNothing = new())
        {
            LinkId link = opensAndSendsNothing.ConnectAndSend(accepter.Endpoint, payload: []);
            accepter.PumpAndDrainAll(3);
            opensAndSendsNothing.Close(link);
            accepter.PumpAndDrainAll(3);
        }

        // Act & Assert — a truncated handshake reuses the same, now-reclaimed Slot.
        using (HostilePeer truncated = new())
        {
            LinkId link = truncated.ConnectAndSend(accepter.Endpoint, BuildHello().AsSpan(0, 10).ToArray());
            accepter.PumpAndDrainAll(3);
            truncated.Close(link);
            accepter.PumpAndDrainAll(3);
        }

        // Act & Assert — a garbage-version handshake reuses it a third time.
        using (HostilePeer garbageVersion = new())
        {
            garbageVersion.ConnectAndSend(accepter.Endpoint, BuildHello(version: 0xDEAD_BEEF));
            accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused), maxPumps: 10);
            accepter.PumpAndDrainAll(3);
        }

        // Assert — the Slot survived all three hostile occupants; a legitimate connection reuses it a
        // fourth time and establishes normally.
        AssertLegitimateConnectionStillSucceeds(accepter);
    }
}
