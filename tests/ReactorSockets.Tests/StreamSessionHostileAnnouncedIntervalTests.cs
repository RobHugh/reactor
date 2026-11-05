using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #422 on the stream profile: a Hello's announced send-idle interval and Linger are a
/// <c>ulong</c> off the wire, and an out-of-range one used to throw out of
/// <see cref="StreamSessionReactor.Pump"/>. Driven from outside the L2 seam (tests/CLAUDE.md) by a
/// <see cref="HostilePeer"/> writing a hand-built record.
/// </summary>
public class StreamSessionHostileAnnouncedIntervalTests
{
    private const ulong LegalInterval = 60_000;

    /// <summary>The largest legal announcement: the largest count of milliseconds 32 bits can hold.</summary>
    private const ulong MaximumLegalInterval = uint.MaxValue;

    /// <summary>Inside the window that used to survive the handshake and then overflow every Pump.</summary>
    private const ulong SurvivesConversionOverflowsDeadline = 600_000_000_000_000;

    /// <summary>Each illegal value in each announced field, with the other field legal.</summary>
    public static TheoryData<ulong, ulong> IllegalAnnouncements => new()
    {
        { 0, LegalInterval },
        { LegalInterval, 0 },
        { MaximumLegalInterval + 1, LegalInterval },
        { LegalInterval, MaximumLegalInterval + 1 },
        { SurvivesConversionOverflowsDeadline, LegalInterval },
        { LegalInterval, SurvivesConversionOverflowsDeadline },
        { ulong.MaxValue, LegalInterval },
        { LegalInterval, ulong.MaxValue },
    };

    [Theory]
    [MemberData(nameof(IllegalAnnouncements))]
    public void Pump_HelloAnnouncingAnIllegalInterval_ClosesTheConnectionAndKeepsPumping(
        ulong sendIdleIntervalMilliseconds, ulong lingerMilliseconds)
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using HostilePeer hostile = new();
        byte[] hello = StreamSessionTestRecords.BuildHello(sendIdleMs: sendIdleIntervalMilliseconds, lingerMs: lingerMilliseconds);

        // Act
        hostile.ConnectAndSend(accepter.Endpoint, hello);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        Action pump = () =>
        {
            events = accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));
            accepter.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerClosed && evt.Reason == DisconnectReason.TransportFailed);
    }

    [Fact]
    public void Pump_AfterClosingAnIllegalInterval_StillAdmitsAGenuineDial()
    {
        // Arrange — one Slot, so a close that wrongly kept the Slot would refuse the genuine dial.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();
        hostile.ConnectAndSend(
            accepter.Endpoint,
            StreamSessionTestRecords.BuildHello(sendIdleMs: SurvivesConversionOverflowsDeadline, lingerMs: LegalInterval));
        accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_HelloAnnouncingTheMaximumAgainstTheLargestHeartbeatCount_EstablishesAndKeepsPumping()
    {
        // Arrange — the largest legal announcement meets the largest count a consumer can configure, so
        // the receive-idle deadline the two imply is far outside what a TimeSpan can represent.
        using StreamSessionReactorHarness accepter = new(missedHeartbeats: int.MaxValue);
        using HostilePeer counterparty = new();
        byte[] hello = StreamSessionTestRecords.BuildHello(sendIdleMs: MaximumLegalInterval, lingerMs: MaximumLegalInterval);

        // Act
        counterparty.ConnectAndSend(accepter.Endpoint, hello);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        Action pump = () =>
        {
            events = accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));
            accepter.TimeProvider.Advance(TimeSpan.FromDays(1));
            accepter.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
        accepter.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerInterrupted);
    }
}
