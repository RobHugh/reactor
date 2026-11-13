using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// An accepted stream connection that has not sent its whole first handshake record within
/// <see cref="StreamSessionOptions.SendIdleInterval"/> × <see cref="StreamSessionOptions.MissedHeartbeats"/>
/// of being accepted is closed with nothing sent (issue #482). The deadline is fixed at accept, so bytes
/// that arrive do not extend it, and a record already in the socket buffer when the deadline passes is
/// still read.
/// </summary>
public class StreamSessionPendingAdmissionDeadlineTests
{
    [Fact]
    public void Pump_SilentConnectionPastItsDeadline_IsClosed()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();

        // Act
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
        accepter.PumpUntil(() => peer.HasPendingReadOrClose);

        // Assert
        peer.HasPendingReadOrClose.Should().BeTrue();
        peer.ExpectGracefulClose();
    }

    [Fact]
    public void Pump_SilentConnectionOneTickBeforeItsDeadline_StaysOpen()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();

        // Act
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline - TimeSpan.FromTicks(1));
        accepter.PumpUntil(() => peer.HasPendingReadOrClose, TimeSpan.FromMilliseconds(200));

        // Assert
        peer.HasPendingReadOrClose.Should().BeFalse();
    }

    [Fact]
    public void Pump_ConnectionSendingPartOfItsRecord_IsClosedAtTheDeadlineFixedAtAccept()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();
        peer.Send([0x01]);
        accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(100));
        accepter.TimeProvider.Advance(TimeSpan.FromSeconds(2));
        peer.Send([0x01]);
        accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(100));

        // Act
        accepter.TimeProvider.Advance(TimeSpan.FromSeconds(1));
        accepter.PumpUntil(() => peer.HasPendingReadOrClose);

        // Assert
        peer.HasPendingReadOrClose.Should().BeTrue();
    }

    [Fact]
    public void Pump_HelloArrivingBeforeTheDeadline_IsAdmitted()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline - TimeSpan.FromTicks(1));
        accepter.Reactor.Pump();
        accepter.DrainToList();

        // Act
        peer.Send(StreamSessionTestRecords.BuildHello());
        var drained = accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerConnected));

        // Assert
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_HelloAlreadyWaitingWhenTheDeadlinePasses_IsAdmitted()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();
        peer.Send(StreamSessionTestRecords.BuildHello());
        Thread.Sleep(50);

        // Act
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
        var drained = accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerConnected));

        // Assert
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_SilentConnectionPastItsDeadline_LogsPendingAdmissionExpiredNamingTheSource()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();

        // Act
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
        accepter.Reactor.Pump();

        // Assert
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.PendingAdmissionExpired).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.Should().Be(Source.FromIPEndPoint(peer.LocalEndPoint));
        record.Value0.Should().Be(0);
        record.Value1.Should().Be(3000);
    }

    [Fact]
    public void Pump_SilentConnectionPastItsDeadline_RaisesNoSessionEvent()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, missedHeartbeats: 3);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();
        accepter.DrainToList();

        // Act
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
        var drained = accepter.PumpAndDrainAll(5);

        // Assert
        drained.Should().BeEmpty();
    }

    [Fact]
    public void Pump_LargestLegalIntervalAndMissedHeartbeatCount_DoesNotThrow()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(
            capacity: 2,
            sendIdleInterval: TimeSpan.FromMilliseconds(uint.MaxValue),
            missedHeartbeats: int.MaxValue);
        using StreamSessionReactorHarness.RawStreamPeer peer = accepter.HoldSilentConnection();
        accepter.TimeProvider.Advance(TimeSpan.FromDays(365));

        // Act
        Action pump = () => accepter.Reactor.Pump();

        // Assert
        pump.Should().NotThrow();
    }
}
