using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A stream Reactor holds at most <see cref="ReactorOptions.Capacity"/> accepted connections waiting for
/// their first handshake record (issue #482). Past that it accepts nothing more that Pump, so further
/// connections wait in the OS backlog, counted as Admission Deferrals and logged once per Pump.
/// </summary>
public class StreamSessionPendingAdmissionLimitTests
{
    [Fact]
    public void Pump_SilentConnectionsHoldingEveryPlace_LeaveTheNextConnectionUnaccepted()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        using StreamSessionReactorHarness.RawStreamPeer first = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer second = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer third = new(accepter.Endpoint);
        third.Send(StreamSessionTestRecords.BuildHello());

        // Act
        accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

        // Assert
        third.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, 100).Should().BeFalse();
    }

    [Fact]
    public void Pump_HeldConnectionsReachingTheirDeadline_AdmitTheConnectionLeftInTheBacklog()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        using StreamSessionReactorHarness.RawStreamPeer first = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer second = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer third = new(accepter.Endpoint);
        third.Send(StreamSessionTestRecords.BuildHello());
        accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

        // Act
        accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
        var drained = accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerConnected));

        // Assert
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_AFrameThatDefersPastTheLimit_EmitsExactlyOneRecordCarryingTheLimit()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        using StreamSessionReactorHarness.RawStreamPeer first = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer second = accepter.HoldSilentConnection();
        List<StreamSessionReactorHarness.RawStreamPeer> queued = [];
        try
        {
            for (int i = 0; i < 3; i++)
            {
                StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
                queued.Add(peer);
                peer.Send(StreamSessionTestRecords.BuildHello());
            }

            Thread.Sleep(20);
            int before = accepter.Sink.Received.Count(r => r.Id == DiagnosticId.PendingAdmissionLimitReached);

            // Act
            accepter.Reactor.Pump();

            // Assert
            List<DiagnosticRecord> records = [.. accepter.Sink.Received.Where(r => r.Id == DiagnosticId.PendingAdmissionLimitReached).Skip(before)];
            records.Should().ContainSingle();
            records[0].Value0.Should().Be(2);
            records[0].Value1.Should().Be(1);
            records[0].Level.Should().Be(DiagnosticLevel.Warn);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.RawStreamPeer peer in queued)
            {
                peer.Dispose();
            }
        }
    }

    [Fact]
    public void Pump_AFrameThatDefersPastTheLimit_CountsOneAdmissionDeferral()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2, admissionCap: 8);
        using StreamSessionReactorHarness.RawStreamPeer first = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer second = accepter.HoldSilentConnection();
        List<StreamSessionReactorHarness.RawStreamPeer> queued = [];
        try
        {
            for (int i = 0; i < 3; i++)
            {
                StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
                queued.Add(peer);
                peer.Send(StreamSessionTestRecords.BuildHello());
            }

            Thread.Sleep(20);
            ulong before = accepter.Reactor.Counters.AdmissionDeferrals;

            // Act
            accepter.Reactor.Pump();

            // Assert
            (accepter.Reactor.Counters.AdmissionDeferrals - before).Should().Be(1);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.RawStreamPeer peer in queued)
            {
                peer.Dispose();
            }
        }
    }

    [Fact]
    public void AttemptFreshConnection_HeldConnectionsBelowCapacity_IsAdmitted()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 3);
        using StreamSessionReactorHarness.RawStreamPeer first = accepter.HoldSilentConnection();
        using StreamSessionReactorHarness.RawStreamPeer second = accepter.HoldSilentConnection();

        // Act
        using var attempt = accepter.AttemptFreshConnection();

        // Assert
        attempt.Admitted.Should().BeTrue();
    }
}
