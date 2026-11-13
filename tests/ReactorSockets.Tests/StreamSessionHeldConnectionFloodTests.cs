using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #482's failure scenario: a remote that opens many connections and sends nothing. The deadline, the
/// <c>Capacity</c> limit and the per-address limit together keep the held set bounded, keep another address
/// admissible, and close every silent connection in bounded time.
/// </summary>
public class StreamSessionHeldConnectionFloodTests
{
    [Fact]
    public void AttemptFreshConnection_OneAddressFloodsSilentConnections_AdmitsAnotherAddress()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 3, defaultMaxPeersPerAddress: true);
        List<StreamSessionReactorHarness.RawStreamPeer> peers = OpenSilent(accepter, 10);
        try
        {
            accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

            // Act
            using StreamSessionReactorHarness.InboundAttemptResult other =
                accepter.AttemptFreshConnection(localAddress: IPAddress.Parse("127.0.0.2"));

            // Assert
            other.Admitted.Should().BeTrue();
        }
        finally
        {
            foreach (StreamSessionReactorHarness.RawStreamPeer p in peers)
            {
                p.Dispose();
            }
        }
    }

    [Fact]
    public void Pump_SilentFloodPastCapacity_LeavesTheExcessUnacceptedUntilTheDeadline()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.RawStreamPeer> peers = OpenSilent(accepter, 20);
        try
        {
            // Act
            accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

            // Assert
            ClosedCount(peers).Should().Be(0);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.RawStreamPeer p in peers)
            {
                p.Dispose();
            }
        }
    }

    [Fact]
    public void Pump_SilentFloodPastCapacity_ClosesExactlyCapacityConnectionsPerDeadline()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.RawStreamPeer> peers = OpenSilent(accepter, 20);
        try
        {
            accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

            // Act
            accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
            accepter.PumpUntil(() => ClosedCount(peers) >= 2);
            accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

            // Assert
            ClosedCount(peers).Should().Be(2);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.RawStreamPeer p in peers)
            {
                p.Dispose();
            }
        }
    }

    [Fact]
    public void Pump_SilentFloodPastCapacity_ClosesEveryConnectionWithinOneDeadlinePerCapacityBatch()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.RawStreamPeer> peers = OpenSilent(accepter, 20);
        try
        {
            accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(300));

            // Act
            for (int round = 1; round <= 10; round++)
            {
                int target = 2 * round;
                accepter.TimeProvider.Advance(accepter.PendingAdmissionDeadline);
                accepter.PumpUntil(() => ClosedCount(peers) >= target);
                // The Pump after a close accepts the next batch from the backlog.
                accepter.PumpUntil(() => false, TimeSpan.FromMilliseconds(100));
            }

            // Assert
            ClosedCount(peers).Should().Be(20);
            accepter.Sink.Received.Count(r => r.Id == DiagnosticId.PendingAdmissionExpired).Should().Be(20);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.RawStreamPeer p in peers)
            {
                p.Dispose();
            }
        }
    }

    /// <summary>Opens <paramref name="count"/> connections from loopback that never send.</summary>
    private static List<StreamSessionReactorHarness.RawStreamPeer> OpenSilent(StreamSessionReactorHarness accepter, int count)
    {
        List<StreamSessionReactorHarness.RawStreamPeer> peers = [];
        for (int i = 0; i < count; i++)
        {
            peers.Add(new StreamSessionReactorHarness.RawStreamPeer(accepter.Endpoint));
        }

        return peers;
    }

    /// <summary>How many of <paramref name="peers"/> the Reactor has closed.</summary>
    private static int ClosedCount(List<StreamSessionReactorHarness.RawStreamPeer> peers) =>
        peers.Count(p => p.HasPendingReadOrClose);
}
