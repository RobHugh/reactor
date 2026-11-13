using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// At the largest <see cref="DatagramSessionOptions.FragmentSize"/> a reliable Message cut into full
/// Fragments is sent and delivered, and so is the Message queued behind it (issue #486). A full
/// Fragment record is 20 + 11 + 65 476 = 65 507 bytes on the wire, one maximum UDP payload exactly. A
/// larger accepted fragment size leaves a Fragment that fits no Datagram, which stalls every record
/// queued for that Peer.
/// </summary>
public class DatagramSessionFragmentSizeAtMaximumTests
{
    /// <summary>
    /// The largest fragment size the options accept: the 65 507-byte UDP payload less the 20-byte
    /// Datagram header and the 11-byte Fragment record header.
    /// </summary>
    private const int FragmentSize = 65_476;

    /// <summary>
    /// Two full Fragments, so both records use the 11-byte Fragment shape, the largest record header.
    /// A Message of one Fragment would use the 7-byte shape and leave 4 bytes spare.
    /// </summary>
    private const int LargeMessageSize = 2 * FragmentSize;

    /// <summary>Room in one reliable Space for the two Fragments and the small Message behind them.</summary>
    private const int RetentionBytes = 3 * FragmentSize;

    /// <summary>Wide enough for the two Fragments and the small Message's sequence.</summary>
    private const int ReceiveWindowFragments = 4;

    /// <summary>Room for the assembled large Message and the small one in the same Frame.</summary>
    private const int ArenaSize = 3 * FragmentSize;

    private static readonly byte[] _smallMessage = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    [Theory]
    [InlineData(DeliveryMode.ReliableOrdered)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    public void Send_TwoFullFragmentsAtTheLargestFragmentSize_DeliversThemAndTheMessageBehindThem(DeliveryMode mode)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            capacity: 1,
            bindLoopback: false,
            maxMessageSize: LargeMessageSize,
            retentionBytes: RetentionBytes,
            fragmentSize: FragmentSize,
            receiveWindowFragments: ReceiveWindowFragments,
            arenaSize: ArenaSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 1,
            maxMessageSize: LargeMessageSize,
            retentionBytes: RetentionBytes,
            fragmentSize: FragmentSize,
            receiveWindowFragments: ReceiveWindowFragments,
            arenaSize: ArenaSize,
            timeProvider: dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        byte[] large = BuildLargeMessage();

        // Act
        dialler.Reactor.Send(peer, mode, large).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(peer, mode, _smallMessage).Should().Be(SendOutcome.Sent);
        List<byte[]> delivered = PumpUntilDelivered(dialler, accepter, count: 2);

        // Assert
        delivered.Should().HaveCount(2, "a full Fragment at the largest fragment size fills one Datagram and is sent");
        delivered.Should().ContainEquivalentOf(large);
        delivered.Should().ContainEquivalentOf(_smallMessage, "the Message queued behind the large one is not stalled by it");
    }

    private static byte[] BuildLargeMessage()
    {
        byte[] message = new byte[LargeMessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        return message;
    }

    /// <summary>
    /// Pumps both ends until the accepter has delivered <paramref name="count"/> Messages or the
    /// deadline passes, and returns what it delivered. It returns silently at the deadline, so assert
    /// the count on the result.
    /// </summary>
    private static List<byte[]> PumpUntilDelivered(
        DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, int count)
    {
        List<byte[]> delivered = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (delivered.Count < count && !deadline.Expired)
        {
            dialler.Reactor.Pump();
            dialler.DrainToList();
            accepter.Reactor.Pump();
            delivered.AddRange(accepter.DrainPayloads());
            PumpDeadline.Yield();
        }

        return delivered;
    }
}
