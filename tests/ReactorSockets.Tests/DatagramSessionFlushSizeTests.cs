using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A data Datagram is at most the 20-byte Datagram header, an 11-byte Fragment record header and
/// <see cref="DatagramSessionOptions.FragmentSize"/> payload bytes, whatever is queued, and one Pump
/// writes at most <c>65 507 / (FragmentSize + 31)</c> data Datagrams per Peer (issue #485).
/// </summary>
public class DatagramSessionFlushSizeTests
{
    private const int FragmentSize = 1200;

    private const int DatagramHeaderSize = 20;

    private const int FragmentRecordSize = 11;

    private const int DatagramLimit = DatagramHeaderSize + FragmentRecordSize + FragmentSize;

    [Fact]
    public void Pump_AMixedQueueWiderThanOneDatagram_WritesNoDatagramAboveFragmentSizePlusHeaders()
    {
        // Arrange
        const int unreliableCount = 20;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, capacity: 1, maxMessageSize: 3000, retentionBytes: 6000, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 1, maxMessageSize: 3000, retentionBytes: 6000, fragmentSize: FragmentSize, arenaSize: 16_384);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
        int capturesBefore = tap.Captures.Count;

        for (int i = 0; i < unreliableCount; i++)
        {
            dialler.Reactor.Send(peer, DeliveryMode.Unreliable, StreamOf(100, (byte)i)).Should().Be(SendOutcome.Sent);
        }

        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, StreamOf(3000, 0xA0)).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(peer, DeliveryMode.ReliableUnordered, StreamOf(50, 0xB0)).Should().Be(SendOutcome.Sent);

        // Act
        dialler.Reactor.Pump();
        List<byte[]> delivered = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (delivered.Count < unreliableCount + 2 && !deadline.Expired)
        {
            tap.Pump();
            accepter.Reactor.Pump();
            delivered.AddRange(accepter.DrainPayloads());
            PumpDeadline.Yield();
        }

        List<byte[]> written = tap.Captures
            .Skip(capturesBefore)
            .Where(c => c.Direction == TapDirection.DiallerToAccepter)
            .Select(c => c.Payload)
            .ToList();

        // Assert
        delivered.Should().HaveCount(unreliableCount + 2, "every queued Message crosses, however many Datagrams it takes");
        written.Should().HaveCountGreaterThan(1, "the queue is wider than one Datagram of this size");
        written.Should().AllSatisfy(d => d.Length.Should().BeLessThanOrEqualTo(DatagramLimit));
    }

    [Fact]
    public void Pump_AQueueOfSixtyFullRecords_WritesFiftyThreeDatagramsThenSevenThenNone()
    {
        // Arrange: each 1200-byte record needs its own 1231-byte Datagram, and 65 507 / 1231 = 53.
        const int records = 60;
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, maxMessageSize: FragmentSize, retentionBytes: FragmentSize * records, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness receiver = new(fragmentSize: FragmentSize);
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        for (int i = 0; i < records; i++)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, StreamOf(FragmentSize, (byte)i)).Should().Be(SendOutcome.Sent);
        }

        // Act
        ulong beforeFirst = sender.Reactor.Counters.DatagramsSent;
        sender.Reactor.Pump();
        ulong afterFirst = sender.Reactor.Counters.DatagramsSent;
        sender.Reactor.Pump();
        ulong afterSecond = sender.Reactor.Counters.DatagramsSent;
        sender.Reactor.Pump();
        ulong afterThird = sender.Reactor.Counters.DatagramsSent;

        // Assert
        (afterFirst - beforeFirst).Should().Be(53UL, "one Pump sends at most 65 507 / (FragmentSize + 31) data Datagrams per Peer");
        (afterSecond - afterFirst).Should().Be(7UL, "the seven records that did not fit leave on the next Pump");
        (afterThird - afterSecond).Should().Be(0UL, "the queue is empty");
    }

    private static byte[] StreamOf(int length, byte seed) =>
        Enumerable.Range(0, length).Select(i => (byte)(seed + i)).ToArray();
}
