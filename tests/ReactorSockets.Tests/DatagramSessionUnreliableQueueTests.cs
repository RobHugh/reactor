using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// An unreliable Send is charged its length plus the 7-byte record header against the Peer's
/// <see cref="DatagramSessionOptions.UnreliableQueueBytes"/>, shared by both unreliable modes, and one
/// that would exceed it returns <see cref="SendOutcome.SendQueueFull"/> (issue #485).
/// </summary>
public class DatagramSessionUnreliableQueueTests
{
    private const int RecordHeaderBytes = 7;

    [Fact]
    public void Send_UnreliablePastTheQueueBudget_ReturnsSendQueueFull()
    {
        // Arrange: ten 1000-byte Messages charge exactly 10 × 1007 = 10 070 bytes.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: 10 * (1000 + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        List<SendOutcome> accepted = [];
        for (int i = 0; i < 10; i++)
        {
            accepted.Add(sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]));
        }

        // Act
        SendOutcome eleventh = sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]);

        // Assert
        accepted.Should().AllSatisfy(o => o.Should().Be(SendOutcome.Sent));
        eleventh.Should().Be(SendOutcome.SendQueueFull);
    }

    [Fact]
    public void Send_AfterAPumpFlushedAFullQueue_IsAcceptedAgain()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: 10 * (1000 + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        while (sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]) == SendOutcome.Sent)
        {
        }

        // Act: ten 1007-byte records leave as ten Datagrams, within one Pump's count of 53.
        sender.Reactor.Pump();
        SendOutcome afterPump = sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]);

        // Assert
        afterPump.Should().Be(SendOutcome.Sent, "the Pump flushed the queue and released its budget");
    }

    [Fact]
    public void Send_UnreliableAndUnreliableSequenced_ShareOneBudget()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: 2 * (1000 + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, new byte[1000]).Should().Be(SendOutcome.Sent);

        // Act
        SendOutcome third = sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, new byte[1000]);
        SendOutcome fourth = sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]);

        // Assert
        third.Should().Be(SendOutcome.SendQueueFull);
        fourth.Should().Be(SendOutcome.SendQueueFull);
    }

    [Fact]
    public void Send_ReliableWithTheUnreliableQueueFull_IsNotCharged()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: 1200 + RecordHeaderBytes);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1200]).Should().Be(SendOutcome.Sent);

        // Act
        SendOutcome ordered = sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[1000]);
        SendOutcome unordered = sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, new byte[1000]);

        // Assert
        ordered.Should().Be(SendOutcome.Sent);
        unordered.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Send_WithTheDefaultBudget_AcceptsRetentionSlotsFullRecords()
    {
        // Arrange: retention 4096 at fragment size 1200 is 3 slots, so the default is 3 × 1207 = 3621.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        List<SendOutcome> accepted = [];
        for (int i = 0; i < 3; i++)
        {
            accepted.Add(sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]));
        }

        // Act: 4 × 1007 = 4028 is over 3621.
        SendOutcome fourth = sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]);

        // Assert
        sender.Options.ResolvedUnreliableQueueBytes.Should().Be(3 * (1200 + RecordHeaderBytes));
        accepted.Should().AllSatisfy(o => o.Should().Be(SendOutcome.Sent));
        fourth.Should().Be(SendOutcome.SendQueueFull);
    }

    [Theory]
    [InlineData(2014, SendOutcome.Sent)]
    [InlineData(2013, SendOutcome.SendQueueFull)]
    public void Send_ASecondMessageAtTheBudgetsEdge_IsChargedItsLengthPlusTheRecordHeader(int budget, SendOutcome expected)
    {
        // Arrange: two 1000-byte Messages charge exactly 2014 bytes.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: budget);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]).Should().Be(SendOutcome.Sent);

        // Act
        SendOutcome second = sender.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1000]);

        // Assert
        second.Should().Be(expected);
    }

    [Fact]
    public void Send_EmptyMessages_AreChargedTheRecordHeaderEach()
    {
        // Arrange: 1207 / 7 = 172 empty records fit, and the 173rd does not.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: 1200 + RecordHeaderBytes);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        List<SendOutcome> accepted = [];
        for (int i = 0; i < 172; i++)
        {
            accepted.Add(sender.Reactor.Send(peer, DeliveryMode.Unreliable, []));
        }

        // Act
        SendOutcome next = sender.Reactor.Send(peer, DeliveryMode.Unreliable, []);

        // Assert
        accepted.Should().AllSatisfy(o => o.Should().Be(SendOutcome.Sent));
        next.Should().Be(SendOutcome.SendQueueFull);
    }

    [Fact]
    public void Pump_AQueuePartlyFlushed_DeliversTheRestAndLaterSendsInOrderWithTheirOwnBytes()
    {
        // Arrange: at fragment size 32 738 a data Datagram is at most 32 769 bytes and one Pump writes
        // 65 507 / 32 769 = 1 of them, so each Pump sends exactly one of these Messages.
        const int size = 32_738;
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, maxMessageSize: size, fragmentSize: size, retentionBytes: size,
            unreliableQueueBytes: 3 * (size + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiver = new(
            maxMessageSize: size, fragmentSize: size, retentionBytes: size, arenaSize: 4 * size);
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        byte[][] messages = [StreamOf(size, 1), StreamOf(size, 2), StreamOf(size, 3), StreamOf(size, 4)];
        for (int i = 0; i < 3; i++)
        {
            sender.Reactor.Send(peer, DeliveryMode.Unreliable, messages[i]).Should().Be(SendOutcome.Sent);
        }

        // Act: the first Pump sends Message 1 and compacts 2 and 3 to the front, making room for 4.
        List<byte[]> delivered = [];
        sender.Reactor.Pump();
        SendOutcome fourth = sender.Reactor.Send(peer, DeliveryMode.Unreliable, messages[3]);
        PumpDeadline deadline = PumpDeadline.Start();
        while (delivered.Count < 4 && !deadline.Expired)
        {
            receiver.Reactor.Pump();
            delivered.AddRange(receiver.DrainPayloads());
            sender.Reactor.Pump();
            PumpDeadline.Yield();
        }

        // Assert
        fourth.Should().Be(SendOutcome.Sent, "the first Pump released Message 1's charge");
        delivered.Should().HaveCount(4);
        delivered.Should().SatisfyRespectively(
            m => m.Should().Equal(messages[0]),
            m => m.Should().Equal(messages[1]),
            m => m.Should().Equal(messages[2]),
            m => m.Should().Equal(messages[3]));
    }

    [Fact]
    public void Pump_AQueueWithAReliableRecordBetweenUnreliableOnes_KeepsEachUnreliableMessagesBytes()
    {
        // Arrange: each Pump sends one record, so the reliable record leaves on its own between the two
        // unreliable ones, and the compaction after each Pump must skip it.
        const int size = 32_738;
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, maxMessageSize: size, fragmentSize: size, retentionBytes: size,
            unreliableQueueBytes: 2 * (size + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiver = new(
            maxMessageSize: size, fragmentSize: size, retentionBytes: size, arenaSize: 4 * size);
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        byte[] first = StreamOf(size, 1);
        byte[] reliable = StreamOf(10, 0xC0);
        byte[] second = StreamOf(size, 2);
        byte[] third = StreamOf(size, 3);
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, first).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, reliable).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, second).Should().Be(SendOutcome.Sent);

        // Act
        sender.Reactor.Pump();
        SendOutcome thirdOutcome = sender.Reactor.Send(peer, DeliveryMode.Unreliable, third);
        List<byte[]> delivered = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (delivered.Count < 4 && !deadline.Expired)
        {
            receiver.Reactor.Pump();
            delivered.AddRange(receiver.DrainPayloads());
            sender.Reactor.Pump();
            PumpDeadline.Yield();
        }

        // Assert
        thirdOutcome.Should().Be(SendOutcome.Sent);
        delivered.Should().HaveCount(4);
        delivered.Where(m => m.Length == size).Should().SatisfyRespectively(
            m => m.Should().Equal(first),
            m => m.Should().Equal(second),
            m => m.Should().Equal(third));
        delivered.Should().ContainSingle(m => m.Length == reliable.Length).Which.Should().Equal(reliable);
    }

    [Fact]
    public void Send_AfterASendQueueFullRefusal_SpendsNoSequence()
    {
        // Arrange: the budget holds two 1200-byte Messages.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, capacity: 1, unreliableQueueBytes: 2 * (1200 + RecordHeaderBytes));
        using DatagramSessionReactorHarness accepter = new(capacity: 1);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
        int capturesBefore = tap.Captures.Count;
        dialler.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1200]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1200]).Should().Be(SendOutcome.Sent);

        // Act
        SendOutcome refused = dialler.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1200]);
        DatagramSessionReactorHarness.PumpOnceAndRelayDataDatagrams(dialler, tap, capturesBefore, 2);
        SendOutcome retried = dialler.Reactor.Send(peer, DeliveryMode.Unreliable, new byte[1200]);
        DatagramSessionReactorHarness.PumpOnceAndRelayDataDatagrams(dialler, tap, capturesBefore, 3);

        // Assert
        refused.Should().Be(SendOutcome.SendQueueFull);
        retried.Should().Be(SendOutcome.Sent);
        DiallerSequences(tap).Should().Equal([0u, 1u, 2u], "the refused Message took no sequence, so the retry follows the second");
    }

    [Fact]
    public void Pump_TwoPeersQueueingUnreliableMessages_EachReceiverGetsItsOwnBytes()
    {
        // Arrange: one Datagram per Pump per Peer, so each Pump sends one Message to each receiver and
        // compacts both queues while they still hold bytes.
        const int size = 32_738;
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, capacity: 2, maxMessageSize: size, fragmentSize: size, retentionBytes: size,
            unreliableQueueBytes: 3 * (size + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiverA = new(
            capacity: 1, maxMessageSize: size, fragmentSize: size, retentionBytes: size, arenaSize: 4 * size);
        using DatagramSessionReactorHarness receiverB = new(
            capacity: 1, maxMessageSize: size, fragmentSize: size, retentionBytes: size, arenaSize: 4 * size);
        PeerId peerA = sender.Reactor.Connect(receiverA.Endpoint);
        PeerId peerB = sender.Reactor.Connect(receiverB.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiverA);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiverB);
        byte[][] toA = [StreamOf(size, 1), StreamOf(size, 2), StreamOf(size, 3), StreamOf(size, 4)];
        byte[][] toB = [StreamOf(size, 101), StreamOf(size, 102), StreamOf(size, 103), StreamOf(size, 104)];
        for (int i = 0; i < 3; i++)
        {
            sender.Reactor.Send(peerA, DeliveryMode.Unreliable, toA[i]).Should().Be(SendOutcome.Sent);
            sender.Reactor.Send(peerB, DeliveryMode.Unreliable, toB[i]).Should().Be(SendOutcome.Sent);
        }

        // Act: the first Pump sends Message 1 to each and compacts 2 and 3 to the front of each region.
        sender.Reactor.Pump();
        SendOutcome fourthA = sender.Reactor.Send(peerA, DeliveryMode.Unreliable, toA[3]);
        SendOutcome fourthB = sender.Reactor.Send(peerB, DeliveryMode.Unreliable, toB[3]);
        List<byte[]> deliveredA = [];
        List<byte[]> deliveredB = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while ((deliveredA.Count < 4 || deliveredB.Count < 4) && !deadline.Expired)
        {
            receiverA.Reactor.Pump();
            receiverB.Reactor.Pump();
            deliveredA.AddRange(receiverA.DrainPayloads());
            deliveredB.AddRange(receiverB.DrainPayloads());
            sender.Reactor.Pump();
            PumpDeadline.Yield();
        }

        // Assert
        fourthA.Should().Be(SendOutcome.Sent);
        fourthB.Should().Be(SendOutcome.Sent);
        deliveredA.Should().HaveCount(4);
        deliveredB.Should().HaveCount(4);
        for (int i = 0; i < 4; i++)
        {
            deliveredA[i].Should().Equal(toA[i]);
            deliveredB[i].Should().Equal(toB[i]);
        }
    }

    [Fact]
    public void Send_UnreliableAfterTheQueueHasCycled_AllocatesNothing()
    {
        // Arrange: two fill-and-flush cycles grow the queue list to its working size.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, unreliableQueueBytes: 12 * (100 + RecordHeaderBytes));
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        byte[] message = new byte[100];
        for (int cycle = 0; cycle < 2; cycle++)
        {
            while (sender.Reactor.Send(peer, DeliveryMode.Unreliable, message) == SendOutcome.Sent)
            {
            }

            sender.Reactor.Pump();
        }

        // Act
        long before = GC.GetAllocatedBytesForCurrentThread();
        while (sender.Reactor.Send(peer, DeliveryMode.Unreliable, message) == SendOutcome.Sent)
        {
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // Assert
        allocated.Should().Be(0, "queued unreliable bytes live in storage allocated at construction");
    }

    /// <summary>The sequence of every unfragmented record in every data datagram the tap relayed from the dialler, in capture order.</summary>
    private static List<uint> DiallerSequences(WireTap tap)
    {
        const int datagramHeaderSize = 20;
        List<uint> sequences = [];
        foreach (byte[] datagram in tap.PayloadsInDirection(TapDirection.DiallerToAccepter))
        {
            if (BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4)) == 0)
            {
                continue;
            }

            int offset = datagramHeaderSize;
            while (offset < datagram.Length)
            {
                sequences.Add(BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4)));
                ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
                offset += RecordHeaderBytes + length;
            }
        }

        return sequences;
    }

    private static byte[] StreamOf(int length, byte seed) =>
        Enumerable.Range(0, length).Select(i => (byte)(seed + i)).ToArray();
}
