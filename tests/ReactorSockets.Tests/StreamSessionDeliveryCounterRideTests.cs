using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #132: a two-way Peer's delivery-counter record rides the retention write already due in that
/// Pump — one gather write carrying the record and this end's own Messages — rather than taking a
/// write of its own.
/// </summary>
/// <remarks>
/// <para>
/// <b>How the write count is observed.</b> Two back-to-back writes inside one Pump land in the
/// counterparty's receive buffer before it reads, so a <see cref="WireObserver"/>'s per-read Segments
/// cannot separate them. The write count is instead read from <see cref="PeerCounters.SocketWrites"/>
/// (issue #325), the Counter seam tests/CLAUDE.md sanctions.
/// </para>
/// <para>
/// The byte stream is still asserted, rather than any read boundary in it: the record lands
/// immediately ahead of the Message the same Pump queued, whole, unaltered, once, carrying the right
/// count — the properties a gather write that split, reordered, duplicated or dropped either part
/// would break.
/// </para>
/// </remarks>
public class StreamSessionDeliveryCounterRideTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private const int DeliveryCounterRecordSize = 12;

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Dials <paramref name="dialler"/> out to a fresh listener on <paramref name="observer"/> and
    /// completes the handshake with a raw Hello of the observer's own, returning the established Peer
    /// id. The same shape <see cref="StreamSessionWriteBatchingTests"/>'s own <c>EstablishSession</c>
    /// uses: the Session Reactor dials out and the observer stands in for the counterparty, because
    /// only the dialling side of an observer-backed pair has a real Reactor to emit counters.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness dialler, WireObserver observer)
    {
        IPEndPoint target = observer.ListenTcp(new IPEndPoint(IPAddress.Loopback, 0));
        dialler.Reactor.Connect(target);

        PumpDeadline segmentDeadline = PumpDeadline.Start();
        while (observer.ReceivedSegments.Count == 0 && !segmentDeadline.Expired)
        {
            dialler.Reactor.Pump();
            dialler.DrainToList();
            observer.Pump();
            PumpDeadline.Yield();
        }

        observer.AcceptedLinks.Should().HaveCount(1, "the dialler's Connect should have reached the observer's listener");
        observer.ReceivedSegments.Should().HaveCount(1, "the dialler's own Hello record should have arrived as a single Segment");
        observer.ReceivedSegments[0].Should().HaveCount(RecordSize);

        observer.SendOnAcceptedLink(observer.AcceptedLinks[0], StreamSessionTestRecords.BuildHello());

        PumpDeadline connectedDeadline = PumpDeadline.Start();
        while (true)
        {
            dialler.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialler.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    return evt.Peer;
                }
            }

            if (connectedDeadline.Expired)
            {
                throw new InvalidOperationException("The Session never established within the Pump budget.");
            }

            observer.Pump();
            PumpDeadline.Yield();
        }
    }

    [Fact]
    public void Pump_TwoWayTraffic_PutsTheDeliveryCounterImmediatelyAheadOfThatPumpsOwnMessage()
    {
        // Arrange — a Peer whose received count advances in a Pump that already owes a retention write.
        const int maxMessageSize = 32;
        using StreamSessionReactorHarness dialler = new(maxMessageSize: maxMessageSize);
        using WireObserver observer = new();
        PeerId peer = EstablishSession(dialler, observer);
        int baseline = observer.ReceivedSegments.Count;

        byte[] inbound = PayloadOf(16, 9);
        byte[] outbound = PayloadOf(16, 8);
        byte[] outboundFrame = BuildFrame(outbound);

        // Act — the counterparty's Message goes on the wire first, then this end queues a Send before
        // every Pump until that Message is delivered. Queueing before every Pump rather than once is
        // what removes the race: whichever Pump ends up reading the inbound Message is guaranteed to
        // have a retention write due in it, which is the condition the ride is about. Loopback timing
        // decides which Pump that is; it must not decide whether the property is exercised at all.
        observer.SendOnAcceptedLink(observer.AcceptedLinks[0], BuildFrame(inbound));

        int pumpsWithASendQueued = 0;
        PumpDeadline deadline = PumpDeadline.Start();
        while (dialler.Reactor.CountersOf(peer).MessagesReceived == 0)
        {
            if (deadline.Expired)
            {
                throw new InvalidOperationException("The counterparty's Message was never delivered within the Pump budget.");
            }

            dialler.Reactor.Send(peer, outbound).Should().Be(SendOutcome.Sent);
            pumpsWithASendQueued++;
            dialler.Reactor.Pump();
            dialler.DrainToList();
            observer.Pump();
            PumpDeadline.Yield();
        }

        // Every Pump of that loop flushed one Message, and the last of them also emitted the counter.
        int expectedBytes = (pumpsWithASendQueued * outboundFrame.Length) + DeliveryCounterRecordSize;
        observer.PumpUntil(() => BytesSinceBaseline(observer, baseline).Length >= expectedBytes);

        // Assert — asserted over the byte stream, never over where the counterparty's reads happened to
        // split it: the record sits immediately ahead of the Message its own Pump queued, and appears
        // exactly once for the one Pump in which the received count advanced.
        byte[] wire = BytesSinceBaseline(observer, baseline);
        byte[] counterRecord = StreamSessionTestRecords.BuildDeliveryCounterRecord(1);

        wire.Should().HaveCount(expectedBytes, "nothing beyond this end's own Messages and the one counter record belongs on the wire");
        wire.Skip(wire.Length - DeliveryCounterRecordSize - outboundFrame.Length).Take(DeliveryCounterRecordSize).Should().Equal(
            counterRecord,
            "the counter leads the write it rides, exactly as it led the wire when it took a write of its own");
        wire.Skip(wire.Length - outboundFrame.Length).Should().Equal(
            outboundFrame,
            "this end's own Message follows the counter, whole and unaltered");
        CountOccurrences(wire, counterRecord).Should().Be(
            1,
            "one Pump advanced the received count, so exactly one counter record reaches the wire");
    }

    [Fact]
    public void Pump_TwoWayTraffic_TakesNoSocketWriteForTheDeliveryCounter()
    {
        // Arrange — the same stimulus as the ordering test above: a Send queued before every Pump, so
        // whichever Pump reads the counterparty's Message already owes a retention write.
        const int maxMessageSize = 32;
        using StreamSessionReactorHarness dialler = new(maxMessageSize: maxMessageSize);
        using WireObserver observer = new();
        PeerId peer = EstablishSession(dialler, observer);
        ulong writesBefore = dialler.Reactor.CountersOf(peer).SocketWrites;

        byte[] outbound = PayloadOf(16, 8);
        observer.SendOnAcceptedLink(observer.AcceptedLinks[0], BuildFrame(PayloadOf(16, 9)));

        // Act
        ulong pumpsWithASendQueued = 0;
        PumpDeadline deadline = PumpDeadline.Start();
        while (dialler.Reactor.CountersOf(peer).MessagesReceived == 0)
        {
            if (deadline.Expired)
            {
                throw new InvalidOperationException("The counterparty's Message was never delivered within the Pump budget.");
            }

            dialler.Reactor.Send(peer, outbound).Should().Be(SendOutcome.Sent);
            pumpsWithASendQueued++;
            dialler.Reactor.Pump();
            dialler.DrainToList();
            observer.Pump();
            PumpDeadline.Yield();
        }

        // Assert — one write per Pump for this end's own Message, and none added by the counter.
        (dialler.Reactor.CountersOf(peer).SocketWrites - writesBefore).Should().Be(
            pumpsWithASendQueued,
            "the delivery counter rides the retention write its Pump already owed rather than taking one of its own");
    }

    [Fact]
    public void Pump_PurelyReceivingPeer_StillEmitsTheDeliveryCounterWithNothingToRide()
    {
        // Arrange — the other half of #25's cost statement: a Peer that never Sends has no write for the
        // counter to ride, so the record goes out alone. The ride is an optimisation of a write that was
        // happening anyway, never a condition of emitting at all.
        const int maxMessageSize = 32;
        using StreamSessionReactorHarness dialler = new(maxMessageSize: maxMessageSize);
        using WireObserver observer = new();
        PeerId peer = EstablishSession(dialler, observer);
        int baseline = observer.ReceivedSegments.Count;
        ulong writesBefore = dialler.Reactor.CountersOf(peer).SocketWrites;

        byte[] inbound = PayloadOf(16, 5);

        // Act — nothing is ever queued on this end, so the only bytes it can write are the counter's.
        observer.SendOnAcceptedLink(observer.AcceptedLinks[0], BuildFrame(inbound));

        PumpDeadline deadline = PumpDeadline.Start();
        while (dialler.Reactor.CountersOf(peer).MessagesReceived == 0)
        {
            if (deadline.Expired)
            {
                throw new InvalidOperationException("The counterparty's Message was never delivered within the Pump budget.");
            }

            dialler.Reactor.Pump();
            dialler.DrainToList();
            observer.Pump();
            PumpDeadline.Yield();
        }

        observer.PumpUntil(() => BytesSinceBaseline(observer, baseline).Length >= DeliveryCounterRecordSize);

        // Assert
        BytesSinceBaseline(observer, baseline).Should().Equal(
            StreamSessionTestRecords.BuildDeliveryCounterRecord(1),
            "with nothing of its own queued, the counter record is the whole of what this end writes");
        (dialler.Reactor.CountersOf(peer).SocketWrites - writesBefore).Should().Be(
            1,
            "with no write to ride, the counter takes exactly one write of its own");
    }

    /// <summary>
    /// Every byte the observer has read past <paramref name="baseline"/> Segments, concatenated — the
    /// wire as a stream, which is what a TCP counterparty actually sees, rather than the read
    /// boundaries the observer's own Pump timing happened to produce.
    /// </summary>
    private static byte[] BytesSinceBaseline(WireObserver observer, int baseline)
    {
        List<byte> bytes = [];
        for (int i = baseline; i < observer.ReceivedSegments.Count; i++)
        {
            bytes.AddRange(observer.ReceivedSegments[i]);
        }

        return [.. bytes];
    }

    /// <summary>How many times <paramref name="needle"/> occurs in <paramref name="haystack"/>.</summary>
    private static int CountOccurrences(byte[] haystack, byte[] needle)
    {
        int count = 0;
        for (int start = 0; start + needle.Length <= haystack.Length; start++)
        {
            if (haystack.AsSpan(start, needle.Length).SequenceEqual(needle))
            {
                count++;
            }
        }

        return count;
    }
}
