using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.6.2 (issue #90): writes to a Peer's socket happen once per <see cref="StreamSessionReactor.Pump"/>
/// call, with everything currently queued in it — never once per <see cref="StreamSessionReactor.Send"/>,
/// and never off the back of readiness alone in a way that could produce more than one write per Pump
/// for the same Peer. Both acceptance criteria are proven the same way the parent ticket asks for: a
/// <see cref="WireObserver"/> wrapping a real <see cref="RawReactor"/> around the receiving end, so what
/// is asserted is Segments actually arriving on the wire rather than the Reactor's own call count. This
/// is the first stream-profile use of <see cref="WireObserver"/> — every earlier use is a datagram or
/// hostile-peer test — so the Session Reactor here dials out to the observer's listener and the
/// observer stands in for the counterparty, replying with a raw Hello record
/// (<see cref="WireObserver.SendOnAcceptedLink"/>) to complete the handshake exactly as
/// <c>StreamSessionRetentionFlushTests</c>' <c>FlushPeer</c> does from the other direction.
/// </summary>
public class StreamSessionWriteBatchingTests
{
    private const int RecordSize = 41; // 1 type byte + 40-byte Hello payload; a Refusal is padded to it.
    private const byte HelloType = 1;
    private const uint ProtocolVersion = 1;

    private static byte[] BuildHello(uint maxMessageSize = 4096, ulong sendIdleMs = 1000, ulong lingerMs = 1000)
    {
        byte[] record = new byte[RecordSize];
        record[0] = HelloType;
        int offset = 1;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), sendIdleMs);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), lingerMs);
        return record;
    }

    private static byte[] BuildFrame(byte[] payload)
    {
        byte[] frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)payload.Length);
        payload.CopyTo(frame.AsSpan(4));
        return frame;
    }

    private static byte[] PayloadOf(int length, byte marker)
    {
        byte[] payload = new byte[length];
        Array.Fill(payload, marker);
        return payload;
    }

    /// <summary>
    /// Dials <paramref name="dialler"/> out to a fresh listener on <paramref name="observer"/>, pumping
    /// both sides until the dialler's own Hello record has arrived as a Segment, then replies with a raw
    /// Hello of the observer's own so the Session establishes. Returns the established Peer id, with
    /// both sides' handshake traffic already drained out of <paramref name="observer"/>'s Segment list
    /// — every assertion a caller makes afterwards is about bytes this ticket's flush step put on the
    /// wire, not about the handshake that preceded it.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness dialler, WireObserver observer, int maxPumps = 20)
    {
        IPEndPoint target = observer.ListenTcp(new IPEndPoint(IPAddress.Loopback, 0));
        dialler.Reactor.Connect(target);

        for (int round = 0; round < maxPumps && observer.ReceivedSegments.Count == 0; round++)
        {
            dialler.Reactor.Pump();
            dialler.DrainToList();
            observer.Pump();
        }

        observer.AcceptedLinks.Should().HaveCount(1, "the dialler's Connect should have reached the observer's listener");
        observer.ReceivedSegments.Should().HaveCount(1, "the dialler's own Hello record should have arrived as a single Segment");
        // The content itself is not asserted here — it carries a random resume secret and the
        // dialler's own configured MaxMessageSize, neither of which this ticket's flush step touches.
        // Only its length matters: that the whole record arrived as one Segment.
        observer.ReceivedSegments[0].Should().HaveCount(RecordSize);

        observer.SendOnAcceptedLink(observer.AcceptedLinks[0], BuildHello());

        for (int round = 0; round < maxPumps; round++)
        {
            dialler.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialler.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    return evt.Peer;
                }
            }
        }

        throw new InvalidOperationException("The Session never established within the Pump budget.");
    }

    [Fact]
    public void Send_MultipleSendsBeforeASinglePump_ProducesExactlyOneSegmentOnTheWire()
    {
        // Arrange
        const int maxMessageSize = 32;
        using StreamSessionReactorHarness dialler = new(maxMessageSize: maxMessageSize);
        using WireObserver observer = new();
        PeerId peer = EstablishSession(dialler, observer);

        // The observer's Segment list is never reset — it already holds the dialler's own handshake
        // Hello from EstablishSession above — so what this ticket's flush step contributes is measured
        // from this baseline, not from an assumed-empty list.
        int baseline = observer.ReceivedSegments.Count;

        byte[] first = PayloadOf(maxMessageSize, 1);
        byte[] second = PayloadOf(maxMessageSize, 2);
        byte[] third = PayloadOf(maxMessageSize, 3);

        // Act — three Sends queued back-to-back with no Pump between them, then a single Pump.
        dialler.Reactor.Send(peer, first).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(peer, second).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(peer, third).Should().Be(SendOutcome.Sent);

        dialler.Reactor.Pump();
        observer.PumpUntil(() => observer.ReceivedSegments.Count >= baseline + 1);

        // Assert — regardless of how many Sends preceded it, one Pump produces exactly one new Segment
        // arrival, carrying all three framed Messages in order.
        observer.ReceivedSegments.Should().HaveCount(
            baseline + 1, "3.6.2's write batching means N Sends between two Pumps still produce exactly one write");
        observer.ReceivedSegments[baseline].Should().Equal([.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)]);
    }

    [Fact]
    public void Send_RepeatedAcrossSeveralPumps_ProducesExactlyOneSegmentPerPump()
    {
        // Arrange — the batching guarantee is per-Pump, not a one-time effect: a second and third round
        // of Sends, each followed by its own Pump, must each independently produce exactly one Segment.
        const int maxMessageSize = 16;
        using StreamSessionReactorHarness dialler = new(maxMessageSize: maxMessageSize);
        using WireObserver observer = new();
        PeerId peer = EstablishSession(dialler, observer);
        int baseline = observer.ReceivedSegments.Count;

        byte[] roundOneFirst = PayloadOf(maxMessageSize, 11);
        byte[] roundOneSecond = PayloadOf(maxMessageSize, 12);
        byte[] roundTwo = PayloadOf(maxMessageSize, 21);

        // Act — round one: two Sends, one Pump.
        dialler.Reactor.Send(peer, roundOneFirst).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(peer, roundOneSecond).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        observer.PumpUntil(() => observer.ReceivedSegments.Count >= baseline + 1);

        // Act — round two: one Send, one Pump.
        dialler.Reactor.Send(peer, roundTwo).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        observer.PumpUntil(() => observer.ReceivedSegments.Count >= baseline + 2);

        // Assert
        observer.ReceivedSegments.Should().HaveCount(baseline + 2, "each Pump that followed queued Sends must produce exactly one Segment");
        observer.ReceivedSegments[baseline].Should().Equal([.. BuildFrame(roundOneFirst), .. BuildFrame(roundOneSecond)]);
        observer.ReceivedSegments[baseline + 1].Should().Equal(BuildFrame(roundTwo));
    }

    [Fact]
    public void Send_WithNoFollowingPump_PutsNothingOnTheWire()
    {
        // Arrange
        const int maxMessageSize = 32;
        using StreamSessionReactorHarness dialler = new(maxMessageSize: maxMessageSize);
        using WireObserver observer = new();
        PeerId peer = EstablishSession(dialler, observer);
        int baseline = observer.ReceivedSegments.Count;

        byte[] message = PayloadOf(maxMessageSize, 7);

        // Act — a Send with no Pump call following it. Nothing ticks unless the consumer calls Pump, so
        // the observer is given every chance it would need to see bytes if any had actually gone out.
        dialler.Reactor.Send(peer, message).Should().Be(SendOutcome.Sent);
        for (int i = 0; i < 5; i++)
        {
            observer.Pump();
        }

        // Assert
        observer.ReceivedSegments.Should().HaveCount(baseline, "a Send with no following Pump call must put nothing on the wire");
    }
}
