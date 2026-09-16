using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.2.6 (issues #173, #176): both unreliable Spaces' receive windows slide their base forward
/// rather than stalling on a hole (#173's design decision), and every sequence either slide retires
/// un-admitted raises the one per-Peer loss figure by exactly one — with no Diagnostic per gap (ADR 0013),
/// since a lossy path must not fill the Sink with volume. The instrument is the one
/// <see cref="DatagramSessionSpaceRoutingTests"/> already uses: a raw dialler completing an ordinary
/// handshake by hand, then hand-built data datagrams sent at the live session id the accepter assigned.
/// </summary>
/// <remarks>
/// Every window here is 2 fragments wide (<c>ReceiveWindowFragments: 2</c>, paired with a
/// 1300-byte <c>MaxMessageSize</c> so 2 clears that floor) rather than the 4 the routing tests use: a
/// window of 2 is what lets a handful of small sequence numbers (0-5) both open a hole and later force
/// the window past it, without needing an unreasonably long send to reach the far edge.
/// </remarks>
public class DatagramSessionUnreliableLossTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;
    private const uint DialerMaxMessageSize = 4096;
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;
    private const ulong DialerLingerMilliseconds = 30_000;

    private const int DatagramHeaderSize = 20;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;

    [Fact]
    public void Pump_SequencesZeroToFiveWithThreeOmitted_RaisesUnreliableLossByOneOnceTheWindowPassesThree()
    {
        // Arrange — a 2-fragment window based at 0. Sending 0, 1 and 2 in order advances the base to 3
        // with no holes; sequence 3 itself is then a hole the window is waiting on, and sequence 4
        // still fits inside the window without forcing anything. Only sequence 5 sits at the far edge
        // (offset 2 from base 3, at capacity 2), so admitting it slides the base past the still-open
        // hole at 3 — the one loss this test exists to pin.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act — 0, 1, 2, 4, 5 in order, deliberately omitting 3.
        foreach (uint sequence in new uint[] { 0, 1, 2, 4, 5 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        // Assert — the window has moved past 3 (base is now 6, past the sequence 5 it just admitted),
        // and exactly one loss registered: 3, never seen, retired by the slide that admitted 5.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(1);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_ASequenceArrivingLateButStillInsideTheWindow_DoesNotLeaveAPermanentGapInTheFigure()
    {
        // Arrange — a 2-fragment window based at 0. Sequence 0 admits and advances the base to 1;
        // sequence 2 then fits inside the window (offset 1 from base 1, still under capacity 2) without
        // any slide, leaving a hole at 1. Sequence 1 then arrives late, still well inside the window —
        // no slide is ever needed to admit it.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act
        foreach (uint sequence in new uint[] { 0, 2, 1 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        // Assert — a gap is provisional: 1 arrived before the window ever had to pass it by, so it
        // never touches the loss figure.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_ADuplicateInTheUnreliableSpaceAfterALoss_DoesNotChangeTheLossFigure()
    {
        // Arrange — 0, 1 and 2 admit in order (base advances to 3, no loss). 5 then sits at the far
        // edge from base 3 (offset 2, capacity 2), so admitting it slides past the hole at 3 — one
        // loss — to base 4, where it stays held rather than being cleared: 4 itself never arrived, so
        // the base is blocked on it and 5 remains a tracked, still-classifiable admission.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        foreach (uint sequence in new uint[] { 0, 1, 2, 5 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        accepter.Reactor.CountersOf(accepterPeer).UnreliableLossPackets.Should().Be(1);

        // Act — resend sequence 5, still tracked as admitted, so it classifies as a duplicate.
        byte[] duplicate = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence: 5, payload: [])];
        ulong duplicateDatagramsBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), duplicate);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > duplicateDatagramsBefore);

        // Assert — the duplicate is counted on its own figure and the loss figure is untouched.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.DuplicatePackets.Should().Be(1);
        counters.UnreliableLossPackets.Should().Be(1);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_LossInTheUnreliableSpace_IsNotMovedByTheTwoReliableSpaces()
    {
        // Arrange — the same 0, 1, 2, 4, 5 sequence, raising the Unreliable Space's loss figure to 1.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        foreach (uint sequence in new uint[] { 0, 1, 2, 4, 5 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        accepter.Reactor.CountersOf(accepterPeer).UnreliableLossPackets.Should().Be(1);

        // Act — exercise both reliable Spaces: sequence 0 admits cleanly in each, and a far-ahead
        // sequence 5 in each is refused as out of window rather than sliding, because retransmission
        // is what fills a reliable Space's holes.
        byte[] reliableSpaces =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeReliableUnordered, sequence: 0, payload: []),
            .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: []),
            .. BuildRecord(ModeReliableUnordered, sequence: 5, payload: []),
            .. BuildRecord(ModeReliableOrdered, sequence: 5, payload: []),
        ];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), reliableSpaces);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        // Assert — the loss figure is exactly what it was: a Space whose window never slides cannot
        // contribute to it, whatever it refuses.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(1);
        counters.OutOfWindowPackets.Should().Be(2);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_SequencesZeroToFiveWithThreeOmittedInTheUnreliableSequencedSpace_RaisesUnreliableLossByOne()
    {
        // Arrange — the UnreliableSequenced Space slides on exactly the rule the Unreliable Space does
        // (#173), and its loss lands on the same figure (issue #176): UnreliableLossPackets covers both
        // unreliable Spaces, the way OutOfWindowPackets and DuplicatePackets already cover all four.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act — 0, 1, 2, 4, 5 in order, deliberately omitting 3, exactly as the Unreliable case above.
        foreach (uint sequence in new uint[] { 0, 1, 2, 4, 5 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliableSequenced, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        // Assert — one loss: 3, never seen, retired by the slide that admitted 5.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(1);
        counters.OutOfWindowPackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_LossInBothUnreliableSpaces_AccumulatesOntoTheOneFigure()
    {
        // Arrange — the two unreliable Spaces classify independently, so each opens and passes its own
        // hole at 3, and the figure they share is the sum (issue #176).
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act — the same 0, 1, 2, 4, 5 run in each Space, both records carried in one datagram per step.
        foreach (uint sequence in new uint[] { 0, 1, 2, 4, 5 })
        {
            byte[] datagram =
            [
                .. BuildHeader(sessionId),
                .. BuildRecord(ModeUnreliable, sequence, payload: []),
                .. BuildRecord(ModeUnreliableSequenced, sequence, payload: []),
            ];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        // Assert — two losses on the one figure: sequence 3, lost once in each unreliable Space.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(2);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_TwoSeparateGapsPassedByTheWindow_RaisesTheLossFigureMonotonicallyAndNeverResets()
    {
        // Arrange — a 2-fragment window. The first gap (0, 1, 2, 4, 5 with 3 omitted) raises the
        // figure to 1; a second gap further up the sequence space raises it again rather than
        // replacing the first.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        foreach (uint sequence in new uint[] { 0, 1, 2, 4, 5 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        accepter.Reactor.CountersOf(accepterPeer).UnreliableLossPackets.Should().Be(1);

        // Act — the base is now 6, and nothing has been sent for 6 itself. Sending 8 next sits at the
        // far edge from base 6 (offset 2, at capacity 2) without 6 or 7 ever arriving, so admitting it
        // slides past the still-open hole at 6 — a second, independent loss.
        byte[] second = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence: 8, payload: [])];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), second);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        // Assert — the figure accumulated rather than reset: 2 total losses (3, then 7).
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(2);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_FirstArrivalJumpingFarBeyondTheWindowsCapacity_CountsEverySkippedSequenceIncludingThoseNeverTracked()
    {
        // Arrange — a 2-fragment window based at 0, spanning [0, 1]. The very first arrival is sequence
        // 100: the slide it forces retires the two slots the ring could ever have tracked (both holes),
        // plus every sequence between the old far edge and the new one that never had a slot to be
        // tracked in at all — a jump wider than the window, not merely one past its edge.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] farJump = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence: 100, payload: [])];

        // Act — the new base is 100 - 2 + 1 = 99, so 99 sequences (0 through 98) are retired un-admitted.
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), farJump);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        // Assert — admitted (not discarded), and every retired sequence counted, not only the two the
        // ring itself had slots for.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.UnreliableLossPackets.Should().Be(99);
        counters.OutOfWindowPackets.Should().Be(0);
        counters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_SequenceBelowTheBaseAfterASlide_ReturnsOutOfWindowWithoutRaisingTheLossFigureFurther()
    {
        // Arrange — the same 0, 1, 2, 4, 5 sequence (3 omitted) that raises the Unreliable Space's loss
        // figure to 1 by sliding the base to 6.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        foreach (uint sequence in new uint[] { 0, 1, 2, 4, 5 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload: [])];
            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        }

        accepter.Reactor.CountersOf(accepterPeer).UnreliableLossPackets.Should().Be(1);

        // Act — sequence 0 is already retired, below the base the slide left at 6: an ordinary
        // below-base arrival, not a new far-ahead one, so nothing should slide for it.
        byte[] belowBase = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence: 0, payload: [])];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), belowBase);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        // Assert — out of window, exactly as a non-sliding window reports a below-base sequence, and
        // the loss figure is exactly what it was: a below-base arrival is not a slide.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.OutOfWindowPackets.Should().Be(1);
        counters.UnreliableLossPackets.Should().Be(1);
        counters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionSpaceRoutingTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced,
    /// read off the drain so a test can call <see cref="DatagramSessionReactor.CountersOf"/> on it.
    /// Clears the Sink established by that handshake — establishment logs its own
    /// <see cref="DiagnosticId.LingerNegotiated"/> (issue #364), and every test in this file wants a
    /// clean Sink for the arrivals it is actually testing.
    /// </summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        PeerId peer = drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
        accepter.Sink.Received.Clear();
        return peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            accepter.Reactor.Pump();
            client.Pump();
            foreach (RawEvent evt in client.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    return evt.Payload.ToArray();
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The handshake reply did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    private static byte[] BuildHelloDatagram(ulong clientNonce)
    {
        byte[] body = new byte[4 + 8];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildHelloWithCookieDatagram(ulong clientNonce, long epoch, byte[] cookie)
    {
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + 8 + ResumeSecretLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerSessionId);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerMaxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerSendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerLingerMilliseconds);
        offset += 8;
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented packet record: flags, a four-byte sequence, a two-byte length, then the payload.</summary>
    private static byte[] BuildRecord(byte modeBits, uint sequence, ReadOnlySpan<byte> payload)
    {
        const int unfragmentedRecordSize = 7;
        byte[] record = new byte[unfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(unfragmentedRecordSize));
        return record;
    }
}
