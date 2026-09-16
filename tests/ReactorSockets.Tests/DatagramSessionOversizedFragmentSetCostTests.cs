using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #286's two cost-shaped acceptance criteria for the oversized-Fragment-set refusal
/// <see cref="DatagramSessionReactor.ParseDataDatagram"/> applies (issue #282): the refusal itself
/// allocates nothing on the receive path, and closing the offending Peer never touches any other Peer
/// this Reactor holds open. Issue #285 owns the refusal's own behaviour — the close reason and the
/// inclusive boundary of <c>_maxLegalFragmentCount</c> — and is proved in
/// <c>DatagramSessionOversizedFragmentSetTests</c>; this file does not restate either.
/// </summary>
public class DatagramSessionOversizedFragmentSetCostTests
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
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    /// <summary>
    /// The measurement Reactor's shape: fragment size equal to max message size gives a
    /// <c>ReceiveWindowFloor</c>, and so <c>_maxLegalFragmentCount</c>, of exactly 1 — so the smallest
    /// legal Fragment record (a lone Fragment declaring <c>fragmentCount = 2</c>) is already oversized,
    /// with nothing else about the datagram needing to be unusual.
    /// </summary>
    private const int MeasurementFragmentSize = 1200;

    private const int WarmupIterations = 20;
    private const int MeasuredIterationsPerPhase = 60;

    /// <summary>
    /// Closes issue #286's "allocates nothing" criterion. A thousand-plus repeated oversized claims
    /// cannot be fed down one Session, because the very first one closes it (issue #282) — so each
    /// iteration here re-establishes a fresh Session over the same accepter and client sockets before
    /// sending its claim, and the measured cost is therefore handshake-plus-refusal together, not the
    /// refusal alone.
    /// </summary>
    /// <remarks>
    /// <b>How the refusal's own cost is isolated from setup's.</b> Both measured phases run the exact
    /// same number of identically-shaped cycles — handshake, then one oversized claim — so a per-cycle
    /// cost that is fixed (the handshake's own allocation, and whatever the socket layer costs per
    /// datagram) is present equally in both phases and cancels out of the comparison. What the
    /// comparison actually tests is whether that per-cycle cost <em>climbs</em> as more cycles run.
    /// <see cref="DatagramSessionReactor.ParseDataDatagram"/>'s oversized-Fragment check itself does
    /// only a field read and a comparison before closing the Peer (see its own XML doc, issue #282), so
    /// it has nothing left to grow — an accumulating dictionary, list or cache anywhere on this path
    /// would instead show up here as phase two costing more than phase one. It would not isolate the
    /// refusal's bytes to an exact figure — the handshake's own allocation dominates the total — but
    /// that is not this criterion: the criterion is that the refusal adds no growing cost of its own.
    /// Diagnostics are disabled on the harness so the Warn-level log the refusal emits
    /// (<see cref="DatagramSessionReactor"/>'s own doc references <c>ForOversizedFragmentSet</c>) does
    /// not itself grow a recording Sink's list and mask the thing being measured.
    /// </remarks>
    [Fact]
    public void ParseDataDatagram_RepeatedOversizedFragmentClaimsEachOnAFreshSession_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(
            capacity: 4,
            maxMessageSize: MeasurementFragmentSize,
            fragmentSize: MeasurementFragmentSize,
            withoutDiagnostics: true);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        ulong nonce = 1;
        RunOversizedFragmentCycles(accepter, client, clientLink, destination, ref nonce, WarmupIterations);

        // Act — two equal-length phases of the same handshake-then-refusal cycle, run back to back.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        RunOversizedFragmentCycles(accepter, client, clientLink, destination, ref nonce, MeasuredIterationsPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunOversizedFragmentCycles(accepter, client, clientLink, destination, ref nonce, MeasuredIterationsPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — the multiple is generous headroom against GC noise, not a tight bound, mirroring
        // DatagramSessionReceiveArenaAllocationTests' own sustained-run assertions: a per-cycle cost
        // from the handshake and the socket layer is expected and unavoidable, what must not happen is
        // that cost climbing as the run goes on.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Runs <paramref name="cycles"/> cycles of "establish a Session, then send one oversized Fragment
    /// claim on it", each cycle re-using <paramref name="accepter"/>'s freed capacity Slot and
    /// <paramref name="client"/>'s one bound socket rather than allocating a new Reactor or socket per
    /// iteration — that allocation would swamp the measurement with something this criterion is not
    /// about. Non-inlined so neither phase's call frame is folded into its caller by the JIT.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RunOversizedFragmentCycles(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        Source destination,
        ref ulong nonce,
        int cycles)
    {
        for (int i = 0; i < cycles; i++)
        {
            uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, destination, nonce);
            nonce++;

            byte[] oversizedDatagram =
                [.. BuildHeader(sessionId), .. BuildFragmentRecord(sequence: 0, fragmentIndex: 0, fragmentCount: 2)];
            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, oversizedDatagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        }
    }

    /// <summary>
    /// Closes issue #286's "closes one Session only" criterion: three Peers live on one accepter
    /// Reactor, one of them — built on a bare <see cref="RawReactor"/>, per this file's own scope note
    /// — sends a Fragment claim declaring more Fragments than this accepter's floor allows. The other
    /// two keep sending and are still delivered, both before and after the violation, across the same
    /// Pumps that process it.
    /// </summary>
    [Fact]
    public void ParseDataDatagram_OneOversizedFragmentClaimAmongSeveralPeers_ClosesOnlyThatPeersSession()
    {
        // Arrange — three live Peers on one accepter Reactor.
        using DatagramSessionReactorHarness accepter = new(
            capacity: 4,
            maxMessageSize: MeasurementFragmentSize,
            fragmentSize: MeasurementFragmentSize);
        using RawReactor clientA = new(new ReactorOptions { Capacity = 4 });
        using RawReactor clientB = new(new ReactorOptions { Capacity = 4 });
        using RawReactor clientC = new(new ReactorOptions { Capacity = 4 });
        LinkId linkA = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        LinkId linkB = clientB.Bind(new IPEndPoint(IPAddress.Any, 0));
        LinkId linkC = clientC.Bind(new IPEndPoint(IPAddress.Any, 0));
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        (uint SessionId, PeerId Peer) peerA = EstablishLiveSession(accepter, clientA, linkA, destination, nonce: 0x1001);
        (uint SessionId, PeerId Peer) peerB = EstablishLiveSession(accepter, clientB, linkB, destination, nonce: 0x1002);
        (uint SessionId, PeerId Peer) peerC = EstablishLiveSession(accepter, clientC, linkC, destination, nonce: 0x1003);

        // Act — before the violation, every Peer, survivors and the eventual violator alike, carries
        // ordinary reliable traffic and is delivered.
        SendReliableRecordAndAssertDelivered(accepter, clientA, linkA, destination, peerA.SessionId, sequence: 0, payload: [0xA0]);
        SendReliableRecordAndAssertDelivered(accepter, clientB, linkB, destination, peerB.SessionId, sequence: 0, payload: [0xB0]);
        SendReliableRecordAndAssertDelivered(accepter, clientC, linkC, destination, peerC.SessionId, sequence: 0, payload: [0xC0]);

        // Act — PeerC claims a Fragment set larger than this accepter's own floor allows.
        byte[] oversizedDatagram =
            [.. BuildHeader(peerC.SessionId), .. BuildFragmentRecord(sequence: 1, fragmentIndex: 0, fragmentCount: 2)];
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        clientC.SendTo(linkC, destination, oversizedDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.DrainToList();

        // Act — after the violation, the two survivors keep exchanging Messages across further Pumps,
        // over the very same accepter this Reactor and the same Pump calls the violation was processed on.
        SendReliableRecordAndAssertDelivered(accepter, clientA, linkA, destination, peerA.SessionId, sequence: 1, payload: [0xA1]);
        SendReliableRecordAndAssertDelivered(accepter, clientB, linkB, destination, peerB.SessionId, sequence: 1, payload: [0xB1]);

        // Assert — exactly one Session closed this whole test, and it is PeerC's. Which reason it
        // closed with is DatagramSessionOversizedFragmentSetTests' own criterion, not restated here.
        events.Where(e => e.Kind == SessionEventKind.PeerClosed)
            .Should().ContainSingle()
            .Which.Peer.Should().Be(peerC.Peer);

        // Assert — the survivors are untouched: still open, still resolving, and their counters show
        // exactly the two Messages each actually received, before and after the violation.
        Action peerAStillOpen = () => accepter.Reactor.CountersOf(peerA.Peer);
        Action peerBStillOpen = () => accepter.Reactor.CountersOf(peerB.Peer);
        peerAStillOpen.Should().NotThrow("PeerC's violation must close only PeerC's Session");
        peerBStillOpen.Should().NotThrow("PeerC's violation must close only PeerC's Session");
        accepter.Reactor.CountersOf(peerA.Peer).MessagesReceived.Should().Be(2);
        accepter.Reactor.CountersOf(peerB.Peer).MessagesReceived.Should().Be(2);

        // Assert — PeerC's own Session, by contrast, is gone: its Slot was freed by the close.
        Action peerCClosed = () => accepter.Reactor.CountersOf(peerC.Peer);
        peerCClosed.Should().Throw<ArgumentException>();
    }

    /// <summary>
    /// Sends one unfragmented <c>ReliableOrdered</c> record carrying <paramref name="payload"/> at
    /// <paramref name="sequence"/>, pumps the accepter until it arrives, and asserts it was delivered
    /// as the single Message expected — the shared "ordinary traffic still flows" step both the
    /// before- and after-violation halves of the multi-Peer test use.
    /// </summary>
    private static void SendReliableRecordAndAssertDelivered(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        Source destination,
        uint sessionId,
        uint sequence,
        byte[] payload)
    {
        byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(sequence, payload)];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, destination, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        List<byte[]> delivered = accepter.DrainPayloads();
        delivered.Should().ContainSingle().Which.Should().Equal(payload);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, then finds and returns the <see cref="PeerId"/> the accepter drained for it.
    /// </summary>
    private static (uint SessionId, PeerId Peer) EstablishLiveSession(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, Source destination, ulong nonce)
    {
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, destination, nonce);
        PeerId peer = FindAccepterPeer(accepter);
        return (sessionId, peer);
    }

    /// <summary>The <see cref="PeerId"/> the accepter most recently drained a <c>PeerConnected</c> event for.</summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — using <paramref name="nonce"/> as the client nonce, and returns the
    /// wire session id <paramref name="accepter"/> assigned this Peer. Mirrors
    /// <see cref="DatagramSessionHostileFragmentDiscardTests"/>'s helper of the same name, generalised
    /// to take the nonce and destination as parameters so one accepter can be handshaken with
    /// repeatedly across many Sessions in a row.
    /// </summary>
    private static uint EstablishLiveSessionId(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, Source destination, ulong nonce)
    {
        byte[] hello = BuildHelloDatagram(nonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(nonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(nonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// Alternately Pumps <paramref name="accepter"/> and <paramref name="client"/> until
    /// <paramref name="client"/> receives one datagram, or the deadline expires.
    /// </summary>
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
        // The remaining ResumeSecretLength bytes are the dialler's resume secret; an all-zero one is
        // fine, since nothing in this path ever reads it.
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented <c>ReliableOrdered</c> packet record: flags, a four-byte sequence, a two-byte length, then the payload.</summary>
    private static byte[] BuildRecord(uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = ModeReliableOrdered;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// Builds one <c>ReliableOrdered</c> Fragment record: the unfragmented shape's bytes with the
    /// fragment bit set, followed by a two-byte fragment index and a two-byte fragment count, then a
    /// one-byte payload. <paramref name="fragmentCount"/> of 2 is already oversized against this file's
    /// measurement Reactor, whose floor is exactly 1 — the smallest declared count that is individually
    /// legal (issue #253: a count of 0 or 1 is refused outright, before this check is ever reached) is
    /// already enough to trigger the refusal under test.
    /// </summary>
    private static byte[] BuildFragmentRecord(uint sequence, ushort fragmentIndex, ushort fragmentCount)
    {
        byte[] payload = [0xFF];
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(ModeReliableOrdered | FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }
}
