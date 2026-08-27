using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #178: the <c>u32</c> sequence wrap, exercised through the public seam. Ordering in the
/// datagram data path is decided in exactly one place — <c>SequenceNumber.IsNewerThan</c>, which
/// reinterprets an unsigned difference as signed so a sequence just above the wrap is newer than one
/// just below it — and until now nothing drove a receive window across that boundary.
/// </summary>
/// <remarks>
/// <para>
/// The wrap is reached by arriving at it, not by starting there: no test-only constructor parameter
/// and no widened member (ADR 0015). The two unreliable Spaces' windows slide their base forward to
/// admit a far-ahead arrival (#173), so a handful of forward jumps — each well inside the half-space
/// the serial comparison resolves — walks a window's base from zero up to just below
/// <see cref="uint.MaxValue"/> at the cost of a few datagrams. The instrument is the one
/// <see cref="DatagramSessionUnreliableLossTests"/> already uses: a raw dialler completing an ordinary
/// handshake by hand, then hand-built data datagrams sent at the live session id the accepter
/// assigned, with the three per-Peer classification counters as the only observation.
/// </para>
/// <para>
/// Every window here is 8 fragments wide, comfortably above this profile's floor of 4 for the
/// harness's 4096-byte <c>MaxMessageSize</c> and 1200-byte fragment size: wide enough that a run of
/// sequences either side of the boundary fits inside one window at once, which is what makes
/// "admitted, deduplicated and advanced across the wrap" a claim a single window can be asked about.
/// </para>
/// </remarks>
public class DatagramSessionSequenceWrapTests
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

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;

    /// <summary>The window width every test here builds its Reactor with; see the class remarks.</summary>
    private const int Capacity = 8;

    /// <summary>
    /// How far one walking jump moves a sliding window's base. Below 2^31, the distance beyond which
    /// serial comparison stops resolving two sequences at all, and large enough that five jumps cover
    /// the whole <c>u32</c> space — so walking to the wrap costs a handful of datagrams rather than
    /// four billion.
    /// </summary>
    private const uint JumpStride = 1_000_000_000;

    /// <summary>One past the largest <c>u32</c>: the wrap point, in the absolute space the model counts in.</summary>
    private const ulong Wrap = 1UL << 32;

    /// <summary>
    /// The fixed seed for the fuzz test's <see cref="Random"/>: the date this test was written, chosen
    /// only so a failure is reproducible across runs and machines — never re-rolled to make a failing
    /// case pass.
    /// </summary>
    private const int Seed = 20260820;

    /// <summary>How many fuzzed arrivals the property test drives across the wrap.</summary>
    private const int FuzzArrivals = 3000;

    /// <summary>How many records one fuzzed datagram carries; 32 of them fit far inside one fragment.</summary>
    private const int RecordsPerDatagram = 32;

    [Fact]
    public void Pump_ASequenceJustAboveTheWrapArrivingAtAWindowJustBelowIt_IsAdmittedRatherThanRefusedAsOlder()
    {
        // Arrange — walk the Unreliable Space's window up to a base of 0xFFFF_FFF0, so its span ends
        // sixteen sequences short of the wrap.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: Capacity);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        WindowModel model = new(Capacity);
        WalkBaseTo(accepter, client, clientLink, sessionId, ModeUnreliable, model, targetBase: 0xFFFF_FFF0);

        PeerCounters afterWalk = accepter.Reactor.CountersOf(accepterPeer);

        // Act — sequence 2, just above the wrap, which is eighteen ahead of the base and so past the
        // window's far edge. Numerically it is far below every sequence the window has just seen.
        Send(accepter, client, clientLink, sessionId, ModeUnreliable, sequence: 0x0000_0002);

        // Assert — treated as newer: the window slid it in and admitted it. A plain a > b comparison
        // would read it as older than the base and refuse it, which this counter would show.
        PeerCounters afterWrap = accepter.Reactor.CountersOf(accepterPeer);
        afterWrap.OutOfWindowPackets.Should().Be(afterWalk.OutOfWindowPackets);
        afterWrap.DuplicatePackets.Should().Be(afterWalk.DuplicatePackets);

        // Act — 0xFFFF_FFF8, just below the wrap, now sits below the base the slide left behind.
        Send(accepter, client, clientLink, sessionId, ModeUnreliable, sequence: 0xFFFF_FFF8);

        // Assert — refused as older, the other half of the same ordering: the sequence just above the
        // wrap really did displace the ones just below it.
        PeerCounters afterOlder = accepter.Reactor.CountersOf(accepterPeer);
        afterOlder.OutOfWindowPackets.Should().Be(afterWalk.OutOfWindowPackets + 1);
        afterOlder.DuplicatePackets.Should().Be(afterWalk.DuplicatePackets);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_ArrivalsSpanningTheWrap_AreAdmittedDeduplicatedAndAdvanceTheBaseAcrossIt()
    {
        // Arrange — walk to a base of 0xFFFF_FFF0, then let sequence 3 (just above the wrap) slide the
        // window to a base of 0xFFFF_FFFC. Its span is now [0xFFFF_FFFC, 0x0000_0004): eight slots
        // straddling the wrap, four either side, which is the only arrangement in which one window can
        // be asked to admit, deduplicate and advance across the boundary.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: Capacity);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        WindowModel model = new(Capacity);
        WalkBaseTo(accepter, client, clientLink, sessionId, ModeUnreliable, model, targetBase: 0xFFFF_FFF0);
        Send(accepter, client, clientLink, sessionId, ModeUnreliable, sequence: 0x0000_0003);

        PeerCounters straddling = accepter.Reactor.CountersOf(accepterPeer);

        // Act — a scripted run either side of the boundary. The base stays at 0xFFFF_FFFC through the
        // first five, since the sequence sitting on it has not arrived yet, which is what keeps every
        // admitted sequence still classifiable rather than immediately retired.
        uint[] arrivals =
        [
            0xFFFF_FFFD, // below the wrap, inside the window: admitted
            0xFFFF_FFFD, // duplicate 1, below the wrap
            0x0000_0001, // above the wrap, inside the same window: admitted
            0x0000_0001, // duplicate 2, above the wrap
            0x0000_0003, // duplicate 3: the sequence the slide itself admitted, still remembered
            0xFFFF_FFFC, // fills the base's own slot: admits and advances the base to 0xFFFF_FFFE
            0xFFFF_FFFC, // now below the base: out of window, which is how the advance shows
            0xFFFF_FFFE, // admitted, advancing the base to 0xFFFF_FFFF
            0xFFFF_FFFF, // admitted, advancing the base across the wrap itself to zero
            0x0000_0001, // duplicate 4: still admitted, and still inside a window now based past the wrap
        ];

        foreach (uint sequence in arrivals)
        {
            Send(accepter, client, clientLink, sessionId, ModeUnreliable, sequence);
        }

        // Assert — four duplicates and one below-the-base refusal, exactly as scripted. Nothing here
        // is derivable from a window that stopped at the wrap: every admission after the third arrival
        // depends on the boundary being invisible to the ordering.
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.DuplicatePackets.Should().Be(straddling.DuplicatePackets + 4);
        after.OutOfWindowPackets.Should().Be(straddling.OutOfWindowPackets + 1);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_FuzzedArrivalsWalkedOntoTheWrap_ClassifyExactlyAsAReferenceWindowDoes()
    {
        // Arrange — both sliding Spaces walked to a base 256 short of the wrap, then thousands of
        // randomly generated arrivals around each Space's current base, biased forward so the run
        // carries both windows across the boundary and well beyond it. The reference the outcomes are
        // checked against counts in absolute u64 sequences with ordinary comparisons, so it agrees
        // with the window only if the window's wrap-aware arithmetic is right.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: Capacity);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        WindowModel unreliable = new(Capacity);
        WindowModel unreliableSequenced = new(Capacity);
        WalkBaseTo(accepter, client, clientLink, sessionId, ModeUnreliable, unreliable, targetBase: 0xFFFF_FF00);
        WalkBaseTo(accepter, client, clientLink, sessionId, ModeUnreliableSequenced, unreliableSequenced, targetBase: 0xFFFF_FF00);

        PeerCounters beforeFuzz = accepter.Reactor.CountersOf(accepterPeer);
        ulong datagramsBefore = accepter.Reactor.Counters.DatagramsReceived;
        Random random = new(Seed);
        ulong expectedDuplicates = 0;
        ulong expectedOutOfWindow = 0;
        ulong expectedLoss = 0;
        int datagramsSent = 0;

        // Act — the corpus is generated and modelled record by record as each datagram is packed, so
        // every arrival is drawn relative to the base its own Space holds at that moment.
        for (int sent = 0; sent < FuzzArrivals; sent += RecordsPerDatagram)
        {
            List<byte> datagram = [.. BuildHeader(sessionId)];
            int records = Math.Min(RecordsPerDatagram, FuzzArrivals - sent);
            for (int i = 0; i < records; i++)
            {
                bool sequenced = random.Next(2) == 0;
                WindowModel model = sequenced ? unreliableSequenced : unreliable;
                byte mode = sequenced ? ModeUnreliableSequenced : ModeUnreliable;
                ulong sequence = NextArrival(random, model);

                (ReferenceOutcome outcome, ulong loss) = model.Classify(sequence);
                expectedDuplicates += outcome == ReferenceOutcome.Duplicate ? 1UL : 0UL;
                expectedOutOfWindow += outcome == ReferenceOutcome.OutOfWindow ? 1UL : 0UL;
                expectedLoss += loss;

                datagram.AddRange(BuildRecord(mode, (uint)sequence, payload: []));
            }

            ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), [.. datagram]);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
            datagramsSent++;
        }

        // Assert — the run really did cross the wrap in both Spaces, so the agreement below is
        // evidence about the boundary rather than about the ordinary case.
        unreliable.Base.Should().BeGreaterThan(Wrap);
        unreliableSequenced.Base.Should().BeGreaterThan(Wrap);

        // Assert — every datagram arrived, so the reference and the window saw the same input.
        (accepter.Reactor.Counters.DatagramsReceived - datagramsBefore).Should().Be((ulong)datagramsSent);

        // Assert — the three classification counters match the reference exactly. A single arrival
        // classified differently anywhere across the boundary moves at least one of them.
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        (after.DuplicatePackets - beforeFuzz.DuplicatePackets).Should().Be(expectedDuplicates);
        (after.OutOfWindowPackets - beforeFuzz.OutOfWindowPackets).Should().Be(expectedOutOfWindow);
        (after.UnreliableLossPackets - beforeFuzz.UnreliableLossPackets).Should().Be(expectedLoss);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");

        // Assert — each production window's own base really is where the reference says it is, and so
        // really is past the wrap: the counter sums above are aggregates, and only this pins the two
        // bases together. Three probes per Space, at the seam. The models are left behind here, since
        // nothing is asserted against them after this point.
        foreach ((WindowModel model, byte mode) in new[] { (unreliable, ModeUnreliable), (unreliableSequenced, ModeUnreliableSequenced) })
        {
            ulong basis = model.Base;
            PeerCounters beforeProbes = accepter.Reactor.CountersOf(accepterPeer);

            // One below the base: retired, so refused.
            Send(accepter, client, clientLink, sessionId, mode, (uint)(basis - 1));
            PeerCounters afterBelow = accepter.Reactor.CountersOf(accepterPeer);
            afterBelow.OutOfWindowPackets.Should().Be(beforeProbes.OutOfWindowPackets + 1);

            // The base's own sequence: the one thing the window is waiting on, so admitted — neither
            // refused as retired nor reported as already seen.
            Send(accepter, client, clientLink, sessionId, mode, (uint)basis);
            PeerCounters afterBase = accepter.Reactor.CountersOf(accepterPeer);
            afterBase.OutOfWindowPackets.Should().Be(beforeProbes.OutOfWindowPackets + 1);
            afterBase.DuplicatePackets.Should().Be(beforeProbes.DuplicatePackets);

            // The same sequence again: refused now, because admitting it advanced the base past it.
            Send(accepter, client, clientLink, sessionId, mode, (uint)basis);
            PeerCounters afterAdvance = accepter.Reactor.CountersOf(accepterPeer);
            afterAdvance.OutOfWindowPackets.Should().Be(beforeProbes.OutOfWindowPackets + 2);
            afterAdvance.DuplicatePackets.Should().Be(beforeProbes.DuplicatePackets);
        }
    }

    /// <summary>
    /// Draws the next arriving sequence for <paramref name="model"/>'s Space, as an offset from the
    /// base that Space currently holds: mostly inside the window (admissions and duplicates), often
    /// just past its far edge (a slide retiring a slot or two), sometimes below it (a refusal), and
    /// occasionally a long jump ahead. Every draw stays far inside the half-space serial comparison
    /// resolves, since a sender more than 2^31 ahead of the receiver is a distance the window's own
    /// bound rules out (ADR 0009).
    /// </summary>
    private static ulong NextArrival(Random random, WindowModel model)
    {
        int roll = random.Next(100);
        if (roll < 10)
        {
            return model.Base - (ulong)random.Next(1, 20);
        }

        if (roll < 70)
        {
            return model.Base + (ulong)random.Next(0, Capacity);
        }

        if (roll < 95)
        {
            return model.Base + (ulong)random.Next(Capacity, Capacity + 50);
        }

        return model.Base + (ulong)random.Next(1_000, 100_000);
    }

    /// <summary>
    /// Walks <paramref name="mode"/>'s sliding window forward until its base is exactly
    /// <paramref name="targetBase"/>, by sending far-ahead sequences that each slide it
    /// <see cref="JumpStride"/> closer. This is how the wrap is reached without a test-only
    /// constructor parameter: the window arrives at it, one ordinary admission at a time.
    /// </summary>
    private static void WalkBaseTo(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        byte mode,
        WindowModel model,
        ulong targetBase)
    {
        while (model.Base < targetBase)
        {
            // A slide leaves the base Capacity - 1 below the sequence that forced it, so the landing
            // sequence for the final jump is the target plus that much.
            ulong landing = Math.Min(model.Base + JumpStride, targetBase + Capacity - 1);
            model.Classify(landing);
            Send(accepter, client, clientLink, sessionId, mode, (uint)landing);
        }

        model.Base.Should().Be(targetBase);
    }

    /// <summary>
    /// Sends one single-record datagram and pumps the accepter until it has taken it off the socket,
    /// so an arrival is fully classified before the next one is sent.
    /// </summary>
    private static void Send(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        byte mode,
        uint sequence)
    {
        byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(mode, sequence, payload: [])];
        ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
    }

    /// <summary>
    /// The PeerId the accepter raised for the handshake completed above — the subject every counter
    /// assertion here reads.
    /// </summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> and returns the wire
    /// session id <paramref name="accepter"/> assigned this Peer, matching
    /// <see cref="DatagramSessionUnreliableLossTests"/>'s helper of the same name.
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
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + ResumeSecretLength];
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
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    private static byte[] BuildRecord(byte modeBits, uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// What the reference window reports for one arrival: the shape the production window's outcomes
    /// are compared against.
    /// </summary>
    private enum ReferenceOutcome
    {
        /// <summary>Inside the window and not seen before.</summary>
        Admitted,

        /// <summary>Inside the window and already recorded.</summary>
        Duplicate,

        /// <summary>Below the base, so already admitted or already retired.</summary>
        OutOfWindow,
    }

    /// <summary>
    /// A reference sliding receive window written the obvious way: it counts in absolute <c>u64</c>
    /// sequences that never wrap, so every ordering question it answers is a plain <c>&lt;</c>. The
    /// test knows the absolute sequence it put on the wire and the wire carries the low 32 bits of
    /// it, so the production window agrees with this model only if its wrap-aware arithmetic recovers
    /// the ordering the model never had to encode.
    /// </summary>
    /// <remarks>
    /// Deliberately not a second copy of the same trick: sharing the signed-difference arithmetic
    /// would make the comparison circular. Sliding mirrors what <c>ReceiveWindow</c> promises for a
    /// <c>slides: true</c> window (#173) — a sequence at or past the far edge moves the base to make
    /// room and retires every slot the move passes un-admitted.
    /// </remarks>
    private sealed class WindowModel(int capacity)
    {
        private readonly HashSet<ulong> _admitted = [];

        /// <summary>The oldest sequence not yet admitted, in absolute terms.</summary>
        public ulong Base { get; private set; }

        /// <summary>
        /// Classifies <paramref name="sequence"/>, returning the outcome together with how many
        /// sequences a slide retired un-admitted in the process.
        /// </summary>
        public (ReferenceOutcome Outcome, ulong Loss) Classify(ulong sequence)
        {
            if (sequence < Base)
            {
                return (ReferenceOutcome.OutOfWindow, 0);
            }

            ulong loss = 0;
            if (sequence >= Base + (ulong)capacity)
            {
                ulong newBase = sequence - (ulong)capacity + 1;
                ulong retired = newBase - Base;
                ulong salvaged = (ulong)_admitted.RemoveWhere(admitted => admitted < newBase);
                loss = retired - salvaged;
                Base = newBase;
            }

            if (!_admitted.Add(sequence))
            {
                return (ReferenceOutcome.Duplicate, loss);
            }

            while (_admitted.Remove(Base))
            {
                Base++;
            }

            return (ReferenceOutcome.Admitted, loss);
        }
    }
}
