using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #262: a half-arrived Fragment set survives an interruption of its Peer's transport at no
/// cost, because the pieces already held live in the ordinary per-Peer receive window
/// (<see cref="DatagramSessionReactor.HoldFragment"/>'s own remarks) â€” state a lost and rebuilt
/// transport never touches. The Datagram profile has no wire-level Resume of its own yet (that is
/// Slice 8, issue #52, still open); what already exists, and is exercised here, is the property that
/// makes such a Resume unnecessary for this purpose in the first place: a Session is identified by its
/// wire token rather than by the address or link a datagram arrived on
/// (<see cref="DatagramSessionReactor.ProcessDataDatagram"/>'s remarks; proved for demultiplexing by
/// <see cref="DatagramSessionIdentityTests.Pump_WhenALiveSessionsDatagramArrivesFromANewAddress_StillAttributesItToThatSession"/>).
/// An "interruption" here is therefore a real transport-level outage â€” a sustained run of dropped
/// datagrams in both directions â€” rather than a protocol event, and "resume" is nothing more than the
/// outage ending: no handshake, no counter exchange, nothing this library has to do to make the held
/// Fragments still be there afterwards.
/// </summary>
/// <remarks>
/// Every test drives two real <see cref="DatagramSessionReactor"/>s through a <see cref="WireTap"/>
/// chained ahead of a <see cref="LossyProxy"/> (dialler -&gt; tap -&gt; proxy -&gt; accepter), the same
/// composition <see cref="DatagramSessionFragmentRetransmitIdentityTests"/> uses: the tap captures every
/// Datagram the dialler actually transmits, including the ones the proxy then drops, which is what lets
/// the "not resent" assertion below read the wire directly rather than inferring it from a counter.
/// <see cref="FragmentSize"/> is sized so no two Fragment records ever share one Datagram (matching that
/// same test's own sizing note), so each dropped or interrupted Fragment's transmission is isolated to
/// its own captured Datagram.
/// </remarks>
/// <remarks>
/// <b>Finding, not this ticket's scope:</b> nothing in the current ack-application path
/// (<see cref="DatagramSessionReactor.ParseDataDatagram"/>'s two <c>ApplyAck</c> calls) ever calls
/// <see cref="RetentionPool.Release"/> for a Datagram Peer's send-side retention. <c>ApplyAck</c> only
/// releases a sequence from the retransmit-deadline tracker (<see cref="OutstandingReliableSequences"/>)
/// â€” a different "release" than freeing its <see cref="RetentionPool"/> slot. The only place
/// <see cref="RetentionPool.Release"/> is ever called is nowhere in production code: a Peer's outbound
/// retention slots for a reliable Space, once filled, stay filled for that Peer's whole life; they are
/// only reset in bulk when a vacated capacity Slot is reused by a new Peer
/// (<see cref="RetentionPool.ResetShareHolder"/>, exercised by
/// <see cref="DatagramSessionRetentionSlotRecycleTests"/>). This is unrelated to receive-side Fragment
/// survival â€” the subject of this file â€” and is left unfixed here, per this ticket's test-only scope;
/// <see cref="RetentionBytes"/> below is sized generously so this file's own further Sends are never
/// incidentally blocked by it.
/// </remarks>
public class DatagramSessionFragmentSurvivesInterruptionTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;

    /// <summary>The largest Datagram this library ever writes or reads.</summary>
    private const int MaxUdpDatagramSize = 65_507;

    /// <summary>
    /// One full Fragment record leaves under 12 spare bytes in a Datagram â€” less than even the
    /// smallest possible second record could need â€” so every Fragment this test sends, full or short,
    /// rides its own Datagram (matches <see cref="DatagramSessionFragmentRetransmitIdentityTests.FragmentSize"/>).
    /// </summary>
    private const int FragmentSize = MaxUdpDatagramSize - DatagramHeaderSize - FragmentRecordSize - 11;

    /// <summary>Four pieces: two arrive and are acked before the interruption, two are still missing when it begins.</summary>
    private const int FragmentCount = 4;

    /// <summary>Far short of <see cref="FragmentSize"/>, so the last Fragment is a genuine partial remainder.</summary>
    private const int LastFragmentRemainder = 1_000;

    /// <summary>An exact multiple of <see cref="FragmentSize"/> plus one short remainder, cutting into exactly <see cref="FragmentCount"/> Fragments.</summary>
    private const int MessageSize = (FragmentSize * (FragmentCount - 1)) + LastFragmentRemainder;

    /// <summary>
    /// Retention sized to hold every Fragment one Send retains, plus one further slot. Datagram
    /// retention slots are freed only when a Peer's Slot is reused by a new occupant
    /// (<see cref="RetentionPool.ResetShareHolder"/>) â€” nothing in the ack path calls
    /// <see cref="RetentionPool.Release"/> today (see the class remarks) â€” so the one extra slot is
    /// what <see cref="Pump_AfterCompletionFollowingTheInterruption_TheGroupsSlotsAreReleasedSoFurtherMessagesStillFitTheWindow"/>
    /// needs room for its own further Send, on top of the original set's four Fragments that stay
    /// retained for this Fixture's whole life.
    /// </summary>
    private const int RetentionBytes = FragmentSize * (FragmentCount + 1);

    /// <summary>How many Fragments arrive, and are acked, before the interruption begins.</summary>
    private const int FragmentsBeforeInterruption = FragmentCount / 2;

    /// <summary>Comfortably past the RTO, whose ceiling is the harness's one-second Linger.</summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Pump_APeerInterruptedWithAFragmentSetHalfArrived_ResumesAndDeliversTheMessageWholeExactlyOnce()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        byte[] message = fixture.SendMessageAndCarryItHalfwayThroughAnInterruption(DeliveryMode.ReliableOrdered);

        // Act â€” the outage ends: nothing further is dropped, so the still-missing Fragments' next
        // retransmission finally lands.
        List<byte[]> delivered = fixture.ResumeAndCollectDeliveredPayloads();

        // Assert â€” one whole Message, byte-exact, and counted exactly once.
        delivered.Should().ContainSingle("the half-arrived set completes into one Message, not a run of partial ones")
            .Which.Should().Equal(message);
        fixture.Accepter.Reactor.CountersOf(fixture.AccepterPeer).MessagesReceived.Should().Be(
            1UL, "the completed set is one Message, counted once, exactly as an unfragmented Message would be");
    }

    [Fact]
    public void Pump_AfterTheInterruptionEnds_TheFragmentsAckedBeforeItAreNeverRetransmitted()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        fixture.SendMessageAndCarryItHalfwayThroughAnInterruption(DeliveryMode.ReliableOrdered);
        int captureCountAtAck = fixture.Tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Count();

        // Act
        fixture.ResumeAndCollectDeliveredPayloads();

        // Assert â€” every Datagram the dialler has transmitted since the pre-interruption ack, captured
        // by the tap ahead of the proxy so a dropped attempt would show up too, is decoded by hand; the
        // sequences of the two Fragments already acked before the interruption never reappear among
        // them. This is the assertion the ticket turns on: the sender's own retransmission is selective
        // per unacked sequence, not a wholesale resend of the buffered set it happens to belong to.
        List<uint> sequencesSentSinceTheAck = fixture.Tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .Skip(captureCountAtAck)
            .SelectMany(DecodeFragmentSequences)
            .ToList();

        List<uint> ackedBeforeInterruption = Enumerable.Range(0, FragmentsBeforeInterruption).Select(i => (uint)i).ToList();
        sequencesSentSinceTheAck.Should().NotContain(
            ackedBeforeInterruption,
            "the Fragments already acked before the interruption were never resent on the wire, only the still-missing ones were");
        sequencesSentSinceTheAck.Should().Contain(
            (uint)FragmentsBeforeInterruption,
            "the still-missing Fragments are exactly what the resumed retransmission carries");
    }

    [Fact]
    public void Pump_AfterCompletionFollowingTheInterruption_TheGroupsSlotsAreReleasedSoFurtherMessagesStillFitTheWindow()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        fixture.SendMessageAndCarryItHalfwayThroughAnInterruption(DeliveryMode.ReliableOrdered);
        fixture.ResumeAndCollectDeliveredPayloads().Should().ContainSingle();

        // Act â€” an ordinary further Message. This is deliberately about the accepter's receive window,
        // not the dialler's own send-side retention (see the class remarks on RetentionPool.Release
        // never being called): RetentionBytes already gives the dialler's retention pool one spare slot
        // for exactly this Send, so nothing about this Act depends on the completed group's Fragments
        // ever being released from the dialler's own retention.
        byte[] further = [9, 9, 9];
        fixture.Dialler.Reactor.Send(fixture.DiallerPeer, DeliveryMode.ReliableOrdered, further).Should().Be(SendOutcome.Sent);
        List<byte[]> delivered = fixture.PumpRoundsCollectingPayloads(rounds: 6);

        // Assert
        delivered.Should().ContainSingle("the surviving group's slots were released on completion, exactly as an ordinary completion releases them")
            .Which.Should().Equal(further);
    }

    /// <summary>
    /// The dual-Space acceptance criterion is about receive-side survival, not the send side's single
    /// per-Peer queue (<see cref="DatagramSessionReactor.FlushPeer"/> packs one Peer's records strictly
    /// in Send order regardless of Space, so two independent live <c>Send</c> calls can never be made to
    /// progress "at once" â€” the second Space's own first Fragment would not even reach the wire until
    /// the first Space's whole queued Message had). Held Fragments are injected by hand through a bare
    /// <see cref="RawReactor"/> instead, exactly as <see cref="DatagramSessionFragmentInterleavingTests"/>
    /// does for the same reason: this proves the receive window, not the retransmit machinery, and #260
    /// already covers the retransmit side for a single Space.
    /// </summary>
    [Fact]
    public void Pump_AFragmentSetHalfArrivedAcrossBothReliableSpaces_SurvivesTheSameInterruptionAndBothComplete()
    {
        // Arrange â€” a two-piece set in each reliable Space, one piece of each held before the outage.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] orderedFragment0 = BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x10]);
        byte[] unorderedFragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x30]);
        byte[] orderedFragment1 = BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x20]);
        byte[] unorderedFragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x40]);

        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. orderedFragment0]);
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. unorderedFragment0]);
        accepter.DrainPayloads().Should().BeEmpty("only the first piece of each Space's set has arrived");

        // Act â€” the interruption: no datagram of any kind crosses the wire for a long stretch. Nothing
        // here is a timer this profile owns (no expiry for an incomplete set, by design), so this is
        // simulated the same way any silence is â€” Pumping with nothing sent, the clock advanced well
        // past every deadline that exists elsewhere in this Reactor, to prove none of them touches an
        // incomplete Fragment group either.
        accepter.TimeProvider.Advance(TimeSpan.FromMinutes(5));
        for (int i = 0; i < 5; i++)
        {
            accepter.Reactor.Pump();
        }

        accepter.DrainPayloads().Should().BeEmpty("the interruption is ongoing; both sets are still exactly half arrived");

        // The resume: nothing renegotiated, nothing handshaken â€” the remaining piece of each Space's set
        // simply arrives.
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. orderedFragment1]);
        List<byte[]> afterOrderedCompletes = accepter.DrainPayloads();

        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. unorderedFragment1]);
        List<byte[]> afterUnorderedCompletes = accepter.DrainPayloads();

        // Assert â€” both Spaces' sets survived the same outage and completed into their own distinct
        // Messages, neither one's progress visible in the other's.
        afterOrderedCompletes.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x10, 0x20 });
        afterUnorderedCompletes.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x30, 0x40 });
    }

    /// <summary>Reads every Fragment record's sequence out of one captured Datagram, ignoring handshake control Datagrams and unfragmented records.</summary>
    private static IEnumerable<uint> DecodeFragmentSequences(byte[] datagram)
    {
        if (datagram.Length < DatagramHeaderSize || BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4)) == 0)
        {
            yield break;
        }

        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;
            int headerSize = isFragment ? FragmentRecordSize : UnfragmentedRecordSize;

            if (isFragment)
            {
                yield return sequence;
            }

            offset += headerSize + length;
        }
    }

    // ---- RawReactor injection helpers for the dual-Space test, matching DatagramSessionFragmentInterleavingTests ----

    private const uint ControlSession = 0;
    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int InjectionResumeSecretLength = 16;
    private const int ControlDatagramSize = 256;
    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;
    private const uint DialerMaxMessageSize = 4096;
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte ModeReliableUnordered = 0b0000_0010;

    /// <summary>Completes an ordinary handshake by hand over <paramref name="client"/> and returns the wire session id <paramref name="accepter"/> assigned this Peer.</summary>
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

    private static void SendAndPumpUntilReceived(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, byte[] datagram)
    {
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
    }

    /// <summary>Alternately Pumps <paramref name="accepter"/> and <paramref name="client"/> until <paramref name="client"/> receives one datagram, or the deadline expires.</summary>
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
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + InjectionResumeSecretLength];
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
        // The remaining InjectionResumeSecretLength bytes are the dialler's resume secret; an all-zero
        // one is fine, since nothing in this file's path ever reads it.
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one Fragment record: the unfragmented shape's bytes with the fragment bit set, followed by a two-byte fragment index and a two-byte fragment count, then the payload.</summary>
    private static byte[] BuildFragmentRecord(
        byte modeBits, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }

    /// <summary>
    /// The one-Space fixture every test above but the last shares: a dialler and an accepter connected
    /// through a <see cref="WireTap"/> chained ahead of a <see cref="LossyProxy"/>, plus the send/interrupt/resume
    /// choreography every acceptance criterion needs a variant of.
    /// </summary>
    private sealed class Fixture : IDisposable
    {
        public required DatagramSessionReactorHarness Dialler { get; init; }
        public required DatagramSessionReactorHarness Accepter { get; init; }
        public required WireTap Tap { get; init; }
        public required LossyProxy Proxy { get; init; }
        public required PeerId DiallerPeer { get; init; }
        public required PeerId AccepterPeer { get; init; }

        public static Fixture Build()
        {
            DatagramSessionReactorHarness dialler = new(
                capacity: 1, bindLoopback: false, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
            DatagramSessionReactorHarness accepter = new(
                capacity: 1, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize,
                arenaSize: MessageSize, receiveWindowFragments: FragmentCount * 2);
            LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
            WireTap tap = new(proxy.PublicEndPoint, dialler.TimeProvider);

            PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
            PeerId accepterPeer = ConnectThroughTapAndProxy(dialler, tap, proxy, accepter);

            return new Fixture
            {
                Dialler = dialler,
                Accepter = accepter,
                Tap = tap,
                Proxy = proxy,
                DiallerPeer = diallerPeer,
                AccepterPeer = accepterPeer,
            };
        }

        /// <summary>
        /// Sends one <see cref="MessageSize"/>-byte Message under <paramref name="mode"/>, lets exactly
        /// <see cref="FragmentsBeforeInterruption"/> of its Fragments arrive and get acked, then starts a
        /// sustained outage â€” every remaining transmission, original and retransmitted alike, dropped â€”
        /// long enough to outlast more than one retransmit deadline, leaving the set genuinely
        /// interrupted rather than merely slow.
        /// </summary>
        public byte[] SendMessageAndCarryItHalfwayThroughAnInterruption(DeliveryMode mode)
        {
            byte[] message = new byte[MessageSize];
            for (int i = 0; i < message.Length; i++)
            {
                message[i] = (byte)i;
            }

            Dialler.Reactor.Send(DiallerPeer, mode, message).Should().Be(
                SendOutcome.Sent, "retention was sized to hold every Fragment this Message cuts into");

            for (int i = 0; i < FragmentsBeforeInterruption; i++)
            {
                PumpRelayRound();
            }

            Accepter.DrainPayloads().Should().BeEmpty("only half the set has arrived so far");
            Accepter.Reactor.CountersOf(AccepterPeer).MessagesReceived.Should().Be(0UL);

            // The accepter answers so its ack block â€” reflecting the Fragments it admitted while the
            // rest were still missing â€” travels back before the outage begins. Without this, every
            // Fragment the dialler is still tracking, acked or not, would eventually be treated as lost.
            Accepter.Reactor.Send(AccepterPeer, DeliveryMode.Unreliable, [0]).Should().Be(SendOutcome.Sent);
            Accepter.Reactor.Pump();
            Proxy.Pump();
            Tap.Pump();
            Dialler.Reactor.Pump();

            // The interruption: every remaining transmission from here â€” the still-missing Fragments'
            // first attempt and at least one retransmission of each â€” is dropped outright.
            int remainingFragments = FragmentCount - FragmentsBeforeInterruption;
            for (int i = 0; i < remainingFragments * 2; i++)
            {
                Proxy.DropNext(RelayDirection.ClientToUpstream);
            }

            for (int i = 0; i < remainingFragments; i++)
            {
                PumpRelayRound();
            }

            Dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);

            for (int i = 0; i < remainingFragments; i++)
            {
                PumpRelayRound();
            }

            Accepter.DrainPayloads().Should().BeEmpty("the interruption is still ongoing; nothing further could have arrived");
            Accepter.Reactor.CountersOf(AccepterPeer).MessagesReceived.Should().Be(
                0UL, "the surviving half of the set sits untouched in its slots throughout the interruption");

            return message;
        }

        /// <summary>Ends the interruption â€” nothing further is dropped â€” and pumps until the set completes.</summary>
        public List<byte[]> ResumeAndCollectDeliveredPayloads()
        {
            Dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);
            return PumpRoundsCollectingPayloads(rounds: 16);
        }

        public List<byte[]> PumpRoundsCollectingPayloads(int rounds)
        {
            List<byte[]> delivered = [];
            for (int i = 0; i < rounds; i++)
            {
                PumpRelayRound();
                delivered.AddRange(Accepter.DrainPayloads());
            }

            return delivered;
        }

        public void PumpRelayRound()
        {
            Dialler.Reactor.Pump();
            Tap.Pump();
            Proxy.Pump();
            Accepter.Reactor.Pump();
            Proxy.Pump();
            Tap.Pump();
        }

        private static PeerId ConnectThroughTapAndProxy(
            DatagramSessionReactorHarness dialler, WireTap tap, LossyProxy proxy, DatagramSessionReactorHarness accepter)
        {
            PumpDeadline deadline = PumpDeadline.Start();
            PeerId? accepterPeer = null;
            bool diallerConnected = false;

            while (true)
            {
                dialler.Reactor.Pump();
                tap.Pump();
                proxy.Pump();
                accepter.Reactor.Pump();
                proxy.Pump();
                tap.Pump();

                foreach ((SessionEventKind kind, PeerId eventPeer, DisconnectReason _) in accepter.DrainToList())
                {
                    if (kind == SessionEventKind.PeerConnected)
                    {
                        accepterPeer = eventPeer;
                    }
                }

                diallerConnected |= dialler.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);

                if (diallerConnected && accepterPeer is PeerId established)
                {
                    return established;
                }

                if (deadline.Expired)
                {
                    throw new InvalidOperationException("The tapped-and-proxied handshake did not complete within the Pump budget.");
                }

                PumpDeadline.Yield();
            }
        }

        public void Dispose()
        {
            Dialler.Dispose();
            Accepter.Dispose();
            Tap.Dispose();
            Proxy.Dispose();
        }
    }

}
