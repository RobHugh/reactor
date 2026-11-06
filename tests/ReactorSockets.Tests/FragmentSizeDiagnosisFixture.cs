using System.Buffers.Binary;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// The shared instrument the fragment-size stall diagnosis tests are built on: two
/// <see cref="DatagramSessionReactor"/>s talking through a <see cref="LossyProxy"/>, all three driven by
/// one <see cref="FakeTimeProvider"/> so a single <see cref="PumpRound"/> call's clock advance moves every
/// retransmission timer in the scenario together. What differs between the positive (a size threshold
/// the Fragments cannot cross) and the negative (ordinary every-Nth loss) is only which standing rule the
/// proxy carries, so that rule is this fixture's one required constructor argument rather than a fact it
/// bakes in — everything else a run needs (establishing the pair, sending the two shapes of Message,
/// pumping all three together) lives here once.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why one clock.</b> <see cref="DatagramSessionReactorHarness"/> builds its own
/// <see cref="FakeTimeProvider"/> per instance by default, which is right for a single-Reactor test but
/// wrong here: the stall signature's four retransmission rounds are read off <c>RetransmissionTimer</c>
/// deadlines on both Peers and the proxy's own delayed-relay queue, and those only stay in lockstep if
/// they all read the same clock. The harness accepts an injected <see cref="FakeTimeProvider"/> for exactly
/// this reason (see its constructor remarks). A caller that passes none gets the harness's own clock.
/// </para>
/// <para>
/// <b>Why the roles are fixed.</b> The dialler is the only side that sends Messages; the accepter is the
/// only side the stall signature and the delivered payloads are read from. Issue #269's own diagnosis
/// needs an unfragmented reliable record acked on one Space while a Fragment on another goes unacked, so
/// <see cref="SendSmallMessage"/> uses <see cref="DeliveryMode.ReliableUnordered"/> and
/// <see cref="SendLargeMessage"/> uses <see cref="DeliveryMode.ReliableOrdered"/> — the same Space split
/// <see cref="DatagramSessionFragmentSizeUnviableTests"/> uses, but produced here by two live Sessions
/// acking whatever actually arrived rather than by a hand-crafted ack, since this fixture's whole point is
/// to prove the diagnosis end to end through a real proxy rather than against a picked ack.
/// </para>
/// <para>
/// <b>Bounded, not waited on.</b> Every pump method here, <see cref="Establish"/> included, takes a
/// round budget and returns once it is spent, never blocking on a condition that might not arrive — the
/// fixture's own contract is to terminate, leaving a caller's own assertions to report a stall that never
/// resolved as a failure rather than a hang. A handshake that never completes is the one failure a
/// caller cannot express an assertion about, since it happens before a caller holds the fixture at all,
/// so <see cref="Establish"/> is the one place here allowed to throw.
/// </para>
/// <para>
/// <b>Issue #304's "no wall-clock waits", including the handshake.</b> No method here reads a real
/// clock or sleeps. <see cref="Establish"/> pumps up to <see cref="DefaultHandshakeRounds"/> rounds and
/// gives up, rather than running against <see cref="PumpDeadline"/>'s <c>Thread.Sleep(1)</c> and
/// <see cref="System.Diagnostics.Stopwatch"/> budget the way the harness's own connect helpers do.
/// Measured on issue #304 before the switch, with a throwaway probe rather than a kept test: 5000 fresh
/// proxied handshakes on this path each completed within three rounds, none needing a wait of any kind. Every timing decision either run makes after that — RTO
/// expiry, backoff, and the four retransmission rounds <see cref="FragmentStallSignature"/> counts — is
/// read off <see cref="Clock"/>, the shared <see cref="FakeTimeProvider"/>, and advanced only by
/// <see cref="PumpRound"/>'s own <paramref name="advance"/> parameter.
/// </para>
/// <para>
/// <b>Why that does not generalise to the stream harness.</b> <see cref="PumpDeadline"/> exists for
/// issue #153, whose intermittent failures were all <c>StreamSession</c> handshake and resume tests
/// driving a real TCP connect and accept, where a connection can genuinely take about a millisecond to
/// become acceptable and no count of Pumps covers that. This fixture is UDP throughout: a datagram is
/// sent on the sender's own Pump with no accept to settle, which is why a round budget is sound here and
/// remains the wrong instrument in <see cref="StreamSessionReactorHarness"/> and the other real-time
/// callers of <see cref="PumpDeadline"/>. Do not read this fixture as precedent for those.
/// </para>
/// <para>
/// <b>Issue #307's shared assertions.</b> <see cref="AssertNoProbeWasObserved"/> and
/// <see cref="AssertConfiguredFragmentSizeUnchanged"/> live here, on the instrument both runs already
/// share, rather than as free functions in a file of their own: both read state only this fixture holds
/// — <see cref="Proxy"/>'s captured wire and <see cref="Dialler"/>/<see cref="Accepter"/>'s own
/// <see cref="DatagramSessionReactorHarness.Options"/> — and a caller in either run file gets the same
/// check by calling the same method, which is what stops a later change satisfying the "no probe, no
/// resize" decisions on one run while quietly breaking the other. The proxy's capture starts at
/// construction, before either run's own standing rule is installed, so both assertions read only what
/// was captured since the handshake completed — the handshake's own fixed-size control Datagrams are
/// not this fixture's own traffic to account for and would otherwise show up as unexplained.
/// </para>
/// </remarks>
internal sealed class FragmentSizeDiagnosisFixture : IDisposable
{
    /// <summary>
    /// The Reactor fragment size both ends are configured with. Small enough that
    /// <see cref="LargeMessageLength"/> is always cut into three Fragments, and large enough that
    /// <see cref="SmallMessageLength"/>'s unfragmented Datagram sits nowhere near it — the gap between the
    /// two is exactly where a caller's proxy size threshold belongs.
    /// </summary>
    public const int FragmentSize = 600;

    /// <summary>The <see cref="DatagramSessionOptions.MaxMessageSize"/> both ends are configured with.</summary>
    public const int MaxMessageSize = 4096;

    /// <summary>Sized so the fragmented Message and its retransmissions, on both Peers, stay retained for a whole run.</summary>
    public const int RetentionBytes = 16384;

    /// <summary>
    /// The payload length <see cref="SendSmallMessage"/> sends. One byte is comfortably unfragmented
    /// under <see cref="FragmentSize"/> and produces the smallest Datagram the wire carries, which is the
    /// low end of the gap a caller's size threshold sits in.
    /// </summary>
    public const int SmallMessageLength = 1;

    /// <summary>
    /// The payload length <see cref="SendLargeMessage"/> sends. Wider than <see cref="FragmentSize"/> by
    /// more than double, so it is always cut into exactly three Fragments (two full-size, one remainder)
    /// regardless of small rounding elsewhere.
    /// </summary>
    public const int LargeMessageLength = 1500;

    /// <summary>
    /// The payload length <see cref="SendSpanningMessage"/> sends: wide enough that its Fragment set
    /// cannot fit in one Datagram (issue #317). A Reactor packs as many of a Message's Fragments into
    /// one outbound Datagram as the 65 507-byte UDP maximum has room for, so a Message any smaller than
    /// this crosses the wire as a single Datagram and a network that loses that Datagram loses the whole
    /// set — all or nothing, which is issue #271's black hole and leaves nothing held to report against.
    /// A set spanning two Datagrams is the smallest shape a real path can leave partially held.
    /// At <see cref="FragmentSize"/> this is 110 Fragments, of which the first Datagram carries the 107
    /// that fit and the second carries the rest.
    /// </summary>
    public const int SpanningMessageLength = 66000;

    /// <summary>
    /// How many rounds <see cref="Establish"/> pumps before declaring the handshake a fixture failure.
    /// Three rounds is what the proxied handshake actually takes (see the type remarks), so this is
    /// overshoot by two orders of magnitude — overshooting costs nothing, because the loop returns the
    /// moment both ends are connected, while a budget that ever undershoots is the whole defect.
    /// </summary>
    public const int DefaultHandshakeRounds = 1000;

    /// <summary>How long a caller's <see cref="AdvanceClock"/> moves the shared clock by default, absent an override.</summary>
    public static readonly TimeSpan DefaultClockAdvance = TimeSpan.FromMilliseconds(50);

    // The wire-header arithmetic issue #307's wire-level assertion walks by hand — the same 20-byte
    // Datagram header, 7-byte unfragmented record header and 11-byte Fragment record header
    // DatagramSessionFragmentSizeUnviableTests already relies on (see that file's own remarks). Recorded
    // here rather than referenced from there because these are the internal protocol's own wire
    // constants, not a public surface either test file may reference across the seam.
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte ModeMask = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    // The two ushort fields a Fragment record header carries past the unfragmented shape, in the order
    // the wire carries them: the Fragment's index within its set, then the count the set declares.
    private const int FragmentIndexOffset = UnfragmentedRecordSize;
    private const int FragmentCountOffset = FragmentIndexOffset + sizeof(ushort);

    private readonly List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> _diallerEvents = [];
    private readonly List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> _accepterEvents = [];
    private readonly List<byte[]> _delivered = [];
    private readonly byte[] _smallPayload = Enumerable.Range(0, SmallMessageLength).Select(i => (byte)i).ToArray();
    private readonly byte[] _largePayload = Enumerable.Range(0, LargeMessageLength).Select(i => (byte)i).ToArray();
    private readonly byte[] _spanningPayload = Enumerable.Range(0, SpanningMessageLength).Select(i => (byte)i).ToArray();
    private readonly byte[] _keepAlivePayload = [0];
    private readonly int _wireBaselineCount;
    private int _orderedSequencesSent;

    /// <summary>
    /// Builds the pair, wires them through a fresh <see cref="LossyProxy"/> sharing this fixture's clock,
    /// lets <paramref name="configureProxyRule"/> install the one standing rule the caller's run needs,
    /// and pumps the handshake to completion before returning — a caller receives an established pair,
    /// never a fixture mid-handshake.
    /// </summary>
    /// <param name="configureProxyRule">
    /// Installs the proxy's one standing rule — a size threshold for the positive run, an every-Nth loss
    /// count for the negative one, or both, or neither. This fixture never chooses a rule itself: it hands
    /// the caller a live <see cref="LossyProxy"/> and gets out of the way, which is what keeps the same
    /// fixture usable for both runs #271 asks for. Invoked only after the handshake has already
    /// completed: the handshake's own control Datagrams are a fixed 256 bytes, unrelated to the fragment
    /// size a run's rule is scoped to reason about, so installing the rule any earlier would make an
    /// ordinary size threshold block the handshake itself rather than the Fragments it is meant to test.
    /// </param>
    /// <param name="fragmentSize">Overrides <see cref="FragmentSize"/> for both ends, if a run ever needs a different one.</param>
    /// <param name="maxMessageSize">Overrides <see cref="MaxMessageSize"/> for both ends.</param>
    /// <param name="retentionBytes">Overrides <see cref="RetentionBytes"/> for both ends.</param>
    /// <param name="handshakeRounds">
    /// How many rounds the handshake is allowed to settle within. Defaults to
    /// <see cref="DefaultHandshakeRounds"/>; a round budget rather than a real-time one because nothing
    /// on this path waits on a wall clock (see the type remarks).
    /// </param>
    /// <exception cref="InvalidOperationException">The handshake did not complete within <paramref name="handshakeRounds"/> — a fixture failure, not a scenario outcome.</exception>
    public FragmentSizeDiagnosisFixture(
        Action<LossyProxy> configureProxyRule,
        int fragmentSize = FragmentSize,
        int maxMessageSize = MaxMessageSize,
        int retentionBytes = RetentionBytes,
        int handshakeRounds = DefaultHandshakeRounds)
    {
        ArgumentNullException.ThrowIfNull(configureProxyRule);

        // The large Message is the only shape this fixture ever sends on the ReliableOrdered Space, so
        // its fragment count — derived from the same fragmentSize a caller may override — is what the
        // known sequence range AssertNoProbeWasObserved checks a Fragment record against is measured in.
        FragmentsPerLargeMessage = (LargeMessageLength + fragmentSize - 1) / fragmentSize;
        FragmentsPerSpanningMessage = (SpanningMessageLength + fragmentSize - 1) / fragmentSize;

        Clock = new FakeTimeProvider();
        Dialler = new DatagramSessionReactorHarness(
            bindLoopback: false,
            maxMessageSize: maxMessageSize,
            retentionBytes: retentionBytes,
            fragmentSize: fragmentSize,
            timeProvider: Clock);
        Accepter = new DatagramSessionReactorHarness(
            maxMessageSize: maxMessageSize,
            retentionBytes: retentionBytes,
            fragmentSize: fragmentSize,
            timeProvider: Clock);
        Proxy = new LossyProxy(Accepter.Endpoint, Clock);

        // The handshake's own control Datagrams are a fixed size (256 bytes) unrelated to the fragment
        // size a run's standing rule is scoped to reason about, so the rule is installed only once the
        // pair is already established — installing it first would make an ordinary size threshold below
        // that fixed size block the handshake itself, which is not a fragment-size scenario at all.
        DiallerPeer = Dialler.Reactor.Connect(Proxy.PublicEndPoint);
        AccepterPeer = Establish(handshakeRounds);

        // Everything the proxy captures from here on is this fixture's own scenario traffic — the
        // handshake's own fixed-size control Datagrams, captured above during Establish, are deliberately
        // excluded from AssertNoProbeWasObserved's walk (see the type remarks).
        _wireBaselineCount = Proxy.Captures.Count;

        configureProxyRule(Proxy);
    }

    /// <summary>The clock every Reactor and the proxy share — advancing it is the only way anything here measures time.</summary>
    public FakeTimeProvider Clock { get; }

    /// <summary>
    /// How many Fragments one <see cref="SendLargeMessage"/> is cut into, given the fragment size this
    /// fixture was constructed with. A caller reasoning about which sequences belong to which Fragment
    /// set derives them from this rather than restating three.
    /// </summary>
    public int FragmentsPerLargeMessage { get; }

    /// <summary>How many Fragments one <see cref="SendSpanningMessage"/> is cut into, given the fragment size this fixture was constructed with.</summary>
    public int FragmentsPerSpanningMessage { get; }

    /// <summary>
    /// The sequence the next Fragment this fixture sends on the <see cref="DeliveryMode.ReliableOrdered"/>
    /// Space will carry — equivalently, the first sequence of the next Fragment set, since the large and
    /// spanning Messages are the only things this fixture ever sends on that Space and each takes the
    /// sequences immediately after the last. A caller reads this before a Send to learn which set the
    /// Fragments about to go out belong to, rather than restating that arithmetic against a fragment
    /// size it may have overridden.
    /// </summary>
    public uint NextOrderedSequence => (uint)_orderedSequencesSent;

    /// <summary>The sending Reactor's harness. Its own Events and Diagnostics are exposed for the case where the dialler itself is what a run needs to inspect.</summary>
    public DatagramSessionReactorHarness Dialler { get; }

    /// <summary>The receiving Reactor's harness, on the far side of <see cref="Proxy"/>.</summary>
    public DatagramSessionReactorHarness Accepter { get; }

    /// <summary>The proxy between them, carrying whatever standing rule the constructor's <c>configureProxyRule</c> installed.</summary>
    public LossyProxy Proxy { get; }

    /// <summary>The Peer id the dialler addresses the accepter as.</summary>
    public PeerId DiallerPeer { get; }

    /// <summary>The Peer id the accepter addresses the dialler as.</summary>
    public PeerId AccepterPeer { get; }

    /// <summary>Every Event the dialler has drained across the whole run, in the order it was produced.</summary>
    public IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DiallerEvents => _diallerEvents;

    /// <summary>Every Event the accepter has drained across the whole run, in the order it was produced.</summary>
    public IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AccepterEvents => _accepterEvents;

    /// <summary>Every <see cref="DiagnosticRecord"/> the dialler's Sink has recorded across the whole run.</summary>
    public IReadOnlyList<DiagnosticRecord> DiallerDiagnostics => Dialler.Sink.Received;

    /// <summary>Every <see cref="DiagnosticRecord"/> the accepter's Sink has recorded across the whole run.</summary>
    public IReadOnlyList<DiagnosticRecord> AccepterDiagnostics => Accepter.Sink.Received;

    /// <summary>
    /// Every Message payload the accepter has delivered across the whole run, copied out of the frame
    /// arena that aliased them at delivery time so they outlive it — the dialler is the only sender in
    /// this fixture, so this is where a caller reads what actually got through.
    /// </summary>
    public IReadOnlyList<byte[]> DeliveredPayloads => _delivered;

    /// <summary>
    /// Sends <see cref="SmallMessageLength"/> bytes from the dialler to the accepter as
    /// <see cref="DeliveryMode.ReliableUnordered"/> — the Space the fragment-size stall signature needs
    /// still being acked while the other Space's Fragments are not (see the type remarks).
    /// </summary>
    public SendOutcome SendSmallMessage() => Dialler.Reactor.Send(DiallerPeer, DeliveryMode.ReliableUnordered, _smallPayload);

    /// <summary>
    /// Sends <see cref="LargeMessageLength"/> bytes from the dialler to the accepter as
    /// <see cref="DeliveryMode.ReliableOrdered"/>, cut into three Fragments by both ends' configured
    /// <see cref="FragmentSize"/>.
    /// </summary>
    /// <remarks>
    /// Callable more than once (issue #317): a second call queues a second Fragment set on the same
    /// Space, at the sequences immediately after the first set's, which is what makes two sets overlap
    /// in a receive ring narrow enough for the later one to lap onto the earlier one's slots. Every
    /// accepted call widens the sequence range <see cref="AssertNoProbeWasObserved"/> recognises as this
    /// fixture's own Fragment traffic by exactly <see cref="FragmentsPerLargeMessage"/>, so that check
    /// stays exactly as tight for a run sending two Messages as for one sending one.
    /// </remarks>
    public SendOutcome SendLargeMessage() => SendOrdered(_largePayload, FragmentsPerLargeMessage);

    /// <summary>
    /// Sends <see cref="SpanningMessageLength"/> bytes from the dialler to the accepter as
    /// <see cref="DeliveryMode.ReliableOrdered"/> — the same Space and shape as
    /// <see cref="SendLargeMessage"/>, but wide enough that its Fragment set crosses the wire as more
    /// than one Datagram (see <see cref="SpanningMessageLength"/>), which is what lets an ordinary
    /// whole-Datagram loss leave the set partially held rather than entirely missing.
    /// </summary>
    public SendOutcome SendSpanningMessage() => SendOrdered(_spanningPayload, FragmentsPerSpanningMessage);

    /// <summary>
    /// Installs a standing <see cref="LossyProxy.SetPayloadDropRule"/> on the dialler-to-accepter
    /// direction that drops every Datagram carrying a Fragment at one of <paramref name="sequences"/>,
    /// and forwards everything else untouched — with no sequences at all, it forwards everything, which
    /// is how a caller lifts a rule it installed earlier. Retransmissions included: the rule is a
    /// property of the path, so a Fragment named here never arrives however many times the dialler
    /// resends it, which is what keeps a Fragment set incomplete for a whole run rather than only until
    /// the first repair.
    /// </summary>
    /// <remarks>
    /// Whole Datagrams, never individual records: a Datagram carrying several Fragments is dropped
    /// entire if any one of them names a listed sequence, because that is the only loss a real path can
    /// inflict.
    /// </remarks>
    public void DropFragmentsAtSequences(params uint[] sequences)
    {
        ArgumentNullException.ThrowIfNull(sequences);
        HashSet<uint> dropped = [.. sequences];
        Proxy.SetPayloadDropRule(
            RelayDirection.ClientToUpstream,
            datagram => DecodeRecords(datagram).Any(record => record.IsFragment && dropped.Contains(record.Sequence)));
    }

    /// <summary>
    /// Sends one Message on the <see cref="DeliveryMode.ReliableOrdered"/> Space and, if it was
    /// accepted, advances <see cref="NextOrderedSequence"/> past the Fragments it will occupy — the one
    /// place that bookkeeping happens, so the two Message shapes cannot drift apart on it.
    /// </summary>
    private SendOutcome SendOrdered(byte[] payload, int fragments)
    {
        SendOutcome outcome = Dialler.Reactor.Send(DiallerPeer, DeliveryMode.ReliableOrdered, payload);
        if (outcome == SendOutcome.Sent)
        {
            _orderedSequencesSent += fragments;
        }

        return outcome;
    }

    /// <summary>
    /// Runs one round: the dialler, the proxy, the accepter's own keep-alive Send followed by its Pump,
    /// the proxy again — so a reply the accepter's Pump produced is relayed back within the same round —
    /// and then advances the shared clock by <paramref name="advance"/> (or
    /// <see cref="DefaultClockAdvance"/>). Advancing after the round, never before, matches
    /// <see cref="DatagramSessionFragmentSizeUnviableTests"/>'s own stepping: a timer due exactly at the
    /// new time fires on the Pump that follows the advance, not the one before it.
    /// </summary>
    /// <remarks>
    /// Not a plain reuse of <see cref="DatagramSessionReactorHarness.PumpRoundThroughProxy"/>: an
    /// acknowledgement only leaves a Peer piggybacked on that Peer's own next outbound Datagram
    /// (<c>FlushOutboundDatagrams</c> reads the fresh ack blocks, but only for a Peer with something
    /// queued to flush), and this fixture's accepter otherwise never calls <c>Send</c> at all — it only
    /// receives. Without a Send of its own queued before its Pump, the accepter would never acknowledge
    /// anything, the dialler's retention for whatever it sent would never free, and the run would stall on
    /// retention rather than the fragment-size signature this fixture exists to reach. The keep-alive is
    /// <see cref="DeliveryMode.Unreliable"/> and one byte so it never itself occupies a retention slot or
    /// competes with the two Messages a caller actually cares about.
    /// </remarks>
    public void PumpRound(TimeSpan? advance = null)
    {
        Dialler.Reactor.Pump();
        Proxy.Pump();
        Accepter.Reactor.Send(AccepterPeer, DeliveryMode.Unreliable, _keepAlivePayload);
        Accepter.Reactor.Pump();
        Proxy.Pump();
        Collect();
        Clock.Advance(advance ?? DefaultClockAdvance);
    }

    /// <summary>Runs exactly <paramref name="rounds"/> rounds (see <see cref="PumpRound"/>), for driving a fixed amount of scenario time without waiting on any outcome.</summary>
    public void PumpRounds(int rounds, TimeSpan? advancePerRound = null)
    {
        for (int round = 0; round < rounds; round++)
        {
            PumpRound(advancePerRound);
        }
    }

    /// <summary>
    /// Runs rounds until <paramref name="condition"/> holds or <paramref name="maxRounds"/> is spent,
    /// whichever comes first, returning whether it held. This is the fixture's only conditional loop, and
    /// it is bounded by a round count rather than a deadline on purpose: every one of this scenario's own
    /// timings — RTO expiry, the four stall rounds, the proxy's every-Nth count — advances with
    /// <paramref name="advancePerRound"/>, so a round budget bounds scenario time directly instead of
    /// bounding wall-clock time and hoping it corresponds. A predicate that never holds exhausts the
    /// budget and returns <see langword="false"/> rather than hanging — the fixture asserts nothing, so a
    /// caller decides what a spent budget means.
    /// </summary>
    public bool PumpRoundsUntil(Func<bool> condition, int maxRounds, TimeSpan? advancePerRound = null)
    {
        ArgumentNullException.ThrowIfNull(condition);

        for (int round = 0; round < maxRounds; round++)
        {
            PumpRound(advancePerRound);
            if (condition())
            {
                return true;
            }
        }

        return condition();
    }

    /// <summary>Drains both ends once, appending everything produced to the accumulated Events and delivered payloads.</summary>
    private void Collect()
    {
        _diallerEvents.AddRange(Dialler.DrainToList());
        _accepterEvents.AddRange(Accepter.DrainToList());
        _delivered.AddRange(Accepter.DrainPayloads());
    }

    /// <summary>
    /// Pumps the round through the proxy until both ends have raised <see cref="SessionEventKind.PeerConnected"/>,
    /// collecting every Event produced along the way rather than discarding the handshake's own Events —
    /// unlike <see cref="DatagramSessionReactorHarness.PumpThroughProxyUntilConnected"/>, which this
    /// fixture cannot reuse here for exactly that reason: it drains internally and returns only the
    /// established <see cref="PeerId"/>, so the handshake's Events would never reach
    /// <see cref="DiallerEvents"/> or <see cref="AccepterEvents"/>.
    /// </summary>
    private PeerId Establish(int rounds)
    {
        PeerId? accepterPeer = null;
        bool diallerConnected = false;

        for (int round = 0; round < rounds; round++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughProxy(Dialler, Proxy, Accepter);
            int diallerEventsBefore = _diallerEvents.Count;
            int accepterEventsBefore = _accepterEvents.Count;
            Collect();

            for (int i = accepterEventsBefore; i < _accepterEvents.Count; i++)
            {
                if (_accepterEvents[i].Kind == SessionEventKind.PeerConnected)
                {
                    accepterPeer = _accepterEvents[i].Peer;
                }
            }

            for (int i = diallerEventsBefore; i < _diallerEvents.Count; i++)
            {
                diallerConnected |= _diallerEvents[i].Kind == SessionEventKind.PeerConnected;
            }

            if (diallerConnected && accepterPeer is PeerId established)
            {
                return established;
            }
        }

        throw new InvalidOperationException(
            $"The proxied handshake did not complete within {rounds} rounds.");
    }

    /// <summary>
    /// Issue #307's first shared assertion: across every datagram this fixture's proxy has observed
    /// since the handshake completed, every record decodes as one of the three shapes this fixture's own
    /// scenario ever produces, travelling only the one direction that shape can legitimately come from —
    /// a Fragment of the large Message or the small Message's own record
    /// (<see cref="DeliveryMode.ReliableOrdered"/> with sequence within the large Message's own fragment
    /// count, or <see cref="DeliveryMode.ReliableUnordered"/> at exactly <see cref="SmallMessageLength"/>
    /// byte, both only ever sent by the dialler, <see cref="RelayDirection.ClientToUpstream"/>), or the
    /// accepter's keep-alive (<see cref="DeliveryMode.Unreliable"/>, exactly one byte, only ever sent by
    /// the accepter, <see cref="RelayDirection.UpstreamToClient"/>) — or carries no record at all, which
    /// is accepted only if the whole datagram is exactly <see cref="DatagramHeaderSize"/> bytes: a bare
    /// ack or heartbeat legitimately carries no record, but a record-less datagram with anything trailing
    /// its header would otherwise pass this check unconditionally, since a datagram that decodes to no
    /// records never enters the loop below at all.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>What this catches.</b> A probe — the thing the "we do not probe" design decision forbids — is
    /// exactly a datagram this enumeration cannot place: an extra control-shaped message, a record naming
    /// a Space or sequence outside the two Messages this fixture ever sends, a record of an unexpected
    /// length (the same framing <see cref="DatagramSessionFragmentSizeUnviableTests"/> uses for its own
    /// crafted scenario), a known record shape travelling the wrong direction (a keep-alive-shaped record
    /// arriving from the dialler, or a Fragment or small-Message-shaped record arriving from the
    /// accepter), or a record-less datagram carrying a payload past the Datagram header. Checked at the
    /// wire, on what the proxy actually observed arriving, rather than at an internal flag — a flag can be
    /// right while the datagram still leaves.
    /// </para>
    /// <para>
    /// <b>What this does not catch.</b> Volume is deliberately out of scope: this check never bounds how
    /// many datagrams of a known shape appear, because retransmission legitimately repeats the Fragment
    /// and small-Message shapes, and a count-based assertion would be flaky against exactly the retries
    /// this fixture's own scenarios rely on. It also cannot catch a probe that reuses a known shape
    /// exactly — a same-direction record with a mode, sequence and length matching one of the three known
    /// shapes would pass, whatever bytes it actually carries as payload, because this walk reads only a
    /// record's header fields, never its payload content.
    /// </para>
    /// </remarks>
    public void AssertNoProbeWasObserved()
    {
        foreach (RelayCapture capture in Proxy.Captures.Skip(_wireBaselineCount))
        {
            bool sawAnyRecord = false;

            foreach ((byte mode, uint sequence, int length, bool _, ushort _, ushort _) in DecodeRecords(capture.Payload))
            {
                sawAnyRecord = true;

                bool isKnownFragment = mode == (byte)DeliveryMode.ReliableOrdered
                    && sequence < NextOrderedSequence
                    && capture.Direction == RelayDirection.ClientToUpstream;
                bool isKnownSmallMessage = mode == (byte)DeliveryMode.ReliableUnordered
                    && length == SmallMessageLength
                    && capture.Direction == RelayDirection.ClientToUpstream;
                bool isKnownKeepAlive = mode == (byte)DeliveryMode.Unreliable
                    && length == _keepAlivePayload.Length
                    && capture.Direction == RelayDirection.UpstreamToClient;

                (isKnownFragment || isKnownSmallMessage || isKnownKeepAlive).Should().BeTrue(
                    $"a record with mode {mode}, sequence {sequence} and length {length}, captured travelling {capture.Direction}, " +
                    "is not the large Message's own Fragment shape or the small Message's own shape travelling from the dialler, " +
                    "or the accepter's keep-alive travelling back — a probe would show up as exactly this kind of unaccounted-for " +
                    "or wrong-direction record on the wire");
            }

            if (!sawAnyRecord)
            {
                capture.Payload.Length.Should().Be(
                    DatagramHeaderSize,
                    $"a record-less datagram captured travelling {capture.Direction} is only a legitimate bare ack or heartbeat " +
                    "if it is nothing more than the Datagram header itself — a probe carrying a payload but no record would " +
                    "otherwise never enter the record walk above and would slip past this check unconditionally");
            }
        }
    }

    /// <summary>
    /// Issue #307's second shared assertion: both ends' configured fragment size, read back off the same
    /// <see cref="DatagramSessionOptions"/> instance each harness was constructed with
    /// (<see cref="DatagramSessionOptions.ResolvedFragmentSize"/> via
    /// <see cref="DatagramSessionReactorHarness.Options"/>), is exactly <paramref name="expectedFragmentSize"/>
    /// — the value this fixture was constructed with. This is the observable form of "we diagnose, we do
    /// not recover": the positive run's diagnosis names the fragment size as the cause without ever
    /// rewriting it, and the negative run's ordinary loss recovers the Message without touching it either.
    /// </summary>
    /// <remarks>
    /// <b>What this can and cannot detect.</b> <see cref="DatagramSessionReactor"/> exposes no public
    /// member of its own that reports a Peer's or its own configured fragment size back — its public
    /// surface is limited to <see cref="DatagramSessionReactor.BoundEndpoints"/>,
    /// <see cref="DatagramSessionReactor.Counters"/>, <c>Pump</c>, <c>Drain</c>, <c>Connect</c>,
    /// <c>Close</c>, <c>CountersOf</c>, <c>Send</c> and <c>Dispose</c>; none of them surface
    /// <see cref="DatagramSessionOptions.ResolvedFragmentSize"/> or anything derived from it. This
    /// assertion therefore reads the value off the very <see cref="DatagramSessionOptions"/> object each
    /// harness constructed and handed to the Reactor at construction — the same object a real consumer
    /// would have retained their own reference to, since the Reactor hands nothing back. It can catch the
    /// Reactor mutating that shared object in place, which is the failure mode "we do not recover" is
    /// meant to rule out if recovery were ever implemented by rewriting the options a Peer already holds
    /// a reference to. It cannot catch a hypothetical fragment size the Reactor tracked internally,
    /// separately from this object, and changed without ever touching it: there is no public surface to
    /// observe that through, so this assertion cannot be tightened past the options read-back without a
    /// change to <see cref="DatagramSessionReactor"/>'s own public surface, which is out of scope for this
    /// test-only fixture.
    /// </remarks>
    public void AssertConfiguredFragmentSizeUnchanged(int expectedFragmentSize)
    {
        Dialler.Options.ResolvedFragmentSize.Should().Be(
            expectedFragmentSize,
            "the dialler's configured fragment size must be exactly what this fixture was constructed with — a diagnosis that named this setting as the cause must never have rewritten it");
        Accepter.Options.ResolvedFragmentSize.Should().Be(
            expectedFragmentSize,
            "the accepter's configured fragment size must likewise be unchanged — the diagnosis is scoped to the stalled Peer's own retransmissions, never to either end's own setting");
    }

    /// <summary>
    /// Walks every packet record in <paramref name="datagram"/> by hand, returning each one's mode bits,
    /// sequence and declared payload length — the same fragment-aware walk
    /// <see cref="DatagramSessionFragmentSizeUnviableTests"/> uses (see its own remarks), reimplemented
    /// here because that walk is private to a different class. <see cref="DatagramDecoder"/> cannot be
    /// reused for this instead: it assumes every record uses the unfragmented, 7-byte header shape, so it
    /// cannot walk a datagram carrying an 11-byte Fragment record header without misreading everything
    /// after it.
    /// </summary>
    private static IEnumerable<(byte Mode, uint Sequence, int Length, bool IsFragment, ushort FragmentIndex, ushort FragmentCount)> DecodeRecords(byte[] datagram)
    {
        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;
            int headerSize = isFragment ? FragmentRecordSize : UnfragmentedRecordSize;
            ushort fragmentIndex = isFragment ? BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + FragmentIndexOffset, 2)) : (ushort)0;
            ushort fragmentCount = isFragment ? BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + FragmentCountOffset, 2)) : (ushort)0;

            yield return ((byte)(flags & ModeMask), sequence, length, isFragment, fragmentIndex, fragmentCount);

            offset += headerSize + length;
        }
    }

    public void Dispose()
    {
        Dialler.Dispose();
        Accepter.Dispose();
        Proxy.Dispose();
        GC.SuppressFinalize(this);
    }
}
