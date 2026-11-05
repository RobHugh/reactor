using System.Net;
using System.Security.Cryptography;

namespace ReactorSockets;

/// <summary>
/// The construction surface of a <see cref="DatagramSessionReactor"/> (ADR 0012): the shared L0
/// budget composed in as <see cref="Reactor"/>, plus the knobs this profile alone has. Composed
/// rather than inherited, so that nothing protocol-specific here can ever leak into
/// <see cref="ReactorOptions"/> and turn it into the base class ADR 0011 refused.
/// </summary>
/// <remarks>
/// <para>
/// A knob carries a default only when that default is <em>derived</em> — from machinery this library
/// owns, from another knob, or from a citable standard. A value that trades the consumer's own
/// resources against their own traffic, and that nothing in this library can derive, is required with
/// no default: <see cref="MaxMessageSize"/>, <see cref="RetentionBytes"/>,
/// <see cref="SendIdleInterval"/> and <see cref="Linger"/> are all of that kind, and so is
/// <see cref="ReactorOptions.Capacity"/>. Guessing any of them would be guessing at the consumer's
/// application.
/// </para>
/// <para>
/// Values fixed by the wire format are not here at all and cannot be reached from here: the 32-bit
/// acknowledgement width, the 64-entry duplicate window, the four sequence Spaces, zero as the
/// reserved session id, the 20-byte data header and the thirty-second cookie epoch are protocol
/// constants. Changing one changes what a datagram means, which is a version, not a setting.
/// </para>
/// <para>
/// <b>The memory bill.</b> A Reactor built from these options preallocates, and the whole of it is
/// these two lines:
/// </para>
/// <list type="bullet">
/// <item><description>
/// receive: <c>Capacity × 2 × ReceiveWindowFragments × FragmentSize</c> — two of the four Spaces are
/// reliable and reassemble, and each holds a window of whole fragment slots per Peer.
/// </description></item>
/// <item><description>
/// send: <c>Capacity × 2 × RetentionBytes</c> — the same two Spaces retain what they have sent until
/// it is acknowledged, per Peer.
/// </description></item>
/// </list>
/// <para>
/// <c>FragmentSize</c> is in one line and not the other because the window counts packets and retention
/// counts bytes. Neither line has a traffic term: both are allocated at construction and never grow.
/// Each line also carries a few bytes of bookkeeping per fragment slot, which
/// <see cref="ReceiveBudgetBytes"/> and <see cref="SendBudgetBytes"/> include exactly — under 2% at the
/// default fragment size.
/// </para>
/// <para>
/// The frame arena (<see cref="ReactorOptions.ArenaSize"/>) is over and above those two lines and is
/// shared across the whole Reactor rather than multiplied by capacity.
/// </para>
/// <para>
/// One further cost is allocated only under traffic: ordinary <c>ReliableOrdered</c> records waiting
/// behind a gap. It has a ceiling of its own, <see cref="HeldRecordBudgetBytes"/>, and reaching it refuses
/// further records rather than growing.
/// </para>
/// <para>
/// <see cref="MaxMessageSize"/> reaches three places at once: it is announced to the remote as a
/// receive limit, it floors the receive window at the fragments one maximum Message occupies
/// (<see cref="ReceiveWindowFloor"/>), and it floors the frame arena, because an arena that cannot
/// hold one maximum Message could never deliver one. The limit is enforced as well as announced: a
/// completed Message above it is discarded rather than delivered, with
/// <see cref="DiagnosticId.OversizedMessage"/>. Careless enlargement grows memory in
/// two directions rather than one: the window floor is per Peer, so the receive line above rises
/// with it multiplied by capacity and by the Spaces, and the shared arena is pushed up alongside.
/// Retention is the exception this profile makes. <see cref="RetentionBytes"/> is floored by
/// <see cref="ResolvedFragmentSize"/> rather than by <see cref="MaxMessageSize"/>, because a
/// reliable Space retransmits fragment by fragment and needs only one fragment to have something to
/// resend.
/// </para>
/// <para>
/// <b>Security.</b> The session field on every datagram is a demultiplexing token, not an
/// authenticator: it makes blind off-path injection a 32-bit guess, and does nothing against anyone who
/// has seen one datagram of the Session. This library does not encrypt, and on-path attackers are out
/// of scope (ADR 0009). A consumer who needs confidentiality or integrity against an on-path attacker
/// provides it themselves, inside the Message.
/// </para>
/// <para>
/// Every check below is deterministic and entirely the caller's, so failures throw at construction —
/// <see cref="ArgumentOutOfRangeException"/> for a single bad value and
/// <see cref="ArgumentException"/> for a combination that cannot work — rather than being reported
/// through a <c>Result</c>.
/// </para>
/// </remarks>
public sealed class DatagramSessionOptions
{
    /// <summary>
    /// The smallest fragment size this profile will accept, in bytes: RFC 791's guaranteed minimum
    /// reassembly buffer. Below this, there is no standard that says a datagram of that size can be
    /// carried at all.
    /// </summary>
    private const int MinimumFragmentSize = 576;

    /// <summary>
    /// The largest fragment size this profile will accept, in bytes: the maximum UDP payload, being
    /// 65 535 less the 20-byte IPv4 header and the 8-byte UDP header. Above this there is no datagram
    /// to put a fragment in.
    /// </summary>
    private const int MaximumFragmentSize = 65_507;

    /// <summary>
    /// The default fragment size, in bytes: QUIC's own floor, being the 1280-byte IPv6 minimum MTU
    /// less the largest plausible header stack. This is the one fragment size with citable provenance
    /// for "will get through an arbitrary internet path", which is why it is the only one that can be
    /// a default rather than a guess.
    /// </summary>
    private const int DefaultFragmentSize = 1200;

    /// <summary>
    /// The default number of consecutive missed heartbeats before a Peer is declared gone: RFC 4271's
    /// hold time, three times the keepalive interval. Two would make a single lost heartbeat fatal;
    /// three is the smallest count that survives one loss, and is the count a standard already
    /// defends.
    /// </summary>
    private const int DefaultMissedHeartbeats = 3;

    /// <summary>
    /// The number of Spaces that reassemble on receive and retain on send — the reliable pair of the
    /// four. Internal rather than private so <see cref="DatagramSessionReactor"/> can size its receive
    /// arena (issue #171) against the same constant this budget is computed from, rather than a second
    /// literal 2 that could drift from it.
    /// </summary>
    internal const int ReliableSpaceCount = 2;

    /// <summary>
    /// The number of Spaces a Peer classifies arrivals against — all four of ADR 0009's delivery
    /// modes, reliable and unreliable alike. Every one of them owns a receive window and so a run of
    /// admission bytes in the receive arena, which is why the receive budget multiplies by this as
    /// well as by <see cref="ReliableSpaceCount"/> (issue #174).
    /// </summary>
    internal const int SpaceCount = 4;

    /// <summary>
    /// The bytes one receive-arena length slot occupies for a retaining Space (issue #257): a
    /// <c>u16</c> recording how many payload bytes the Fragment held in a slot actually occupies, wide
    /// enough for any <see cref="ResolvedFragmentSize"/> up to the largest a UDP payload can carry.
    /// Internal so <see cref="Core.ReceiveArena"/> sizes its length region against the same constant
    /// this budget is computed from, rather than a second literal 2 that could drift from it.
    /// </summary>
    internal const int FragmentLengthBytes = 2;

    /// <summary>
    /// Bytes one waiting-Fragment-group entry occupies in the receive arena: a <c>u32</c> first
    /// sequence and a <c>u16</c> declared Fragment count, the pair that names a group (issue #259).
    /// </summary>
    internal const int PendingGroupEntryBytes = 6;

    /// <summary>
    /// Bytes one held-Fragment's group-identity entry occupies in the receive arena: the same <c>u32</c>
    /// first sequence and <c>u16</c> declared Fragment count as <see cref="PendingGroupEntryBytes"/>, but
    /// one entry per receive-window slot rather than per waiting group — what still names an incomplete
    /// set's held and declared counts at the moment its slot is discarded and its length residue is
    /// cleared (issue #297).
    /// </summary>
    internal const int FragmentGroupEntryBytes = 6;

    /// <summary>
    /// The most one held ordinary record costs beside its payload on a 64-bit runtime (issue #424): an
    /// 8-byte reference and a 4-byte length in the send-order ring, and a byte array's 24-byte object
    /// header with up to 8 bytes of padding.
    /// </summary>
    internal const int HeldRecordSlotBookkeepingBytes = 8 + 4 + 24 + 8;

    /// <summary>
    /// The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
    /// Required — a Reactor cannot be built without one, and its <see cref="ReactorOptions.Capacity"/>
    /// has no default of its own.
    /// </summary>
    public required ReactorOptions Reactor { get; init; }

    /// <summary>
    /// The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
    /// statement about the consumer's own protocol that nothing here can derive. It is also the knob
    /// that multiplies this profile's memory bill in more than one direction — the class remarks on
    /// <see cref="DatagramSessionOptions"/> say what it floors and what raising it costs.
    /// </summary>
    /// <remarks>
    /// Announced to the peer at the handshake precisely so an oversized claim can be refused before
    /// this end holds anything for it: a declared Fragment <c>fragmentCount</c> larger than this
    /// Reactor's receive window (<see cref="ResolvedReceiveWindowFragments"/>) is closed on its first
    /// Fragment, before any receive-window slot or Fragment-group bookkeeping exists for it (issue
    /// #282, reworked under #264). The bound is the window rather than <see cref="ReceiveWindowFloor"/>
    /// alone, because the floor is computed from this end's own <see cref="ResolvedFragmentSize"/> and
    /// fragment size is unilateral and per-direction (ADR 0004): a conforming peer with a smaller
    /// fragment size legally cuts the same, within-limit Message into more Fragments than the floor.
    /// <see cref="Validate"/> already requires the window to be at least the floor, so a set at exactly
    /// this value is always admitted.
    /// </remarks>
    public required int MaxMessageSize { get; init; }

    /// <summary>
    /// The bytes each reliable Space retains per Peer for retransmission, until what it holds is
    /// acknowledged. Required: this is the consumer trading their memory against their tolerance for
    /// stalling on loss, and the right answer depends on their traffic, not on anything here.
    /// </summary>
    public required int RetentionBytes { get; init; }

    /// <summary>
    /// The most accepted Peers one address may hold at once (issue #427), so one reachable address
    /// cannot occupy every capacity Slot. Required: how many Peers legitimately share an address depends on
    /// the NATs and proxies in front of this Reactor, which nothing here can derive. At least 1. A value of
    /// <see cref="ReactorOptions.Capacity"/> or above sets no limit.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The count is by address, without the port: a host can answer a Cookie from as many ports as it can
    /// bind, so a limit that counted ports would limit nothing. The address family and IPv6 scope stay in
    /// the key, because they distinguish hosts (ADR 0014). An IPv6 host holding a whole prefix is as many
    /// addresses as it can bind, and this limit does not group them.
    /// </para>
    /// <para>
    /// A Peer is charged to the address its handshake came from for as long as it holds its Slot, including
    /// while Interrupted or Closing and after a Resume moves it to another address. A dialled Peer is not
    /// charged: this Reactor chose to make it. A handshake over the limit is refused the same way as one at
    /// a full Reactor, and reported with <see cref="DiagnosticId.AddressPeerLimitReached"/>.
    /// </para>
    /// </remarks>
    public required int MaxPeersPerAddress { get; init; }

    /// <summary>
    /// How long a Session may go without sending before it sends a Ping instead (issue #340). Required:
    /// shorter costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the
    /// consumer knows which they would rather spend. There is no option to disable this: every
    /// Established Peer that has sent no data datagram for this long is always sent one seven-byte
    /// control record, whatever else it did or did not receive. At most 2^32 - 1 milliseconds (about 49.7
    /// days), the most a handshake may announce.
    /// </summary>
    /// <remarks>
    /// The Ping this produces is a Heartbeat, and a Heartbeat is not a keepalive: it proves the party
    /// behind this Session is still there — able to receive and eventually acknowledge application data
    /// — not merely that some NAT mapping between here and there is still open, which a bare keepalive a
    /// middlebox itself could answer would not prove. The receive side is derived, not configured: a
    /// Peer is declared gone after the remote's announced interval times <see cref="MissedHeartbeats"/>
    /// of silence (ADR 0012).
    /// </remarks>
    public required TimeSpan SendIdleInterval { get; init; }

    /// <summary>
    /// How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
    /// flight. Required, for the same reason as <see cref="SendIdleInterval"/>: it trades a prompt
    /// teardown against a clean one. At most 2^32 - 1 milliseconds (about 49.7 days), the most a handshake
    /// may announce.
    /// </summary>
    public required TimeSpan Linger { get; init; }

    /// <summary>
    /// The endpoints to bind at construction, in the order supplied; possibly empty. Declared once
    /// and never rebound (ADR 0014) — a Session's addresses are part of what it is, not something it
    /// acquires later. An empty set binds nothing until the first dial, which binds a single
    /// wildcard, port-zero socket in the destination's address family. Provenance: empty is the
    /// absence of a declaration, not a chosen address — the dial-only socket's family and port are
    /// derived from the destination and the OS, never configured.
    /// </summary>
    public IReadOnlyList<IPEndPoint> Endpoints { get; init; } = [];

    /// <summary>
    /// The bytes a fragment carries, and therefore the unit every buffer here is measured in.
    /// Defaults to 1200. Provenance: QUIC's floor beneath the 1280-byte IPv6 minimum MTU (RFC 9000),
    /// the one value with citable provenance for surviving an arbitrary path. Must lie between 576
    /// (RFC 791's guaranteed reassembly minimum) and 65 507 (the maximum UDP payload).
    /// </summary>
    /// <remarks>
    /// Unilateral and per-direction (ADR 0004): each side of a Session cuts its own outbound Messages
    /// by its own configured value, and the two directions need not agree. It is never announced in
    /// the handshake, because a receiver reassembles a Fragment set from <c>fragmentIndex</c> and
    /// <c>fragmentCount</c> alone and never needs to know the sender's choice — so two Sessions
    /// configured with different values still interoperate, each cutting by its own. This value is
    /// resolved once per Peer, at admission, onto that Peer's own slot; a later change to this options
    /// instance never reaches a Peer already open. See <see cref="ResolvedFragmentSize"/>.
    /// </remarks>
    public int? FragmentSize { get; init; }

    /// <summary>
    /// The fragments each reliable Space may have outstanding per Peer. Defaults to exactly the
    /// number a maximum Message occupies. Provenance: derived from <see cref="MaxMessageSize"/> and
    /// <see cref="FragmentSize"/> — ADR 0004's deadlock floor rather than a performance choice: a window narrower than one whole Message can never deliver one, so it can
    /// never advance, so the sender can never send the rest. This resolved value is also the inbound
    /// bound a declared Fragment <c>fragmentCount</c> is checked against (issue #282, under #264): a set
    /// larger than the window can never be reassembled, since there is no slot to hold every one of
    /// its pieces at once, so <see cref="DatagramSessionReactor"/> closes it before any is held.
    /// </summary>
    public int? ReceiveWindowFragments { get; init; }

    /// <summary>
    /// How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
    /// Defaults to 3. Provenance: RFC 4271's hold time, three times the keepalive interval. Must be at
    /// least 2, because a count of 1 makes a single lost datagram fatal and no network is that reliable.
    /// </summary>
    public int? MissedHeartbeats { get; init; }

    /// <summary>
    /// The most datagrams one Pump reads from each bound socket (issue #428), so a flood cannot keep a
    /// Pump from returning. Defaults to <see cref="ResolvedMaxDatagramsPerPump"/>. Provenance: derived from
    /// a full Reactor's worth of traffic, as <see cref="ReactorOptions.AdmissionCap"/> is (ADR 0012). At
    /// least 1.
    /// </summary>
    /// <remarks>
    /// What this leaves unread waits in the socket's receive buffer for a later Pump. If that buffer
    /// fills, the operating system drops what arrives next and no counter here sees it. Lower this to
    /// bound how long one Pump can take, at the cost of reading a flood more slowly. A Pump that reaches it
    /// reports <see cref="DiagnosticId.DatagramBudgetReached"/>.
    /// </remarks>
    public int? MaxDatagramsPerPump { get; init; }

    /// <summary>
    /// The entropy the cookie secrets, session ids and resume secrets are drawn from. A construction
    /// knob in ADR 0012's sense — the consumer's own resource, and a consumer with a hardware source
    /// or a policy about which generator may be used is entitled to supply it. It carries a default
    /// only because that default is derived rather than guessed. Provenance: the platform's own CSPRNG.
    /// </summary>
    /// <remarks>
    /// Nothing disposes this, and nothing should. A supplied generator belongs to the consumer, who
    /// may hand the same one to several Reactors; a Reactor that disposed it would be disposing a
    /// resource it does not own. The default instance is not a leak either — on .NET the generator
    /// <see cref="RandomNumberGenerator.Create()"/> returns holds no handle, drawing from the OS on
    /// each call, so its <c>Dispose</c> has nothing to release.
    /// </remarks>
    public RandomNumberGenerator Entropy { get; init; } = RandomNumberGenerator.Create();

    /// <summary>
    /// The resolved fragment size: <see cref="FragmentSize"/> if set, otherwise 1200.
    /// </summary>
    /// <remarks>
    /// A Peer captures this value once, onto its own slot, at admission (issue #248) — every
    /// <see cref="DatagramSessionReactor.Send"/> call for that Peer, and every retransmit, cuts by that
    /// captured value rather than reading this property afresh, so fragment size cannot move mid-Session
    /// even if this options instance later would resolve differently (ADR 0004).
    /// </remarks>
    public int ResolvedFragmentSize => FragmentSize ?? DefaultFragmentSize;

    /// <summary>
    /// The resolved receive window: <see cref="ReceiveWindowFragments"/> if set, otherwise the
    /// fragments one maximum Message occupies.
    /// </summary>
    public int ReceiveWindowFloor =>
        (MaxMessageSize + ResolvedFragmentSize - 1) / ResolvedFragmentSize;

    /// <summary>The resolved receive window: <see cref="ReceiveWindowFragments"/> if set, otherwise <see cref="ReceiveWindowFloor"/>.</summary>
    public int ResolvedReceiveWindowFragments => ReceiveWindowFragments ?? ReceiveWindowFloor;

    /// <summary>
    /// The resolved per-socket read limit: <see cref="MaxDatagramsPerPump"/> if set, otherwise one receive
    /// window for every Space of every Peer, plus every handshake the admission cap allows in one Pump.
    /// </summary>
    public int ResolvedMaxDatagramsPerPump =>
        MaxDatagramsPerPump ?? (int)Math.Min(
            ((long)Reactor.Capacity * ResolvedReceiveWindowFragments * SpaceCount) + Reactor.ResolvedAdmissionCap,
            int.MaxValue);

    /// <summary>The resolved missed-heartbeat count: <see cref="MissedHeartbeats"/> if set, otherwise 3.</summary>
    public int ResolvedMissedHeartbeats => MissedHeartbeats ?? DefaultMissedHeartbeats;

    /// <summary>
    /// The receive line of the memory bill in bytes, as a <see langword="long"/> so that a
    /// combination too large to allocate is a value this can report rather than an overflow it
    /// silently wraps through.
    /// </summary>
    /// <remarks>
    /// Four things per Peer scale with the window, and this figure covers all four (issue #174, #257,
    /// #259, #297). Payload slots — <see cref="ResolvedFragmentSize"/> bytes each — exist only for the
    /// <see cref="ReliableSpaceCount"/> Spaces that hold fragments until a Message is whole. The
    /// one-byte-per-slot admission record a receive window classifies against exists for all
    /// <see cref="SpaceCount"/> Spaces, because all four classify whether or not they retain.
    /// <see cref="FragmentLengthBytes"/> per slot records how many bytes of a held Fragment's payload
    /// are actually occupied, and <see cref="FragmentGroupEntryBytes"/> per slot names the group that
    /// payload belongs to — both exist only for the same <see cref="ReliableSpaceCount"/> Spaces as the
    /// payload they describe, and both are the receive window's own reassembly bookkeeping (issue #257,
    /// #297), not a separate buffer: the length says a piece was held, the group-identity entry is what
    /// still lets an abandoned set's held and declared counts be read once that slot is reused. The
    /// six-byte waiting-group entry — <see cref="PendingGroupEntryBytes"/> — exists for the same two
    /// Spaces too, but only half a window's worth each (<see cref="PendingGroupsPerSpace"/>, issue
    /// #259), since a completed-but-undelivered group holds at least two Fragments. This figure is an
    /// exact statement of what the receive arena takes at construction. Ordinary records held behind a
    /// gap are the one receive-side cost outside it, and they have their own ceiling,
    /// <see cref="HeldRecordBudgetBytes"/>.
    /// </remarks>
    public long ReceiveBudgetBytes =>
        ((long)Reactor.Capacity * ResolvedReceiveWindowFragments
            * (((long)ReliableSpaceCount * (ResolvedFragmentSize + FragmentLengthBytes + FragmentGroupEntryBytes)) + SpaceCount))
        + ((long)Reactor.Capacity * ReliableSpaceCount * PendingGroupsPerSpace * PendingGroupEntryBytes);

    /// <summary>
    /// The completed Fragment groups one reliable Space can have waiting for delivery at once: half
    /// <see cref="ResolvedReceiveWindowFragments"/>, never fewer than one. A group is at least two
    /// Fragments wide and holds its slots until it is delivered, so the window's own size is what
    /// bounds this — see <see cref="Session.PendingFragmentGroups"/>.
    /// </summary>
    internal int PendingGroupsPerSpace =>
        ResolvedReceiveWindowFragments / 2 < 1 ? 1 : ResolvedReceiveWindowFragments / 2;

    /// <summary>
    /// How many sequences from its send-order head one <c>ReliableOrdered</c> Space will hold ordinary
    /// records for (issue #424): twice <see cref="ResolvedReceiveWindowFragments"/>. Honest traffic
    /// reaches no further while a Fragment group holds the head, because a group spans at most one window
    /// and the receive window then spans one more from the group's last admitted piece.
    /// </summary>
    internal int OrderedHoldSlots => 2 * ResolvedReceiveWindowFragments;

    /// <summary>
    /// The most heap, in bytes, a Reactor's <c>ReliableOrdered</c> Spaces can hold in ordinary records
    /// waiting behind a gap, across all its Peers (issue #424): <c>Capacity × 2 × ReceiveWindowFragments
    /// × FragmentSize</c>, with each slot's bookkeeping.
    /// </summary>
    /// <remarks>
    /// This is a ceiling reached only under traffic, not a line allocated at construction, so
    /// <see cref="ValidateMemoryBill"/> does not count it. It is over and above
    /// <see cref="ReceiveBudgetBytes"/> and about the same size. A record past either limit is left
    /// unacknowledged and counted on <see cref="PeerCounters.OutOfWindowPackets"/>, so the far end's
    /// retransmission brings it back when the Space has moved. The figure counts what stays held, not
    /// the short-lived arrays the ring leaves behind each time it grows.
    /// </remarks>
    public long HeldRecordBudgetBytes =>
        (long)Reactor.Capacity * OrderedHoldSlots * (ResolvedFragmentSize + HeldRecordSlotBookkeepingBytes);

    /// <summary>
    /// The send line of the memory bill in bytes, reported the same way: <see cref="RetentionSlots"/>
    /// whole fragment slots per reliable Space per Peer, each priced at its payload plus the 15 bytes
    /// of bookkeeping that records what it holds (issue #63). Exact, like the receive line, so it can be
    /// below <c>Capacity × 2 × RetentionBytes</c> when retention is not a whole number of fragments.
    /// </summary>
    public long SendBudgetBytes =>
        (long)Reactor.Capacity * ReliableSpaceCount * RetentionSlots
            * (ResolvedFragmentSize + Core.RetentionPool.SlotBookkeepingBytes);

    /// <summary>
    /// The uniform retention slots one reliable Space holds per Peer: <see cref="RetentionBytes"/>
    /// divided by <see cref="ResolvedFragmentSize"/>, truncating any remainder. Stays derived from the
    /// byte bound rather than a knob of its own (ADR 0012, issue #43) so it cannot drift from
    /// <see cref="RetentionBytes"/> when the fragment size changes. <see cref="Validate"/> already
    /// requires retention to hold at least one fragment, so this is always at least 1.
    /// </summary>
    public int RetentionSlots => RetentionBytes / ResolvedFragmentSize;

    /// <summary>
    /// Validates every knob and every cross-check, throwing before a Reactor allocates anything.
    /// Internal because a consumer never validates an options value themselves — the constructor
    /// calls this, so a combination that cannot work throws at construction and nowhere else.
    /// </summary>
    internal void Validate()
    {
        Reactor.Validate();

        if (MaxPeersPerAddress < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxPeersPerAddress),
                MaxPeersPerAddress,
                $"a per-address limit of {MaxPeersPerAddress} refuses every handshake, so nothing could ever connect to this Reactor — it must be at least 1.");
        }

        if (MaxMessageSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxMessageSize),
                MaxMessageSize,
                $"maximum message size of {MaxMessageSize} bytes carries nothing — it must be at least 1.");
        }

        int fragmentSize = ResolvedFragmentSize;
        if (fragmentSize is < MinimumFragmentSize or > MaximumFragmentSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(FragmentSize),
                fragmentSize,
                $"fragment size of {fragmentSize} bytes lies outside {MinimumFragmentSize}-{MaximumFragmentSize} — " +
                $"{MinimumFragmentSize} is the reassembly buffer RFC 791 guarantees every host provides, and " +
                $"{MaximumFragmentSize} is the largest payload a UDP datagram has room for.");
        }

        if (RetentionBytes < fragmentSize)
        {
            throw new ArgumentException(
                $"retention of {RetentionBytes} bytes cannot hold one {fragmentSize}-byte fragment, so a Space would " +
                "have nothing to retransmit the moment it sent anything — raise RetentionBytes to at least the fragment size.",
                nameof(RetentionBytes));
        }

        int window = ResolvedReceiveWindowFragments;
        int windowFloor = ReceiveWindowFloor;
        if (window < windowFloor)
        {
            throw new ArgumentException(
                $"a receive window of {window} fragments cannot hold one {MaxMessageSize}-byte Message, which occupies " +
                $"{windowFloor} fragments of {fragmentSize} bytes. A Message arrives complete or not at all, so a window " +
                "narrower than one deadlocks rather than running slowly: the window fills with fragments of a Message " +
                "that can never be completed, nothing can be delivered, so nothing is acknowledged, so the window never " +
                $"advances and the sender can never send the rest (ADR 0004). Raise ReceiveWindowFragments to at least " +
                $"{windowFloor}, or lower MaxMessageSize.",
                nameof(ReceiveWindowFragments));
        }

        if (ResolvedMaxDatagramsPerPump < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxDatagramsPerPump),
                ResolvedMaxDatagramsPerPump,
                $"a limit of {ResolvedMaxDatagramsPerPump} datagrams per Pump reads nothing, so nothing could ever arrive — it must be at least 1.");
        }

        int missedHeartbeats = ResolvedMissedHeartbeats;
        if (missedHeartbeats < 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MissedHeartbeats),
                missedHeartbeats,
                $"a missed-heartbeat count of {missedHeartbeats} declares a Peer gone on a single lost datagram — it must be at least 2.");
        }

        if (SendIdleInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SendIdleInterval),
                SendIdleInterval,
                $"a send-idle interval of {SendIdleInterval} never elapses, so no heartbeat would ever be sent — it must be greater than zero.");
        }

        if (Linger <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Linger),
                Linger,
                $"a Linger of {Linger} closes a Session before it can retransmit its Goodbye — it must be greater than zero.");
        }

        if (SendIdleInterval > Session.AnnouncedIntervalLegality.Maximum)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SendIdleInterval),
                SendIdleInterval,
                $"a send-idle interval of {SendIdleInterval} is above the {Session.AnnouncedIntervalLegality.Maximum} a handshake may announce, so every Peer would discard this end's handshake.");
        }

        if (Linger > Session.AnnouncedIntervalLegality.Maximum)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Linger),
                Linger,
                $"a Linger of {Linger} is above the {Session.AnnouncedIntervalLegality.Maximum} a handshake may announce, so every Peer would discard this end's handshake.");
        }

        ValidateEndpoints();
        ValidateMemoryBill();
    }

    /// <summary>
    /// Resolves the frame arena against this profile's floor — one maximum Message, since an arena
    /// too small to hold one could never deliver one.
    /// </summary>
    internal int ResolveArenaSize() =>
        Reactor.ResolveArenaSize(MaxMessageSize, $"one maximum Message of {MaxMessageSize} bytes");

    /// <summary>
    /// Rejects a null or duplicated endpoint. Duplicates are refused rather than de-duplicated
    /// because binding is exclusive: the second bind of the same endpoint would fail anyway, and
    /// failing here names the endpoint while the caller can still see which line of their own
    /// configuration produced it.
    /// </summary>
    private void ValidateEndpoints()
    {
        for (int index = 0; index < Endpoints.Count; index++)
        {
            IPEndPoint endpoint = Endpoints[index] ?? throw new ArgumentException(
                    $"endpoint at index {index} is null — every declared endpoint must name an address and a port.",
                    nameof(Endpoints));

            for (int earlier = 0; earlier < index; earlier++)
            {
                if (Endpoints[earlier].Equals(endpoint))
                {
                    throw new ArgumentException(
                        $"endpoint {endpoint} is declared twice, at index {earlier} and index {index} — binds are exclusive, so the second could never succeed.",
                        nameof(Endpoints));
                }
            }
        }
    }

    /// <summary>
    /// Rejects a combination whose preallocation could not be made, naming both lines of the memory
    /// bill and their total. The check is arithmetic rather than an attempted allocation because the
    /// point is to tell the caller which knob to change, and a caught allocation failure knows only
    /// that something was too big.
    /// </summary>
    private void ValidateMemoryBill()
    {
        long receive = ReceiveBudgetBytes;
        long send = SendBudgetBytes;
        long total = receive + send;

        if (total > Array.MaxLength)
        {
            throw new ArgumentException(
                $"these options ask for {total} bytes of preallocation, which cannot be allocated. " +
                $"Receive is Capacity {Reactor.Capacity} × {ResolvedReceiveWindowFragments} fragments × " +
                $"({ReliableSpaceCount} reliable Spaces × ({ResolvedFragmentSize} payload bytes + {FragmentLengthBytes} " +
                $"length bytes + {FragmentGroupEntryBytes} group-identity bytes) + {SpaceCount} Spaces × 1 " +
                $"admission byte) plus Capacity {Reactor.Capacity} × {ReliableSpaceCount} reliable Spaces × " +
                $"{PendingGroupsPerSpace} waiting groups × {PendingGroupEntryBytes} bytes = {receive}; send is " +
                $"Capacity {Reactor.Capacity} × {ReliableSpaceCount} reliable Spaces × {RetentionSlots} slots × " +
                $"({ResolvedFragmentSize} payload bytes + {Core.RetentionPool.SlotBookkeepingBytes} bookkeeping bytes) = " +
                $"{send}. Lower Capacity, MaxMessageSize, ReceiveWindowFragments or RetentionBytes.",
                nameof(Reactor));
        }
    }
}
