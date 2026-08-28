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
/// The frame arena (<see cref="ReactorOptions.ArenaSize"/>) is over and above those two lines and is
/// shared across the whole Reactor rather than multiplied by capacity. Every check below is
/// deterministic and entirely the caller's, so failures throw at construction —
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
    /// The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
    /// Required — a Reactor cannot be built without one, and its <see cref="ReactorOptions.Capacity"/>
    /// has no default of its own.
    /// </summary>
    public required ReactorOptions Reactor { get; init; }

    /// <summary>
    /// The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
    /// statement about the consumer's own protocol that nothing here can derive. It is also the
    /// frame arena's floor — an arena that cannot hold one maximum Message could never deliver one.
    /// </summary>
    /// <remarks>
    /// Announced to the peer at the handshake precisely so an oversized claim can be refused before
    /// this end holds anything for it: <see cref="ReceiveWindowFloor"/> derives the largest declared Fragment
    /// <c>fragmentCount</c> this value can ever back, and a set claiming more than that is closed on
    /// its first Fragment, before any receive-window slot or Fragment-group bookkeeping exists for it
    /// (issue #282).
    /// </remarks>
    public required int MaxMessageSize { get; init; }

    /// <summary>
    /// The bytes each reliable Space retains per Peer for retransmission, until what it holds is
    /// acknowledged. Required: this is the consumer trading their memory against their tolerance for
    /// stalling on loss, and the right answer depends on their traffic, not on anything here.
    /// </summary>
    public required int RetentionBytes { get; init; }

    /// <summary>
    /// How long a Session may go without sending before it sends a heartbeat instead. Required:
    /// shorter costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the
    /// consumer knows which they would rather spend.
    /// </summary>
    public required TimeSpan SendIdleInterval { get; init; }

    /// <summary>
    /// How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
    /// flight. Required, for the same reason as <see cref="SendIdleInterval"/>: it trades a prompt
    /// teardown against a clean one.
    /// </summary>
    public required TimeSpan Linger { get; init; }

    /// <summary>
    /// The endpoints to bind at construction, in the order supplied; possibly empty. Declared once
    /// and never rebound (ADR 0014) — a Session's addresses are part of what it is, not something it
    /// acquires later. An empty set binds nothing until the first dial, which binds a single
    /// wildcard, port-zero socket in the destination's address family.
    /// </summary>
    public IReadOnlyList<IPEndPoint> Endpoints { get; init; } = [];

    /// <summary>
    /// The bytes a fragment carries, and therefore the unit every buffer here is measured in.
    /// Defaults to 1200 — QUIC's floor beneath the 1280-byte IPv6 minimum MTU — which is the one
    /// value with citable provenance for surviving an arbitrary path. Must lie between 576 (RFC 791's
    /// guaranteed reassembly minimum) and 65 507 (the maximum UDP payload).
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
    /// number a maximum Message occupies, which is ADR 0004's deadlock floor rather than a
    /// performance choice: a window narrower than one whole Message can never deliver one, so it can
    /// never advance, so the sender can never send the rest.
    /// </summary>
    public int? ReceiveWindowFragments { get; init; }

    /// <summary>
    /// How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
    /// Defaults to 3, RFC 4271's hold-time multiple. Must be at least 2, because a count of 1 makes a
    /// single lost datagram fatal and no network is that reliable.
    /// </summary>
    public int? MissedHeartbeats { get; init; }

    /// <summary>
    /// The entropy the cookie secrets, session ids and resume secrets are drawn from. A construction
    /// knob in ADR 0012's sense — the consumer's own resource, and a consumer with a hardware source
    /// or a policy about which generator may be used is entitled to supply it. It carries a default
    /// only because that default is derived rather than guessed: the platform's own CSPRNG is the
    /// citable provenance.
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

    /// <summary>The resolved missed-heartbeat count: <see cref="MissedHeartbeats"/> if set, otherwise 3.</summary>
    public int ResolvedMissedHeartbeats => MissedHeartbeats ?? DefaultMissedHeartbeats;

    /// <summary>
    /// The receive line of the memory bill in bytes, as a <see langword="long"/> so that a
    /// combination too large to allocate is a value this can report rather than an overflow it
    /// silently wraps through.
    /// </summary>
    /// <remarks>
    /// Three things per Peer scale with the window, and this figure covers all three (issue #174,
    /// #257). Payload slots — <see cref="ResolvedFragmentSize"/> bytes each — exist only for the
    /// <see cref="ReliableSpaceCount"/> Spaces that hold fragments until a Message is whole. The
    /// one-byte-per-slot admission record a receive window classifies against exists for all
    /// <see cref="SpaceCount"/> Spaces, because all four classify whether or not they retain.
    /// <see cref="FragmentLengthBytes"/> per slot records how many bytes of a held Fragment's payload
    /// are actually occupied, and exists only for the same <see cref="ReliableSpaceCount"/> Spaces as
    /// the payload it describes — this is the receive window's own reassembly bookkeeping (issue
    /// #257), not a separate buffer. Nothing on the receive side is allocated outside this figure, so
    /// it is an exact statement of what a Reactor takes, not a floor it sits above.
    /// </remarks>
    public long ReceiveBudgetBytes =>
        ((long)Reactor.Capacity * ResolvedReceiveWindowFragments
            * (((long)ReliableSpaceCount * (ResolvedFragmentSize + FragmentLengthBytes)) + SpaceCount))
        + ((long)Reactor.Capacity * ReliableSpaceCount * PendingGroupsPerSpace * PendingGroupEntryBytes);

    /// <summary>
    /// The completed Fragment groups one reliable Space can have waiting for delivery at once: half
    /// <see cref="ResolvedReceiveWindowFragments"/>, never fewer than one. A group is at least two
    /// Fragments wide and holds its slots until it is delivered, so the window's own size is what
    /// bounds this — see <see cref="Session.PendingFragmentGroups"/>.
    /// </summary>
    internal int PendingGroupsPerSpace =>
        ResolvedReceiveWindowFragments / 2 < 1 ? 1 : ResolvedReceiveWindowFragments / 2;

    /// <summary>The send line of the memory bill in bytes, reported the same way.</summary>
    public long SendBudgetBytes =>
        (long)Reactor.Capacity * ReliableSpaceCount * RetentionBytes;

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
                $"length bytes) + {SpaceCount} Spaces × 1 admission byte) = {receive}; send is Capacity " +
                $"{Reactor.Capacity} × {ReliableSpaceCount} reliable Spaces × {RetentionBytes} bytes = {send}. " +
                "Lower Capacity, MaxMessageSize, ReceiveWindowFragments or RetentionBytes.",
                nameof(Reactor));
        }
    }
}
