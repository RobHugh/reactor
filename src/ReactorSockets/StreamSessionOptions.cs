using System.Net;
using System.Security.Cryptography;

namespace ReactorSockets;

/// <summary>
/// The construction surface of a <see cref="StreamSessionReactor"/> (ADR 0011): the shared L0 budget
/// composed in as <see cref="Reactor"/>, plus the knobs this profile alone has. Composed rather than
/// inherited, so that nothing protocol-specific here can ever leak into <see cref="ReactorOptions"/>
/// and turn it into the base class ADR 0011 refused.
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
/// There is no cookie, no ack, no sequence space, no fragment and no RTO here: TCP already provides
/// what those were built for, so a knob to configure one would have nothing under it.
/// </para>
/// <para>
/// <b>The memory bill.</b> A Reactor built from these options holds at most
/// <c>Capacity × (RetentionBytes + receive buffer + Message-boundary ring)</c> bytes, all of it per
/// Peer and none of it shared, so a <c>Send</c> on a healthy Peer never fails because of somebody
/// else's traffic. Retention and the ring are allocated at construction and each receive buffer when
/// its Peer takes a Slot, so the bound has no traffic term. The retention line is buffered <em>twice</em> over — once in our retention, once
/// again in the kernel's own send buffer for the socket — so a stream Peer's real footprint is this
/// bound plus whatever the OS is holding, and the OS's half is not fully ours to control. The third
/// line is what lets retention be released by what the peer has confirmed receiving rather than by what
/// the local kernel merely accepted, and is driven by <see cref="RetentionBytes"/> too: one <c>uint</c>
/// per Message that could be outstanding in a Peer's retention region at once, which this profile's
/// four-byte length prefix bounds at roughly one entry per four retained bytes, so it costs about as
/// much as retention itself.
/// </para>
/// <para>
/// <see cref="MaxMessageSize"/> appears in three places at once: announced to the remote as a receive
/// limit, flooring the receive buffer, and flooring <see cref="RetentionBytes"/>. It is therefore the
/// one knob whose careless enlargement multiplies memory in two directions rather than one.
/// </para>
/// <para>
/// Nagle is off on every stream Session socket and there is no knob to turn it on (ADR 0011): this
/// library batches by writing everything queued since the last Pump once, so a latency measurement
/// measures the consumer's Pump cadence, not TCP's acknowledgement clock.
/// </para>
/// <para>
/// Every check below is deterministic and entirely the caller's, so failures throw at construction —
/// <see cref="ArgumentOutOfRangeException"/> for a single bad value and <see cref="ArgumentException"/>
/// for a combination that cannot work — rather than being reported through a <c>Result</c>.
/// </para>
/// </remarks>
public sealed class StreamSessionOptions
{
    /// <summary>
    /// The default number of consecutive missed heartbeats before a Peer is declared gone: RFC 4271's
    /// hold time, three times the keepalive interval. Two would make a single lost heartbeat fatal;
    /// three is the smallest count that survives one loss, and is the count a standard already
    /// defends.
    /// </summary>
    private const int DefaultMissedHeartbeats = 3;

    /// <summary>The width of the <c>u32</c> length prefix every framed Message carries.</summary>
    private const int LengthPrefixSize = 4;

    /// <summary>
    /// The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
    /// Required — a Reactor cannot be built without one, and its <see cref="ReactorOptions.Capacity"/>
    /// has no default of its own.
    /// </summary>
    public required ReactorOptions Reactor { get; init; }

    /// <summary>
    /// The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
    /// statement about the consumer's own protocol that nothing here can derive. It is also
    /// announced to the remote as a receive limit, and floors both the receive buffer and
    /// <see cref="RetentionBytes"/> — an arena that cannot hold one maximum Message could never
    /// deliver one.
    /// </summary>
    public required int MaxMessageSize { get; init; }

    /// <summary>
    /// The bytes retained per Peer for the send path — retention <em>is</em> the send path (ADR 0011):
    /// a <c>Send</c> frames the Message once and appends it to this Peer's ring, and what reaches the
    /// socket is a copy from it. Required: this is the consumer trading their memory against their
    /// tolerance for a stalled Peer, and the right answer depends on their traffic, not on anything
    /// here. Must be at least <see cref="MaxMessageSize"/> plus the 4-byte length prefix, or a single
    /// maximum Message could never be retained.
    /// </summary>
    public required int RetentionBytes { get; init; }

    /// <summary>
    /// The most accepted Peers one address may hold at once (issue #452), so one reachable address
    /// cannot occupy every capacity Slot. Defaults to <see cref="ResolvedMaxPeersPerAddress"/>, one fewer
    /// than <see cref="ReactorOptions.Capacity"/>. Provenance: derived from capacity, as the largest limit
    /// that still keeps one address from holding every Slot (ADR 0014). How many Peers legitimately share
    /// an address depends on the NATs and proxies in front of this Reactor, so set it lower when strangers
    /// can reach it. At least 1. A value of <see cref="ReactorOptions.Capacity"/> or above sets no limit,
    /// which is what the default is at a capacity of 1.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The count is by address, without the port: one host can open connections from as many ports as it
    /// can bind, so a limit that counted ports would limit nothing. The address family and IPv6 scope stay
    /// in the key, because they distinguish hosts (ADR 0014). IPv6 addresses are counted by the prefix
    /// <see cref="PeerLimitIPv6PrefixLength"/> sets, so a host that can bind a whole prefix gets the limit
    /// once. IPv4 addresses are counted whole.
    /// </para>
    /// <para>
    /// A Peer is charged to the address its connection was accepted from for as long as it holds its Slot,
    /// including while Interrupted or Closing and after a Resume moves it to another address. A Resume is
    /// never refused by this limit, because the Peer it names already owns its Slot (ADR 0011). A dialled
    /// Peer is not charged: this Reactor chose to make it. A connection over the limit is refused the same way as
    /// one at a full Reactor, and reported with <see cref="DiagnosticId.AddressPeerLimitReached"/>.
    /// The same limit applies to accepted connections still waiting for their first handshake record: one address may have at most this many of them held, and a further one that has not sent its whole record with its connection is closed at once with nothing sent.
    /// </para>
    /// </remarks>
    public int? MaxPeersPerAddress { get; init; }

    /// <summary>
    /// How many leading bits of an IPv6 address form the key for <see cref="MaxPeersPerAddress"/>, so
    /// every address in one prefix shares one limit. Defaults to <see cref="ResolvedPeerLimitIPv6PrefixLength"/>,
    /// 64. Provenance: RFC 4291 §2.5.1 fixes the interface identifier of a unicast address at 64 bits, so a
    /// /64 is one link (ADR 0014). From 1 to 128. A value of 128 counts each address on its own.
    /// </summary>
    /// <remarks>
    /// Every on-link host on an IPv6 LAN shares one /64, so at the default those Peers share one limit.
    /// Set 128 when Peers legitimately arrive from one link. A shorter length groups more of a customer's
    /// delegated prefix, at the cost of grouping unrelated customers. The scope stays in the key, so
    /// link-local addresses are grouped per interface. IPv4 addresses are never grouped.
    /// </remarks>
    public int? PeerLimitIPv6PrefixLength { get; init; }

    /// <summary>
    /// How long a Session may go without sending before it sends a heartbeat instead. Required:
    /// shorter costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the
    /// consumer knows which they would rather spend. At most 2^32 - 1 milliseconds (about 49.7 days), the
    /// most a handshake may announce.
    /// </summary>
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
    /// acquires later. A consumer addresses Peers, never the sockets this profile binds, so binding
    /// returns no identity: an empty set means dial-only, a meaningful configuration rather than a
    /// hole, and there is no rebind — a Reactor that must listen elsewhere is a new Reactor.
    /// Provenance: empty is the absence of a declaration, not a chosen address — a dial's local port
    /// is the one TCP's connect binds implicitly.
    /// </summary>
    public IReadOnlyList<IPEndPoint> Endpoints { get; init; } = [];

    /// <summary>
    /// The entropy the resume secret is drawn from. A construction knob in ADR 0011's sense — the
    /// consumer's own resource, and a consumer with a hardware source or a policy about which
    /// generator may be used is entitled to supply it. It carries a default only because that default
    /// is derived rather than guessed. Provenance: the platform's own CSPRNG.
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
    /// The bytes held per Peer for an incoming, not-yet-complete Message. Defaults to
    /// <see cref="MaxMessageSize"/> plus the 4-byte length prefix. Provenance: derived from
    /// <see cref="MaxMessageSize"/> — its own floor, since that is exactly one incomplete maximum
    /// Message and nothing narrower could ever hold one.
    /// </summary>
    public int? ReceiveBufferSize { get; init; }

    /// <summary>
    /// <c>SO_SNDBUF</c> for every Peer socket this Reactor accepts or dials, or <see langword="null"/>
    /// — the default — for the OS default and its own autotuning. This is the kernel's send queue, not
    /// <see cref="RetentionBytes"/>: retention is what this end holds until the peer confirms receipt,
    /// while this bounds how much the kernel will absorb from a flush before a write reports it would
    /// block and the bytes stay in retention, unsent. Unset, the two are unrelated in practice on
    /// Linux, whose send autotuning will absorb megabytes against a peer that has stopped reading; set,
    /// a consumer who needs transport backpressure to reach their own flow control at a known point can
    /// put it there. Provenance: unset leaves the OS's own default and autotuning in place, so the
    /// library chooses nothing.
    /// </summary>
    /// <remarks>
    /// A request, not an exact size — Linux doubles the value and holds it to its own <c>tcp_wmem</c>
    /// floor, so nothing may depend on a precise resulting byte count. It also does not bound
    /// correctness of anything this profile does: a Reactor sends whatever the socket accepts and keeps
    /// the rest, whatever the buffer's size.
    /// </remarks>
    public int? SendBufferSize { get; init; }

    /// <summary>
    /// How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
    /// Defaults to 3. Provenance: RFC 4271's hold time, three times the keepalive interval. Must be at
    /// least 2, because a count of 1 makes a
    /// single lost heartbeat fatal, and in .NET the stall that produces it is as ordinary as a
    /// collection pause.
    /// </summary>
    public int? MissedHeartbeats { get; init; }

    /// <summary>The floor both <see cref="RetentionBytes"/> and the receive buffer are held to: one maximum Message plus its length prefix.</summary>
    public int ReceiveBufferFloor => MaxMessageSize + LengthPrefixSize;

    /// <summary>The resolved receive buffer: <see cref="ReceiveBufferSize"/> if set, otherwise <see cref="ReceiveBufferFloor"/>.</summary>
    public int ResolvedReceiveBufferSize => ReceiveBufferSize ?? ReceiveBufferFloor;

    /// <summary>
    /// The resolved per-address limit: <see cref="MaxPeersPerAddress"/> if set, otherwise one fewer than
    /// <see cref="ReactorOptions.Capacity"/>, and at least 1.
    /// </summary>
    public int ResolvedMaxPeersPerAddress => MaxPeersPerAddress ?? Math.Max(1, Reactor.Capacity - 1);

    /// <summary>
    /// The resolved IPv6 grouping length: <see cref="PeerLimitIPv6PrefixLength"/> if set, otherwise 64.
    /// </summary>
    public int ResolvedPeerLimitIPv6PrefixLength => PeerLimitIPv6PrefixLength ?? Source.DefaultPeerLimitIPv6PrefixLength;

    /// <summary>The resolved missed-heartbeat count: <see cref="MissedHeartbeats"/> if set, otherwise 3.</summary>
    public int ResolvedMissedHeartbeats => MissedHeartbeats ?? DefaultMissedHeartbeats;

    /// <summary>
    /// The receive line of the memory bill in bytes, as a <see langword="long"/> so that a
    /// combination too large to allocate is a value this can report rather than an overflow it
    /// silently wraps through.
    /// </summary>
    public long ReceiveBudgetBytes => (long)Reactor.Capacity * ResolvedReceiveBufferSize;

    /// <summary>The send line of the memory bill in bytes, reported the same way.</summary>
    public long SendBudgetBytes => (long)Reactor.Capacity * RetentionBytes;

    /// <summary>
    /// The number of <see cref="Core.RetentionArena"/> Message-boundary ring entries one Peer's region
    /// holds: <c>⌈RetentionBytes / 4⌉</c>, against the four-byte length prefix this profile passes the
    /// arena as its <c>minimumFramedMessageSize</c>. Delegates to
    /// <see cref="Core.RetentionArena.BoundaryEntriesFor"/> rather than repeating the ceiling division,
    /// so this published memory bill cannot drift from what the arena actually allocates.
    /// </summary>
    private long BoundaryRingEntriesPerPeer =>
        Core.RetentionArena.BoundaryEntriesFor(RetentionBytes, LengthPrefixSize);

    /// <summary>
    /// The Message-boundary ring line of the memory bill in bytes: one <c>uint</c> (4 bytes) per entry
    /// in <see cref="BoundaryRingEntriesPerPeer"/>, across every Peer, reported the same way as the
    /// other two lines. This is what makes releasing retention by what the peer confirmed receiving
    /// possible at all — see the class remarks.
    /// </summary>
    public long BoundaryRingBudgetBytes => (long)Reactor.Capacity * BoundaryRingEntriesPerPeer * sizeof(uint);

    /// <summary>
    /// Validates every knob and every cross-check, throwing before a Reactor allocates anything.
    /// Internal because a consumer never validates an options value themselves — the constructor
    /// calls this, so a combination that cannot work throws at construction and nowhere else.
    /// </summary>
    internal void Validate()
    {
        Reactor.Validate();

        if (MaxPeersPerAddress is int maxPeersPerAddress && maxPeersPerAddress < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxPeersPerAddress),
                maxPeersPerAddress,
                $"a per-address limit of {maxPeersPerAddress} refuses every Hello, so nothing could ever connect to this Reactor — it must be at least 1.");
        }

        Source.ValidatePeerLimitIPv6PrefixLength(PeerLimitIPv6PrefixLength, nameof(PeerLimitIPv6PrefixLength));

        if (MaxMessageSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxMessageSize),
                MaxMessageSize,
                $"maximum message size of {MaxMessageSize} bytes carries nothing — it must be at least 1.");
        }

        int receiveBufferFloor = ReceiveBufferFloor;

        if (RetentionBytes < receiveBufferFloor)
        {
            throw new ArgumentException(
                $"retention of {RetentionBytes} bytes cannot hold one Message of {MaxMessageSize} bytes plus its " +
                $"4-byte length prefix — raise RetentionBytes to at least {receiveBufferFloor}, or lower MaxMessageSize.",
                nameof(RetentionBytes));
        }

        int receiveBuffer = ResolvedReceiveBufferSize;
        if (receiveBuffer < receiveBufferFloor)
        {
            throw new ArgumentException(
                $"a receive buffer of {receiveBuffer} bytes cannot hold one incomplete Message of {MaxMessageSize} bytes " +
                $"plus its 4-byte length prefix — raise ReceiveBufferSize to at least {receiveBufferFloor}, or lower MaxMessageSize.",
                nameof(ReceiveBufferSize));
        }

        // No floor beyond 1, and deliberately none tied to MaxMessageSize: a send buffer smaller than a
        // Message is a slower connection, not a broken one — a flush hands over what the kernel accepts
        // and keeps the rest in retention for the next one.
        if (SendBufferSize is int sendBuffer && sendBuffer < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SendBufferSize),
                sendBuffer,
                $"a send buffer of {sendBuffer} bytes carries nothing — it must be at least 1, or null for the OS default.");
        }

        int missedHeartbeats = ResolvedMissedHeartbeats;
        if (missedHeartbeats < 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MissedHeartbeats),
                missedHeartbeats,
                $"a missed-heartbeat count of {missedHeartbeats} declares a Peer gone on a single lost heartbeat — it must be at least 2.");
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

        if (SendIdleInterval > Session.HandshakeLegality.MaximumInterval)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SendIdleInterval),
                SendIdleInterval,
                $"a send-idle interval of {SendIdleInterval} is above the {Session.HandshakeLegality.MaximumInterval} a handshake may announce, so every Peer would discard this end's handshake.");
        }

        if (Linger > Session.HandshakeLegality.MaximumInterval)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Linger),
                Linger,
                $"a Linger of {Linger} is above the {Session.HandshakeLegality.MaximumInterval} a handshake may announce, so every Peer would discard this end's handshake.");
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
    /// Rejects a null or duplicated endpoint. Duplicates are refused rather than de-duplicated because
    /// binding is exclusive: the second bind of the same endpoint would fail anyway, and failing here
    /// names the endpoint while the caller can still see which line of their own configuration
    /// produced it.
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
        long boundaryRing = BoundaryRingBudgetBytes;
        long total = receive + send + boundaryRing;

        if (total > Array.MaxLength)
        {
            throw new ArgumentException(
                $"these options ask for {total} bytes of preallocation, which cannot be allocated. " +
                $"Receive is Capacity {Reactor.Capacity} × {ResolvedReceiveBufferSize} bytes = {receive}; " +
                $"send is Capacity {Reactor.Capacity} × {RetentionBytes} bytes = {send}; " +
                $"the Message-boundary ring is Capacity {Reactor.Capacity} × {BoundaryRingEntriesPerPeer} entries × " +
                $"4 bytes = {boundaryRing}. Lower Capacity, MaxMessageSize or RetentionBytes.",
                nameof(Reactor));
        }
    }
}
