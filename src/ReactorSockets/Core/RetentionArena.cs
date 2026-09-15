namespace ReactorSockets.Core;

/// <summary>
/// The retention ring's pure-arithmetic core (issue #21, 3.5): one buffer, sliced into one
/// fixed-size region per share-holder (a Peer, in the Session profile), that already-framed bytes
/// a send could not deliver immediately are copied into, drained to the transport from, and released
/// from as delivery confirms.
/// </summary>
/// <remarks>
/// <para>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/>, the same
/// convention <see cref="FrameArena"/> uses. Per-share-holder usage is tracked in preallocated
/// arrays sized once at construction, so nothing here allocates later. Usage is tracked as
/// monotonically-increasing <c>ulong</c> running totals rather than wrapping head/tail indices (ADR
/// 0015, the same move made for <see cref="ShareHolderTable{T}"/>'s generation counter). 2^64 bytes
/// through one share-holder's region is unreachable by any process that will ever run, so the totals
/// cannot wrap and the ring position derived from them (<c>total % BoundPerShareHolder</c>) is always
/// correct by unreachability rather than by a bound this type has to check for.
/// </para>
/// <para>
/// <b>Two cursors past the written total, for two unrelated questions (issue #113).</b>
/// <see cref="_sentTotal"/> is how far this end has gotten handing already-appended bytes to its own
/// transport — advanced by <see cref="MarkSent"/>, read by <see cref="GetUnsent"/>, and never anything
/// but monotonically non-decreasing up to <see cref="_writtenTotal"/>. <see cref="_releasedTotal"/> is
/// how far the *peer* has confirmed receiving — advanced only by <see cref="ReleaseThroughMessageCount"/>
/// once a Session profile decodes that confirmation off the wire, and it is what <see cref="TryAppend"/>
/// weighs against <see cref="BoundPerShareHolder"/> to decide whether more room exists. The two cannot
/// be collapsed into one cursor: this end's own Goodbye sentinel is appended through the very same
/// <see cref="TryAppend"/> a Message is (see below), so it is something this end must still *send*, but
/// nothing a peer's Message-received counter — which counts Messages, not control records — will ever
/// confirm. A single cursor answering both questions would leave the Goodbye's own bytes eternally
/// short of "released" and, depending which question it was actually answering, either wedge
/// <see cref="StreamSessionReactor"/>'s own local-close check forever or re-hand already-sent bytes to
/// the transport a second time the next Pump a Send lands on. Splitting the cursor is what keeps both
/// questions answerable independently, and correctly, once release-on-write is gone.
/// </para>
/// <para>
/// Release also comes in a second unit (issue #110, subtask 4.1.2): <see cref="TryAppend"/> counts
/// the Messages it accepts as well as their bytes, and <see cref="ReleaseThroughMessageCount"/> maps
/// a peer's "I have received through Message N" onto the byte extent that covers Messages 1..N. The
/// appended-Message count and released-Message count are two more <c>ulong</c> running totals, the
/// same discipline as the byte totals and for the same reason. The byte extent of each outstanding
/// Message is recorded in a ring sized <c>⌈BoundPerShareHolder / MinimumFramedMessageSize⌉</c>
/// entries per share-holder — the most Messages that could ever be outstanding at once, since
/// <see cref="TryAppend"/> refuses a write shorter than <see cref="MinimumFramedMessageSize"/> and
/// retained bytes can never exceed <see cref="BoundPerShareHolder"/>. This type still carries no
/// framing knowledge of its own — no length-prefix size, no wire format — it is only ever *told* the
/// smallest write it must plan room for, the same way it is already told
/// <c>boundPerShareHolder</c> and <c>capacity</c>: a caller that understates
/// it breaks the bound the ring is sized on, exactly as a caller that understated
/// <c>boundPerShareHolder</c> would break <see cref="TryAppend"/>'s exhaustion check.
/// </para>
/// <para>
/// <b>The Goodbye sentinel needs no reconciling against a peer's Message count (issue #113).</b>
/// <see cref="TryAppend"/> is called for a Session profile's own outbound Goodbye sentinel exactly as
/// it is for an application Message, so the appended-Message count can run one ahead of the count of
/// Messages a caller would ever describe to <see cref="ReleaseThroughMessageCount"/>. That still needs
/// no offset: a caller only ever appends its Goodbye once every Message it will ever send has already
/// been appended (a Session profile refuses further Sends the moment its own Goodbye is queued), so
/// the Goodbye's boundary entry always sits strictly after every Message's — a legitimate
/// <c>messageCount</c>, which counts only Messages, can never reach it, and every count
/// below it resolves to exactly the Message it names. The Goodbye's own bytes are freed by
/// <see cref="MarkSent"/> instead, the moment this end has handed them to its transport — see the
/// cursor split above.
/// </para>
/// <para>
/// <b>Cost.</b> Each ring entry is one <c>uint</c> (4 bytes), not a <c>ulong</c>: a boundary does not
/// need the same 2^64-of-headroom argument the byte totals get, because the only thing
/// <see cref="ReleaseThroughMessageCount"/> ever needs from one is its position within the region's
/// currently retained bytes, and that is bounded by <see cref="BoundPerShareHolder"/> — an
/// <c>int</c>, so under 2^31. Each entry stores only the low 32 bits of the running total at the
/// moment its Message was appended; <see cref="ReleaseThroughMessageCount"/> reconstructs the full
/// <c>ulong</c> position from those 32 bits and the share-holder's current written total by ordinary
/// unsigned wraparound subtraction — the same trick sequence-number arithmetic uses, correct here for
/// the same reason: the true distance between the current written total and any boundary this method
/// is ever asked to resolve cannot reach 2^32, so recovering it from 32 bits of the difference is
/// unambiguous. That holds as long as <see cref="ReleaseThroughMessageCount"/> is the only thing
/// advancing <see cref="_releasedTotal"/> — true since issue #113 removed the byte-count
/// <c>Release</c> method that used to share the role, the "switch" issue #25 describes. The ring adds
/// <c>4 × Capacity × ⌈BoundPerShareHolder / MinimumFramedMessageSize⌉</c> bytes on top of the
/// <c>Capacity × BoundPerShareHolder</c> the byte buffer already costs — roughly
/// <c>4 / MinimumFramedMessageSize</c> times the retention bill. The stream Session profile passes its
/// wire format's true floor — the four-byte length prefix alone, since both the Goodbye sentinel and a
/// zero-payload Message write exactly that many bytes and nothing shorter is ever appended — which
/// keeps this at roughly 1× rather than the flat 4× a 1-byte-per-entry bound would have cost.
/// </para>
/// </remarks>
internal sealed class RetentionArena
{
    private readonly byte[] _buffer;
    private readonly ulong[] _writtenTotal;
    private readonly ulong[] _sentTotal;
    private readonly ulong[] _releasedTotal;
    private readonly ulong[] _highWaterBytes;
    private readonly ulong[] _appendedMessageCount;
    private readonly ulong[] _releasedMessageCount;
    private readonly ulong[] _sentMessageCount;
    private readonly ulong[] _sentMessageBoundaryTotal;
    private readonly uint[] _messageBoundaryLow32;
    private readonly int _boundaryEntriesPerShareHolder;

    /// <summary>
    /// Constructs an arena of <paramref name="capacity"/> fixed-size regions, each
    /// <paramref name="boundPerShareHolder"/> bytes.
    /// </summary>
    /// <param name="boundPerShareHolder">
    /// The number of bytes each share-holder's region holds. Rejected below 1, which can never
    /// hold anything.
    /// </param>
    /// <param name="capacity">The number of share-holders this arena divides its regions across.</param>
    /// <param name="minimumFramedMessageSize">
    /// The smallest number of bytes any single <see cref="TryAppend"/> call will ever be asked to
    /// retain — a stream Session profile's bare length prefix, for instance, since its Goodbye
    /// sentinel carries no payload and <c>Send</c> enforces no lower bound on a Message's own. Rejected
    /// below 1, the same floor <paramref name="boundPerShareHolder"/> is held to.
    /// This exists solely to size the Message-boundary ring
    /// (<c>⌈boundPerShareHolder / minimumFramedMessageSize⌉</c> entries per share-holder, see the
    /// class remarks) without this type having to know anything about framing itself — the caller
    /// states the floor, and <see cref="TryAppend"/> then enforces it, exactly as it already enforces
    /// <paramref name="boundPerShareHolder"/>. A caller that passes a value larger than any write it
    /// actually makes breaks the ring's bound silently, which is why <see cref="TryAppend"/> refuses
    /// any write shorter than this rather than trusting the caller never to occur.
    /// </param>
    public RetentionArena(int boundPerShareHolder, int capacity, int minimumFramedMessageSize)
    {
        if (boundPerShareHolder < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(boundPerShareHolder),
                boundPerShareHolder,
                $"bound of {boundPerShareHolder} bytes cannot hold anything — bound per share-holder must be at least 1.");
        }

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"capacity of {capacity} admits no share-holders — capacity must be at least 1.");
        }

        if (minimumFramedMessageSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(minimumFramedMessageSize),
                minimumFramedMessageSize,
                $"minimum framed Message size of {minimumFramedMessageSize} bytes cannot describe any write — it must be at least 1.");
        }

        BoundPerShareHolder = boundPerShareHolder;
        Capacity = capacity;
        MinimumFramedMessageSize = minimumFramedMessageSize;

        _boundaryEntriesPerShareHolder =
            checked((int)BoundaryEntriesFor(boundPerShareHolder, minimumFramedMessageSize));

        _buffer = new byte[boundPerShareHolder * capacity];
        _writtenTotal = new ulong[capacity];
        _sentTotal = new ulong[capacity];
        _releasedTotal = new ulong[capacity];
        _highWaterBytes = new ulong[capacity];
        _appendedMessageCount = new ulong[capacity];
        _releasedMessageCount = new ulong[capacity];
        _sentMessageCount = new ulong[capacity];
        _sentMessageBoundaryTotal = new ulong[capacity];
        _messageBoundaryLow32 = new uint[checked(_boundaryEntriesPerShareHolder * capacity)];
    }

    /// <summary>The number of bytes each share-holder's region holds.</summary>
    public int BoundPerShareHolder { get; }

    /// <summary>The number of share-holders this arena divides its regions across.</summary>
    public int Capacity { get; }

    /// <summary>
    /// The smallest number of bytes any single <see cref="TryAppend"/> call is declared to ever
    /// retain, supplied at construction. Sizes the Message-boundary ring; see the class remarks.
    /// </summary>
    public int MinimumFramedMessageSize { get; }

    /// <summary>
    /// How many Message-boundary ring entries one share-holder's region needs —
    /// <c>⌈boundPerShareHolder / minimumFramedMessageSize⌉</c>, the most whole Messages that region can
    /// hold at once. Public and static so the one caller that must advertise this arena's memory cost
    /// before constructing it (<see cref="StreamSessionOptions.BoundaryRingBudgetBytes"/>) bills the
    /// same arithmetic the constructor allocates against, rather than a copy of it that could drift and
    /// quietly make the published memory bill a lie.
    /// </summary>
    /// <remarks>
    /// Returns <c>long</c>, and divides in <c>long</c>, because both arguments are caller-controlled:
    /// the entry count — let alone the count times <c>Capacity</c> times 4 bytes per entry — can exceed
    /// <see cref="int"/> well before it exceeds <see cref="Array.MaxLength"/>, and a silently wrapped
    /// negative length is a far worse failure than an explicit one. The constructor's own
    /// <c>checked</c> cast is what turns the excess into that explicit failure.
    /// </remarks>
    public static long BoundaryEntriesFor(int boundPerShareHolder, int minimumFramedMessageSize) =>
        ((long)boundPerShareHolder + minimumFramedMessageSize - 1) / minimumFramedMessageSize;

    /// <summary>
    /// Attempts to copy <paramref name="framedBytes"/> into <paramref name="shareHolderId"/>'s
    /// region. Refused as <see cref="RetentionOutcome.Exhausted"/> without touching the arena when
    /// the share-holder's currently retained bytes plus <paramref name="framedBytes"/> would exceed
    /// <see cref="BoundPerShareHolder"/>. Otherwise the bytes are copied — wrapping into two copies
    /// when the write crosses the region's end boundary — the share-holder's written total and
    /// high-water mark advance, this call is recorded as one more appended Message for
    /// <see cref="ReleaseThroughMessageCount"/> to map back to, and <see cref="RetentionOutcome.Committed"/>
    /// is returned.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="framedBytes">
    /// The already-framed bytes to retain. Rejected when shorter than
    /// <see cref="MinimumFramedMessageSize"/> — a write the caller declared could never happen is
    /// exactly the write that would let more Messages be outstanding at once than the Message-boundary
    /// ring has room for (see the class remarks), so this is enforced rather than trusted.
    /// </param>
    public RetentionOutcome TryAppend(int shareHolderId, ReadOnlySpan<byte> framedBytes)
    {
        ValidateShareHolderId(shareHolderId);

        if (framedBytes.Length < MinimumFramedMessageSize)
        {
            throw new ArgumentException(
                $"framedBytes of {framedBytes.Length} bytes is shorter than the declared minimum framed " +
                $"Message size of {MinimumFramedMessageSize} bytes.",
                nameof(framedBytes));
        }

        ulong retained = _writtenTotal[shareHolderId] - _releasedTotal[shareHolderId];
        ulong remaining = (ulong)BoundPerShareHolder - retained;
        if ((ulong)framedBytes.Length > remaining)
        {
            return RetentionOutcome.Exhausted;
        }

        int regionStart = shareHolderId * BoundPerShareHolder;
        int position = (int)(_writtenTotal[shareHolderId] % (ulong)BoundPerShareHolder);
        int spaceToRegionEnd = BoundPerShareHolder - position;

        if (framedBytes.Length <= spaceToRegionEnd)
        {
            framedBytes.CopyTo(_buffer.AsSpan(regionStart + position, framedBytes.Length));
        }
        else
        {
            framedBytes[..spaceToRegionEnd].CopyTo(_buffer.AsSpan(regionStart + position, spaceToRegionEnd));
            framedBytes[spaceToRegionEnd..].CopyTo(_buffer.AsSpan(regionStart, framedBytes.Length - spaceToRegionEnd));
        }

        _writtenTotal[shareHolderId] += (ulong)framedBytes.Length;

        ulong retainedAfter = _writtenTotal[shareHolderId] - _releasedTotal[shareHolderId];
        if (retainedAfter > _highWaterBytes[shareHolderId])
        {
            _highWaterBytes[shareHolderId] = retainedAfter;
        }

        _appendedMessageCount[shareHolderId]++;
        _messageBoundaryLow32[BoundaryIndex(shareHolderId, _appendedMessageCount[shareHolderId])] =
            unchecked((uint)_writtenTotal[shareHolderId]);

        return RetentionOutcome.Committed;
    }

    /// <summary>
    /// Returns <paramref name="shareHolderId"/>'s unsent bytes — from its sent total to its written
    /// total — as one or two contiguous memory segments over the backing array, for a caller about to
    /// hand them to its own transport. <c>second</c> is <see langword="default"/> unless
    /// the unsent range wraps past the region's end boundary, in which case it holds the wrapped
    /// remainder. Deliberately independent of what the peer has confirmed
    /// (<see cref="ReleaseThroughMessageCount"/>): see the class remarks on the two cursors past the
    /// written total.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public (ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second) GetUnsent(int shareHolderId)
    {
        (ArraySegment<byte> first, ArraySegment<byte> second) = GetUnsentSegments(shareHolderId);
        return (first.AsMemory(), second.AsMemory());
    }

    /// <summary>
    /// The same two unsent runs <see cref="GetUnsent"/> reports, expressed as
    /// <see cref="ArraySegment{T}"/> over the backing array rather than <see cref="ReadOnlyMemory{T}"/>
    /// — the shape a gather write needs (issue #132), since a scatter/gather socket send takes an
    /// <see cref="System.Collections.Generic.IList{T}"/> of segments and nothing else. This exists
    /// beside <see cref="GetUnsent"/>, rather than replacing it, so a caller writing one run at a time
    /// keeps the span-shaped view it already had, and so neither caller has to prove at runtime that
    /// this arena's memory is array-backed.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public (ArraySegment<byte> first, ArraySegment<byte> second) GetUnsentSegments(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        ulong unsent = _writtenTotal[shareHolderId] - _sentTotal[shareHolderId];
        int regionStart = shareHolderId * BoundPerShareHolder;
        int position = (int)(_sentTotal[shareHolderId] % (ulong)BoundPerShareHolder);
        int spaceToRegionEnd = BoundPerShareHolder - position;

        if (unsent <= (ulong)spaceToRegionEnd)
        {
            return (new ArraySegment<byte>(_buffer, regionStart + position, (int)unsent), default);
        }

        ArraySegment<byte> first = new(_buffer, regionStart + position, spaceToRegionEnd);
        ArraySegment<byte> second = new(_buffer, regionStart, (int)unsent - spaceToRegionEnd);
        return (first, second);
    }

    /// <summary>
    /// Advances <paramref name="shareHolderId"/>'s sent total by <paramref name="bytesSent"/>, marking
    /// that many of its oldest unsent bytes as handed to the transport — the counterpart to
    /// <see cref="GetUnsent"/> a caller advances the instant a socket write accepts them. Throws —
    /// rather than silently clamping — when <paramref name="bytesSent"/> exceeds what
    /// <see cref="GetUnsent"/> currently reports. Never touches <see cref="ReleaseThroughMessageCount"/>'s
    /// released total or the ring room it governs: handing bytes to a transport is not a peer
    /// confirming receipt of them (see the class remarks on the two cursors).
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="bytesSent">The number of oldest unsent bytes to mark sent.</param>
    public void MarkSent(int shareHolderId, int bytesSent)
    {
        ValidateShareHolderId(shareHolderId);

        ulong unsent = _writtenTotal[shareHolderId] - _sentTotal[shareHolderId];
        if ((ulong)bytesSent > unsent)
        {
            throw new ArgumentOutOfRangeException(
                nameof(bytesSent),
                bytesSent,
                $"share-holder {shareHolderId} has only {unsent} unsent bytes — cannot mark {bytesSent} sent.");
        }

        _sentTotal[shareHolderId] += (ulong)bytesSent;

        // Walks the sent-Message cursor past every boundary the new sent total reaches. The next boundary
        // belongs to a Message not yet wholly sent, so not yet released, so its ring entry is still live.
        // Each Message is walked past once, so the cost is constant per Message over the Session.
        while (_sentMessageCount[shareHolderId] < _appendedMessageCount[shareHolderId])
        {
            ulong nextBoundary = MessageBoundaryTotal(shareHolderId, _sentMessageCount[shareHolderId] + 1);
            if (nextBoundary > _sentTotal[shareHolderId])
            {
                break;
            }

            _sentMessageCount[shareHolderId]++;
            _sentMessageBoundaryTotal[shareHolderId] = nextBoundary;
        }
    }

    /// <summary>
    /// Whether <paramref name="shareHolderId"/>'s sent total sits exactly on the boundary between two
    /// appended records, rather than partway through one a short write left half-sent (issue #333). A
    /// caller about to write its own bytes to the same stream outside this arena — a delivery-counter
    /// record — asks this first, since bytes written while it is <see langword="false"/> land inside
    /// that record on the wire. True when nothing has been appended, and whenever everything appended
    /// has been sent.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public bool IsSentOnMessageBoundary(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        return _sentTotal[shareHolderId] == _sentMessageBoundaryTotal[shareHolderId];
    }

    /// <summary>
    /// Releases <paramref name="shareHolderId"/>'s retained bytes through its <paramref name="messageCount"/>th
    /// appended Message — the sole way <see cref="_releasedTotal"/> ever advances (issue #113), for a
    /// caller that knows what the peer has confirmed receiving in Messages rather than in bytes (issue
    /// #25's delivery counter, issue #110).
    /// </summary>
    /// <remarks>
    /// A <paramref name="messageCount"/> at or below what is already released is
    /// <see cref="MessageReleaseOutcome.Released"/> and a no-op — the peer repeats a counter it has
    /// already reported, because the same number rides both of its emission triggers, and that must
    /// not be an error. A <paramref name="messageCount"/> above what has ever been appended is
    /// <see cref="MessageReleaseOutcome.Refused"/> without touching the arena, distinguishably from
    /// the no-op case: it means the peer claims to have received Messages this share-holder never
    /// sent, which is this call's caller's protocol violation to act on, not something to clamp away
    /// silently. A count whose byte boundary lies past what <see cref="MarkSent"/> has recorded is
    /// refused on the same footing — the peer cannot have received bytes this end never handed to its
    /// transport, and admitting one would let the released total overtake the sent total and hand
    /// <see cref="TryAppend"/> room over bytes still queued for <see cref="GetUnsent"/>. Otherwise the
    /// share-holder's released total advances to the byte position recorded
    /// for its <paramref name="messageCount"/>th Message — never backwards — and its released-Message
    /// total advances to match. This is entirely independent of <see cref="MarkSent"/>: what a peer has
    /// confirmed receiving and what this end has merely handed to its own transport are different
    /// facts (see the class remarks on the two cursors), and a caller's own Goodbye sentinel — appended
    /// through the same <see cref="TryAppend"/> a Message is — is freed by <see cref="MarkSent"/> alone,
    /// since no legitimate <paramref name="messageCount"/> ever names it.
    /// </remarks>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="messageCount">
    /// The cumulative count of Messages, 1-based from this share-holder's first ever appended
    /// Message, to release through.
    /// </param>
    public MessageReleaseOutcome ReleaseThroughMessageCount(int shareHolderId, ulong messageCount)
    {
        ValidateShareHolderId(shareHolderId);

        if (messageCount > _appendedMessageCount[shareHolderId])
        {
            return MessageReleaseOutcome.Refused;
        }

        if (messageCount <= _releasedMessageCount[shareHolderId])
        {
            return MessageReleaseOutcome.Released;
        }

        int boundaryIndex = BoundaryIndex(shareHolderId, messageCount);

        // Reconstructs the boundary's full ulong position from its stored low 32 bits and the
        // share-holder's current written total: `delta`, computed by ordinary uint wraparound
        // subtraction, is exactly `writtenTotal - boundary` as long as that true difference is under
        // 2^32 (see the class remarks for why it always is). Subtracting `delta` back out of
        // `writtenTotal` — rather than reconstructing the boundary's own high bits directly — is what
        // keeps this correct without caring which side of a 32-bit rollover either value falls on.
        ulong writtenTotal = _writtenTotal[shareHolderId];
        uint delta = unchecked((uint)writtenTotal - _messageBoundaryLow32[boundaryIndex]);
        ulong boundaryBytes = writtenTotal - delta;

        // A peer cannot have received bytes this end never put on the wire, so a boundary past the sent
        // total is refused for the same reason a count past the appended total is: the peer is claiming
        // something impossible. Checking the appended total alone is not enough, because appended runs
        // ahead of sent the moment a socket write goes short — precisely when retention matters. Without
        // this, a peer confirming an appended-but-unsent Message would advance the released total past
        // the sent total, and since TryAppend's room check is `written - released`, the very next Send
        // would be handed room over bytes still queued for GetUnsent and overwrite them — outbound
        // stream corruption a remote could provoke at will.
        if (boundaryBytes > _sentTotal[shareHolderId])
        {
            return MessageReleaseOutcome.Refused;
        }

        if (boundaryBytes > _releasedTotal[shareHolderId])
        {
            _releasedTotal[shareHolderId] = boundaryBytes;
        }

        _releasedMessageCount[shareHolderId] = messageCount;

        return MessageReleaseOutcome.Released;
    }

    /// <summary>
    /// Moves <paramref name="shareHolderId"/>'s sent total back to the byte boundary of its
    /// <paramref name="messageCount"/>th appended Message — the rewind a Resume needs (issue #127):
    /// the peer's reported delivery counter names how far it actually got, not what this end's own
    /// socket merely accepted, and TCP's own acknowledgement can run ahead of that by everything still
    /// sitting in a send buffer the far application never saw. Once rewound, the bytes between the new
    /// sent total and the old one are exactly what <see cref="GetUnsent"/> hands to a replacement
    /// transport next — already framed, nothing reconstructed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two refusals, both leaving the arena untouched and reporting <paramref name="bytesRewound"/> as
    /// 0. A <paramref name="messageCount"/> above <see cref="AppendedMessageCount"/> is
    /// <see cref="RetentionRewindOutcome.AheadOfAppended"/> — the peer claims a position this end never
    /// sent to. A <paramref name="messageCount"/> below the share-holder's released-Message total is
    /// <see cref="RetentionRewindOutcome.BelowReleasedHead"/> — the peer's own confirmation already
    /// freed those bytes, so honouring a resume point behind them would resume into a hole this ring no
    /// longer has (issue #130's gap case). Neither ever touches <see cref="_releasedTotal"/>,
    /// <see cref="_writtenTotal"/>, <see cref="_highWaterBytes"/>, or either Message count: a rewind
    /// moves only where this end reads from next, never what the peer has already confirmed or what
    /// this end has ever appended.
    /// </para>
    /// <para>
    /// Otherwise <see cref="RetentionRewindOutcome.Rewound"/>: <paramref name="messageCount"/> resolves
    /// to a byte boundary exactly as <see cref="ReleaseThroughMessageCount"/> resolves one — 0 names the
    /// share-holder's own released total, the head the peer has confirmed nothing beyond — and the sent
    /// total moves back to it only if it is strictly behind that boundary. A boundary at or ahead of the
    /// current sent total leaves it alone rather than pushing it forward: a rewind only ever un-sends
    /// bytes this end already believed delivered, never advances past what this end has actually handed
    /// its own transport.
    /// </para>
    /// </remarks>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="messageCount">
    /// The peer's reported delivery counter: the cumulative count of Messages, 1-based from this
    /// share-holder's first ever appended Message, the peer claims to have already received.
    /// </param>
    /// <param name="bytesRewound">
    /// How many bytes the sent total moved back by — the figure a caller reports as a Resume's own
    /// <c>bytesRewound</c> (issue #124). 0 on either refusal, and 0 on a legitimate
    /// <see cref="RetentionRewindOutcome.Rewound"/> whose boundary was already at or behind the sent
    /// total — the peer's counter matched or trailed what this end had not yet even sent, so there is
    /// nothing to un-send.
    /// </param>
    public RetentionRewindOutcome RewindSentToMessageCount(int shareHolderId, ulong messageCount, out ulong bytesRewound)
    {
        ValidateShareHolderId(shareHolderId);

        if (messageCount > _appendedMessageCount[shareHolderId])
        {
            bytesRewound = 0;
            return RetentionRewindOutcome.AheadOfAppended;
        }

        if (messageCount < _releasedMessageCount[shareHolderId])
        {
            bytesRewound = 0;
            return RetentionRewindOutcome.BelowReleasedHead;
        }

        ulong boundaryBytes;
        if (messageCount == 0)
        {
            boundaryBytes = _releasedTotal[shareHolderId];
        }
        else
        {
            // Reconstructs the boundary's full ulong position exactly as ReleaseThroughMessageCount
            // does — see that method's own remarks for why the 32-bit wraparound subtraction is
            // unambiguous here.
            int boundaryIndex = BoundaryIndex(shareHolderId, messageCount);
            ulong writtenTotal = _writtenTotal[shareHolderId];
            uint delta = unchecked((uint)writtenTotal - _messageBoundaryLow32[boundaryIndex]);
            boundaryBytes = writtenTotal - delta;
        }

        if (boundaryBytes < _sentTotal[shareHolderId])
        {
            bytesRewound = _sentTotal[shareHolderId] - boundaryBytes;
            _sentTotal[shareHolderId] = boundaryBytes;
            _sentMessageCount[shareHolderId] = messageCount;
            _sentMessageBoundaryTotal[shareHolderId] = boundaryBytes;
        }
        else
        {
            bytesRewound = 0;
        }

        return RetentionRewindOutcome.Rewound;
    }

    /// <summary>
    /// The cumulative count of Messages <paramref name="shareHolderId"/> has ever had accepted by
    /// <see cref="TryAppend"/>, since construction or its last <see cref="ResetShareHolder"/> — the
    /// upper bound <see cref="ReleaseThroughMessageCount"/> refuses above.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public ulong AppendedMessageCount(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        return _appendedMessageCount[shareHolderId];
    }

    /// <summary>
    /// The largest number of bytes <paramref name="shareHolderId"/> has ever retained at once,
    /// since construction or its last <see cref="ResetShareHolder"/>.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public ulong HighWaterBytes(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        return _highWaterBytes[shareHolderId];
    }

    /// <summary>
    /// Zeroes <paramref name="shareHolderId"/>'s written total, sent total, released total, high-water
    /// mark, and Message counts — used when its slot is reused by a new connection, so the previous
    /// occupant's history does not bleed into the new one's. The Message boundary ring itself is left
    /// alone: every entry is overwritten by <see cref="TryAppend"/> before
    /// <see cref="AppendedMessageCount"/> can reach it again, since that total is reset to zero here
    /// too.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public void ResetShareHolder(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        _writtenTotal[shareHolderId] = 0;
        _sentTotal[shareHolderId] = 0;
        _releasedTotal[shareHolderId] = 0;
        _highWaterBytes[shareHolderId] = 0;
        _appendedMessageCount[shareHolderId] = 0;
        _releasedMessageCount[shareHolderId] = 0;
        _sentMessageCount[shareHolderId] = 0;
        _sentMessageBoundaryTotal[shareHolderId] = 0;
    }

    /// <summary>
    /// Where <paramref name="shareHolderId"/>'s <paramref name="messageOrdinal"/>th appended Message
    /// (1-based) records its byte boundary in <see cref="_messageBoundaryLow32"/>. The single place
    /// that arithmetic lives: <see cref="TryAppend"/> writes an entry and
    /// <see cref="ReleaseThroughMessageCount"/> reads it back, and the two drifting apart would not
    /// fail loudly — it would silently resolve a confirmation to the wrong Message's boundary and
    /// release the wrong extent.
    /// </summary>
    private int BoundaryIndex(int shareHolderId, ulong messageOrdinal) =>
        (shareHolderId * _boundaryEntriesPerShareHolder)
        + (int)((messageOrdinal - 1) % (ulong)_boundaryEntriesPerShareHolder);

    /// <summary>
    /// The full byte position of <paramref name="shareHolderId"/>'s <paramref name="messageOrdinal"/>th
    /// appended Message's end, reconstructed from its stored low 32 bits exactly as
    /// <see cref="ReleaseThroughMessageCount"/> does. Valid only for a Message whose ring entry has not
    /// been overwritten, which holds for any Message not yet released.
    /// </summary>
    private ulong MessageBoundaryTotal(int shareHolderId, ulong messageOrdinal)
    {
        ulong writtenTotal = _writtenTotal[shareHolderId];
        uint delta = unchecked((uint)writtenTotal - _messageBoundaryLow32[BoundaryIndex(shareHolderId, messageOrdinal)]);
        return writtenTotal - delta;
    }

    private void ValidateShareHolderId(int shareHolderId)
    {
        if (shareHolderId < 0 || shareHolderId >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shareHolderId),
                shareHolderId,
                $"share-holder {shareHolderId} is outside the arena's capacity of {Capacity}.");
        }
    }
}
