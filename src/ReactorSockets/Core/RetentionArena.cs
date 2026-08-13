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
/// <paramref name="boundPerShareHolder"/> and <paramref name="capacity"/>: a caller that understates
/// it breaks the bound the ring is sized on, exactly as a caller that understated
/// <paramref name="boundPerShareHolder"/> would break <see cref="TryAppend"/>'s exhaustion check.
/// </para>
/// <para>
/// <b>The Goodbye sentinel needs no reconciling against a peer's Message count (issue #113).</b>
/// <see cref="TryAppend"/> is called for a Session profile's own outbound Goodbye sentinel exactly as
/// it is for an application Message, so the appended-Message count can run one ahead of the count of
/// Messages a caller would ever describe to <see cref="ReleaseThroughMessageCount"/>. That still needs
/// no offset: a caller only ever appends its Goodbye once every Message it will ever send has already
/// been appended (a Session profile refuses further Sends the moment its own Goodbye is queued), so
/// the Goodbye's boundary entry always sits strictly after every Message's — a legitimate
/// <paramref name="messageCount"/>, which counts only Messages, can never reach it, and every count
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

        // Ceiling division in `long`, not `int`: capacity and boundPerShareHolder are each caller
        // controlled and their product — let alone their product times 4 bytes per ring entry — can
        // exceed int.MaxLength well before it would exceed Array.MaxLength, which is what the checked
        // cast below actually enforces (the same limit `new uint[...]` would throw against anyway,
        // just with a clearer failure than a silently wrapped negative length).
        long boundaryEntriesPerShareHolder =
            ((long)boundPerShareHolder + minimumFramedMessageSize - 1) / minimumFramedMessageSize;
        _boundaryEntriesPerShareHolder = checked((int)boundaryEntriesPerShareHolder);

        _buffer = new byte[boundPerShareHolder * capacity];
        _writtenTotal = new ulong[capacity];
        _sentTotal = new ulong[capacity];
        _releasedTotal = new ulong[capacity];
        _highWaterBytes = new ulong[capacity];
        _appendedMessageCount = new ulong[capacity];
        _releasedMessageCount = new ulong[capacity];
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
        int boundaryIndex = shareHolderId * _boundaryEntriesPerShareHolder
            + (int)((_appendedMessageCount[shareHolderId] - 1) % (ulong)_boundaryEntriesPerShareHolder);
        _messageBoundaryLow32[boundaryIndex] = unchecked((uint)_writtenTotal[shareHolderId]);

        return RetentionOutcome.Committed;
    }

    /// <summary>
    /// Returns <paramref name="shareHolderId"/>'s unsent bytes — from its sent total to its written
    /// total — as one or two contiguous memory segments over the backing array, for a caller about to
    /// hand them to its own transport. <paramref name="second"/> is <see langword="default"/> unless
    /// the unsent range wraps past the region's end boundary, in which case it holds the wrapped
    /// remainder. Deliberately independent of what the peer has confirmed
    /// (<see cref="ReleaseThroughMessageCount"/>): see the class remarks on the two cursors past the
    /// written total.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public (ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second) GetUnsent(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        ulong unsent = _writtenTotal[shareHolderId] - _sentTotal[shareHolderId];
        int regionStart = shareHolderId * BoundPerShareHolder;
        int position = (int)(_sentTotal[shareHolderId] % (ulong)BoundPerShareHolder);
        int spaceToRegionEnd = BoundPerShareHolder - position;

        if (unsent <= (ulong)spaceToRegionEnd)
        {
            return (_buffer.AsMemory(regionStart + position, (int)unsent), default);
        }

        ReadOnlyMemory<byte> first = _buffer.AsMemory(regionStart + position, spaceToRegionEnd);
        ReadOnlyMemory<byte> second = _buffer.AsMemory(regionStart, (int)unsent - spaceToRegionEnd);
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
    /// silently. Otherwise the share-holder's released total advances to the byte position recorded
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

        int boundaryIndex = shareHolderId * _boundaryEntriesPerShareHolder
            + (int)((messageCount - 1) % (ulong)_boundaryEntriesPerShareHolder);

        // Reconstructs the boundary's full ulong position from its stored low 32 bits and the
        // share-holder's current written total: `delta`, computed by ordinary uint wraparound
        // subtraction, is exactly `writtenTotal - boundary` as long as that true difference is under
        // 2^32 (see the class remarks for why it always is). Subtracting `delta` back out of
        // `writtenTotal` — rather than reconstructing the boundary's own high bits directly — is what
        // keeps this correct without caring which side of a 32-bit rollover either value falls on.
        ulong writtenTotal = _writtenTotal[shareHolderId];
        uint delta = unchecked((uint)writtenTotal - _messageBoundaryLow32[boundaryIndex]);
        ulong boundaryBytes = writtenTotal - delta;
        if (boundaryBytes > _releasedTotal[shareHolderId])
        {
            _releasedTotal[shareHolderId] = boundaryBytes;
        }

        _releasedMessageCount[shareHolderId] = messageCount;

        return MessageReleaseOutcome.Released;
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
