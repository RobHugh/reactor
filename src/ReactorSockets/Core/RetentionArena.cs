namespace ReactorSockets.Core;

/// <summary>
/// The retention ring's pure-arithmetic core (issue #21, 3.5): one buffer, sliced into one
/// fixed-size region per share-holder (a Peer, in the Session profile), that already-framed bytes
/// a send could not deliver immediately are copied into and released from as delivery confirms.
/// </summary>
/// <remarks>
/// <para>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/>, the same
/// convention <see cref="FrameArena"/> uses. Per-share-holder usage is tracked in preallocated
/// arrays sized once at construction, so nothing here allocates later. Usage is tracked as two
/// <c>ulong</c> monotonically-increasing running totals — bytes ever written and bytes ever
/// released — rather than wrapping head/tail indices (ADR 0015, the same move made for
/// <see cref="ShareHolderTable{T}"/>'s generation counter). 2^64 bytes through one share-holder's
/// region is unreachable by any process that will ever run, so the totals cannot wrap and the ring
/// position derived from them (<c>total % BoundPerShareHolder</c>) is always correct by
/// unreachability rather than by a bound this type has to check for.
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
/// <b>Cost.</b> Each ring entry is one <c>ulong</c> (8 bytes) standing in for one whole Message, so
/// the ring adds <c>8 × Capacity × ⌈BoundPerShareHolder / MinimumFramedMessageSize⌉</c> bytes on top
/// of the <c>Capacity × BoundPerShareHolder</c> the byte buffer already costs — roughly
/// <c>8 / MinimumFramedMessageSize</c> times the retention bill, not the flat 8× a 1-byte-per-entry
/// bound would have cost. The stream Session profile passes its wire format's true floor — the
/// four-byte length prefix alone, since both the Goodbye sentinel and a zero-payload Message write
/// exactly that many bytes and nothing shorter is ever appended — which keeps this at 2× rather than
/// 8×.
/// </para>
/// </remarks>
internal sealed class RetentionArena
{
    private readonly byte[] _buffer;
    private readonly ulong[] _writtenTotal;
    private readonly ulong[] _releasedTotal;
    private readonly ulong[] _highWaterBytes;
    private readonly ulong[] _appendedMessageCount;
    private readonly ulong[] _releasedMessageCount;
    private readonly ulong[] _messageBoundaryBytes;
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
        // controlled and their product — let alone their product times 8 bytes per ring entry — can
        // exceed int.MaxLength well before it would exceed Array.MaxLength, which is what the checked
        // cast below actually enforces (the same limit `new ulong[...]` would throw against anyway,
        // just with a clearer failure than a silently wrapped negative length).
        long boundaryEntriesPerShareHolder =
            ((long)boundPerShareHolder + minimumFramedMessageSize - 1) / minimumFramedMessageSize;
        _boundaryEntriesPerShareHolder = checked((int)boundaryEntriesPerShareHolder);

        _buffer = new byte[boundPerShareHolder * capacity];
        _writtenTotal = new ulong[capacity];
        _releasedTotal = new ulong[capacity];
        _highWaterBytes = new ulong[capacity];
        _appendedMessageCount = new ulong[capacity];
        _releasedMessageCount = new ulong[capacity];
        _messageBoundaryBytes = new ulong[checked(_boundaryEntriesPerShareHolder * capacity)];
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
        _messageBoundaryBytes[boundaryIndex] = _writtenTotal[shareHolderId];

        return RetentionOutcome.Committed;
    }

    /// <summary>
    /// Returns <paramref name="shareHolderId"/>'s unread bytes — from its released total to its
    /// written total — as one or two contiguous memory segments over the backing array.
    /// <paramref name="second"/> is <see langword="default"/> unless the unread range wraps past
    /// the region's end boundary, in which case it holds the wrapped remainder.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public (ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second) GetReadable(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        ulong retained = _writtenTotal[shareHolderId] - _releasedTotal[shareHolderId];
        int regionStart = shareHolderId * BoundPerShareHolder;
        int position = (int)(_releasedTotal[shareHolderId] % (ulong)BoundPerShareHolder);
        int spaceToRegionEnd = BoundPerShareHolder - position;

        if (retained <= (ulong)spaceToRegionEnd)
        {
            return (_buffer.AsMemory(regionStart + position, (int)retained), default);
        }

        ReadOnlyMemory<byte> first = _buffer.AsMemory(regionStart + position, spaceToRegionEnd);
        ReadOnlyMemory<byte> second = _buffer.AsMemory(regionStart, (int)retained - spaceToRegionEnd);
        return (first, second);
    }

    /// <summary>
    /// Advances <paramref name="shareHolderId"/>'s released total by <paramref name="bytesConfirmed"/>,
    /// marking that many of its oldest retained bytes as delivered. Throws — rather than silently
    /// clamping — when <paramref name="bytesConfirmed"/> exceeds what the share-holder currently
    /// retains.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="bytesConfirmed">The number of oldest retained bytes to release.</param>
    public void Release(int shareHolderId, int bytesConfirmed)
    {
        ValidateShareHolderId(shareHolderId);

        ulong retained = _writtenTotal[shareHolderId] - _releasedTotal[shareHolderId];
        if ((ulong)bytesConfirmed > retained)
        {
            throw new ArgumentOutOfRangeException(
                nameof(bytesConfirmed),
                bytesConfirmed,
                $"share-holder {shareHolderId} retains only {retained} bytes — cannot release {bytesConfirmed}.");
        }

        _releasedTotal[shareHolderId] += (ulong)bytesConfirmed;
    }

    /// <summary>
    /// Releases <paramref name="shareHolderId"/>'s retained bytes through its <paramref name="messageCount"/>th
    /// appended Message — the count-based counterpart to <see cref="Release"/>, for a caller that
    /// knows what the peer has confirmed receiving in Messages rather than in bytes (issue #25's
    /// delivery counter, issue #110).
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
    /// for its <paramref name="messageCount"/>th Message — never backwards, even if
    /// <see cref="Release"/>'s independent byte-based bookkeeping had already advanced past that
    /// position — and its released-Message total advances to match.
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
        ulong boundaryBytes = _messageBoundaryBytes[boundaryIndex];
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
    /// Zeroes <paramref name="shareHolderId"/>'s written total, released total, high-water mark, and
    /// Message counts — used when its slot is reused by a new connection, so the previous occupant's
    /// history does not bleed into the new one's. The Message boundary ring itself is left alone:
    /// every entry is overwritten by <see cref="TryAppend"/> before <see cref="AppendedMessageCount"/>
    /// can reach it again, since that total is reset to zero here too.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public void ResetShareHolder(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        _writtenTotal[shareHolderId] = 0;
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
