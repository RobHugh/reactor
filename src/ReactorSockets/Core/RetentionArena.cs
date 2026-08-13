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
/// Message is recorded in a ring sized <see cref="BoundPerShareHolder"/> entries per share-holder —
/// the most Messages that could ever be outstanding at once, since <see cref="TryAppend"/> refuses an
/// empty write and retained bytes can never exceed <see cref="BoundPerShareHolder"/>, so at most one
/// entry is needed per byte of the region. That is looser than any real framing (every Message this
/// library frames carries at least a length prefix), but this type has no framing knowledge to
/// tighten it with — the bound it can prove is the one it uses.
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

    /// <summary>
    /// Constructs an arena of <paramref name="capacity"/> fixed-size regions, each
    /// <paramref name="boundPerShareHolder"/> bytes.
    /// </summary>
    /// <param name="boundPerShareHolder">
    /// The number of bytes each share-holder's region holds. Rejected below 1, which can never
    /// hold anything.
    /// </param>
    /// <param name="capacity">The number of share-holders this arena divides its regions across.</param>
    public RetentionArena(int boundPerShareHolder, int capacity)
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

        BoundPerShareHolder = boundPerShareHolder;
        Capacity = capacity;
        _buffer = new byte[boundPerShareHolder * capacity];
        _writtenTotal = new ulong[capacity];
        _releasedTotal = new ulong[capacity];
        _highWaterBytes = new ulong[capacity];
        _appendedMessageCount = new ulong[capacity];
        _releasedMessageCount = new ulong[capacity];
        _messageBoundaryBytes = new ulong[boundPerShareHolder * capacity];
    }

    /// <summary>The number of bytes each share-holder's region holds.</summary>
    public int BoundPerShareHolder { get; }

    /// <summary>The number of share-holders this arena divides its regions across.</summary>
    public int Capacity { get; }

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
    /// The already-framed bytes to retain. Rejected when empty — an empty write has no byte extent
    /// to give the appended-Message ring a distinct entry for, and admitting one would let an
    /// unbounded number of them share a single byte of the region, breaking the bound that ring is
    /// sized on (see the class remarks).
    /// </param>
    public RetentionOutcome TryAppend(int shareHolderId, ReadOnlySpan<byte> framedBytes)
    {
        ValidateShareHolderId(shareHolderId);

        if (framedBytes.Length == 0)
        {
            throw new ArgumentException(
                "framedBytes cannot be empty — an empty write has no Message boundary to record.",
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
        int boundaryIndex = shareHolderId * BoundPerShareHolder
            + (int)((_appendedMessageCount[shareHolderId] - 1) % (ulong)BoundPerShareHolder);
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

        int boundaryIndex = shareHolderId * BoundPerShareHolder + (int)((messageCount - 1) % (ulong)BoundPerShareHolder);
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
