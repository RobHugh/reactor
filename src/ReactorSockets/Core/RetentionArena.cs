namespace ReactorSockets.Core;

/// <summary>
/// The retention ring's pure-arithmetic core (issue #21, 3.5): one buffer, sliced into one
/// fixed-size region per share-holder (a Peer, in the Session profile), that already-framed bytes
/// a send could not deliver immediately are copied into and released from as delivery confirms.
/// </summary>
/// <remarks>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/>, the same
/// convention <see cref="FrameArena"/> uses. Per-share-holder usage is tracked in preallocated
/// arrays sized once at construction, so nothing here allocates later. Usage is tracked as two
/// <c>ulong</c> monotonically-increasing running totals — bytes ever written and bytes ever
/// released — rather than wrapping head/tail indices (ADR 0015, the same move made for
/// <see cref="ShareHolderTable{T}"/>'s generation counter). 2^64 bytes through one share-holder's
/// region is unreachable by any process that will ever run, so the totals cannot wrap and the ring
/// position derived from them (<c>total % BoundPerShareHolder</c>) is always correct by
/// unreachability rather than by a bound this type has to check for.
/// </remarks>
internal sealed class RetentionArena
{
    private readonly byte[] _buffer;
    private readonly ulong[] _writtenTotal;
    private readonly ulong[] _releasedTotal;
    private readonly ulong[] _highWaterBytes;

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
    /// high-water mark advance, and <see cref="RetentionOutcome.Committed"/> is returned.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="framedBytes">The already-framed bytes to retain.</param>
    public RetentionOutcome TryAppend(int shareHolderId, ReadOnlySpan<byte> framedBytes)
    {
        ValidateShareHolderId(shareHolderId);

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
    /// Zeroes <paramref name="shareHolderId"/>'s written total, released total, and high-water
    /// mark — used when its slot is reused by a new connection, so the previous occupant's history
    /// does not bleed into the new one's.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public void ResetShareHolder(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        _writtenTotal[shareHolderId] = 0;
        _releasedTotal[shareHolderId] = 0;
        _highWaterBytes[shareHolderId] = 0;
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
