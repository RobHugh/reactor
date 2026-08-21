namespace ReactorSockets.Core;

/// <summary>
/// The send side's uniform-slot preallocation (issue #43, 6.5): one buffer, sliced into
/// fixed-size fragment slots, one run of slots per reliable Space per share-holder.
/// <see cref="TryAcquire"/> is the only way a slot becomes occupied; <see cref="Release"/> is
/// the only way one is freed; <see cref="TryRead"/> reads one back for retransmission.
/// </summary>
/// <remarks>
/// <see cref="RetentionArena"/>'s ring does not port here. Its head pointer works because the
/// stream Session profile's acknowledgement is a single cumulative count released strictly in
/// order. The datagram Session profile's reliable Spaces acknowledge with a base plus a bitfield
/// and <c>ReliableUnordered</c> releases out of order by design (issue #43), so a head pointer
/// would either stall behind a hole still outstanding or release bytes a peer has not actually
/// confirmed. What the datagram profile has instead is that everything a reliable Space retains is
/// at most one fragment — never a multi-fragment Message's worth of contiguous bytes — so a slot
/// per outstanding fragment, addressed directly rather than walked, is enough. That is the same
/// shape <see cref="ReceiveArena"/> already uses for the receive side, so this type mirrors its
/// addressing convention — a zero-based share-holder index, a zero-based Space index, a zero-based
/// slot index — rather than inventing a second one.
/// </remarks>
internal sealed class RetentionPool
{
    private readonly byte[] _buffer;

    /// <summary>
    /// Whether the slot at a given per-slot index currently holds a retained fragment. Parallel to
    /// <see cref="_slotSequence"/> and <see cref="_slotLength"/>, indexed the same way — one entry
    /// per share-holder × Space × slot, never per byte, so it stays small regardless of
    /// <see cref="FragmentSize"/>.
    /// </summary>
    private readonly bool[] _occupied;

    /// <summary>The sequence number occupying a slot, valid only where <see cref="_occupied"/> is set.</summary>
    private readonly uint[] _slotSequence;

    /// <summary>The byte length written into a slot, valid only where <see cref="_occupied"/> is set.</summary>
    private readonly int[] _slotLength;

    /// <summary>
    /// The bytes a share-holder currently has retained, summed across both reliable Spaces — one entry
    /// per share-holder, not per Space, since <see cref="HighWaterBytes"/> answers a question about one
    /// share-holder's memory (issue #43) rather than one Space's bound. Kept as a running total rather
    /// than resummed from <see cref="_slotLength"/> on every <see cref="TryAcquire"/>/<see cref="Release"/>,
    /// so both stay a single add or subtract regardless of <see cref="SlotsPerSpace"/>.
    /// </summary>
    private readonly int[] _retainedBytes;

    /// <summary>
    /// The most bytes each share-holder has ever had retained at once, summed across both reliable
    /// Spaces — never decremented by <see cref="Release"/>, only ever raised by <see cref="TryAcquire"/>.
    /// </summary>
    private readonly ulong[] _highWaterBytes;

    /// <summary>
    /// Constructs a pool of <paramref name="capacity"/> share-holders, each holding
    /// <paramref name="reliableSpaceCount"/> reliable Spaces, every Space a run of
    /// <paramref name="slotsPerSpace"/> fixed-size slots — <paramref name="fragmentSize"/> bytes
    /// wide.
    /// </summary>
    /// <param name="capacity">The number of share-holders this pool divides its regions across.</param>
    /// <param name="reliableSpaceCount">
    /// The number of reliable Spaces each share-holder's region is divided into. In practice always
    /// 2 — the reliable pair of the four Spaces ADR 0009 defines, the same count
    /// <see cref="ReceiveArena.RetainingSpaceCount"/> uses for the receive side.
    /// </param>
    /// <param name="slotsPerSpace">
    /// The number of fragment slots one Space's run holds — <see cref="DatagramSessionOptions.RetentionSlots"/>
    /// in practice.
    /// </param>
    /// <param name="fragmentSize">The number of bytes one fragment slot holds.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Any parameter is less than 1, or the total byte count this pool would need exceeds
    /// <see cref="Array.MaxLength"/>.
    /// </exception>
    public RetentionPool(int capacity, int reliableSpaceCount, int slotsPerSpace, int fragmentSize)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"capacity of {capacity} admits no share-holders — capacity must be at least 1.");
        }

        if (reliableSpaceCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(reliableSpaceCount),
                reliableSpaceCount,
                $"reliable Space count of {reliableSpaceCount} holds no runs — reliable Space count must be at least 1.");
        }

        if (slotsPerSpace < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slotsPerSpace),
                slotsPerSpace,
                $"a run of {slotsPerSpace} slots cannot hold anything — slots per Space must be at least 1.");
        }

        if (fragmentSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentSize),
                fragmentSize,
                $"a fragment slot of {fragmentSize} bytes cannot hold anything — it must be at least 1.");
        }

        Capacity = capacity;
        ReliableSpaceCount = reliableSpaceCount;
        SlotsPerSpace = slotsPerSpace;
        FragmentSize = fragmentSize;

        long totalBytes = (long)capacity * reliableSpaceCount * slotsPerSpace * fragmentSize;
        if (totalBytes > Array.MaxLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                totalBytes,
                $"a retention pool of {capacity} share-holders × {reliableSpaceCount} reliable Spaces × " +
                $"{slotsPerSpace} slots × {fragmentSize} bytes = {totalBytes} bytes cannot be allocated — it " +
                $"exceeds Array.MaxLength ({Array.MaxLength}).");
        }

        _buffer = new byte[totalBytes];

        int slotCount = capacity * reliableSpaceCount * slotsPerSpace;
        _occupied = new bool[slotCount];
        _slotSequence = new uint[slotCount];
        _slotLength = new int[slotCount];

        _retainedBytes = new int[capacity];
        _highWaterBytes = new ulong[capacity];
    }

    /// <summary>The number of share-holders this pool divides its regions across.</summary>
    public int Capacity { get; }

    /// <summary>The number of reliable Spaces each share-holder's region is divided into.</summary>
    public int ReliableSpaceCount { get; }

    /// <summary>The number of fragment slots one Space's run holds.</summary>
    public int SlotsPerSpace { get; }

    /// <summary>The number of bytes one fragment slot holds.</summary>
    public int FragmentSize { get; }

    /// <summary>
    /// The pool's total size in bytes: <see cref="Capacity"/> × <see cref="ReliableSpaceCount"/> ×
    /// <see cref="SlotsPerSpace"/> × <see cref="FragmentSize"/>.
    /// </summary>
    public int TotalSize => _buffer.Length;

    /// <summary>
    /// The most bytes <paramref name="shareHolderId"/> has ever had retained at once, summed across
    /// both reliable Spaces, since this pool was constructed (issue #205, subtask 6.5.6). Rises only on
    /// <see cref="TryAcquire"/>; <see cref="Release"/> never lowers it, matching
    /// <see cref="RetentionArena.HighWaterBytes(int)"/>'s monotonic contract on the stream profile's
    /// ring.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public ulong HighWaterBytes(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        return _highWaterBytes[shareHolderId];
    }

    /// <summary>
    /// The fixed-size slot for <paramref name="slotIndex"/> within <paramref name="shareHolderId"/>'s
    /// <paramref name="spaceIndex"/>th reliable Space run. Nothing here clears a slot's previous
    /// contents on reuse; a caller that reuses a share-holder's region for a new connection is
    /// responsible for not trusting stale bytes it never wrote, exactly as
    /// <see cref="RetentionArena.ResetShareHolder"/> resets counters rather than the backing bytes.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="ReliableSpaceCount"/>.</param>
    /// <param name="slotIndex">A zero-based index below <see cref="SlotsPerSpace"/>.</param>
    public Span<byte> GetSlot(int shareHolderId, int spaceIndex, int slotIndex)
    {
        int regionBase = ValidateRegion(shareHolderId, spaceIndex);

        if (slotIndex < 0 || slotIndex >= SlotsPerSpace)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slotIndex),
                slotIndex,
                $"slot {slotIndex} is outside this pool's run of {SlotsPerSpace} slots per Space.");
        }

        int offset = (regionBase + slotIndex) * FragmentSize;
        return _buffer.AsSpan(offset, FragmentSize);
    }

    /// <summary>
    /// Attempts to copy <paramref name="payload"/> into a free slot of <paramref name="shareHolderId"/>'s
    /// <paramref name="spaceIndex"/>th reliable Space run, recording <paramref name="sequenceNumber"/>
    /// and the byte length written against that slot. Refused as <see cref="RetentionSlotOutcome.Exhausted"/>,
    /// without touching the pool, once every slot in that Space is already occupied.
    /// </summary>
    /// <remarks>
    /// Sequence-to-slot lookup — here, a free-slot search — is a linear scan over
    /// <see cref="SlotsPerSpace"/> entries rather than a dictionary keyed by sequence number.
    /// <see cref="SlotsPerSpace"/> is small and bounded (issue #200 derives it from a byte budget), so
    /// the scan costs less than a hash lookup and allocates nothing on the hot path.
    /// </remarks>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="ReliableSpaceCount"/>.</param>
    /// <param name="sequenceNumber">The sequence number the acquired slot is recorded under.</param>
    /// <param name="payload">
    /// The fragment bytes to retain. Must not exceed <see cref="FragmentSize"/> — a caller sending
    /// more than the fragment size it declared is a programming error, not an exhaustion outcome, so
    /// this throws rather than truncating or overflowing into the neighbouring slot.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="payload"/> is longer than <see cref="FragmentSize"/>.</exception>
    public RetentionSlotOutcome TryAcquire(int shareHolderId, int spaceIndex, uint sequenceNumber, ReadOnlySpan<byte> payload)
    {
        int regionBase = ValidateRegion(shareHolderId, spaceIndex);

        if (payload.Length > FragmentSize)
        {
            throw new ArgumentException(
                $"payload of {payload.Length} bytes exceeds this pool's fragment size of {FragmentSize} bytes.",
                nameof(payload));
        }

        int freeSlot = -1;
        for (int slotIndex = 0; slotIndex < SlotsPerSpace; slotIndex++)
        {
            if (!_occupied[regionBase + slotIndex])
            {
                freeSlot = slotIndex;
                break;
            }
        }

        if (freeSlot < 0)
        {
            return RetentionSlotOutcome.Exhausted;
        }

        int offset = (regionBase + freeSlot) * FragmentSize;
        payload.CopyTo(_buffer.AsSpan(offset, payload.Length));

        int stateIndex = regionBase + freeSlot;
        _occupied[stateIndex] = true;
        _slotSequence[stateIndex] = sequenceNumber;
        _slotLength[stateIndex] = payload.Length;

        _retainedBytes[shareHolderId] += payload.Length;
        if ((ulong)_retainedBytes[shareHolderId] > _highWaterBytes[shareHolderId])
        {
            _highWaterBytes[shareHolderId] = (ulong)_retainedBytes[shareHolderId];
        }

        return RetentionSlotOutcome.Committed;
    }

    /// <summary>
    /// Frees the slot in <paramref name="shareHolderId"/>'s <paramref name="spaceIndex"/>th reliable
    /// Space run that holds <paramref name="sequenceNumber"/>, making it immediately available to
    /// the next <see cref="TryAcquire"/> in that Space. A no-op when no occupied slot in that Space
    /// holds <paramref name="sequenceNumber"/> — a duplicate acknowledgement releasing a sequence
    /// already released, or never retained, is ordinary and not an error.
    /// </summary>
    /// <remarks>
    /// Release, like acquire, is a linear scan over <see cref="SlotsPerSpace"/> entries rather than a
    /// dictionary keyed by sequence number — <c>ReliableUnordered</c> confirms delivery out of order
    /// by design, so a release can name any occupied slot in the Space, not just the oldest, and the
    /// scan is cheap and allocation-free for the same reason <see cref="TryAcquire"/>'s is.
    /// </remarks>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="ReliableSpaceCount"/>.</param>
    /// <param name="sequenceNumber">The sequence number of the slot to free.</param>
    public void Release(int shareHolderId, int spaceIndex, uint sequenceNumber)
    {
        int regionBase = ValidateRegion(shareHolderId, spaceIndex);

        for (int slotIndex = 0; slotIndex < SlotsPerSpace; slotIndex++)
        {
            int stateIndex = regionBase + slotIndex;
            if (_occupied[stateIndex] && _slotSequence[stateIndex] == sequenceNumber)
            {
                _occupied[stateIndex] = false;
                _retainedBytes[shareHolderId] -= _slotLength[stateIndex];
                return;
            }
        }
    }

    /// <summary>
    /// Attempts to read back the fragment retained under <paramref name="sequenceNumber"/> in
    /// <paramref name="shareHolderId"/>'s <paramref name="spaceIndex"/>th reliable Space run, for
    /// the retransmit path to resend. Returns <see langword="false"/>, with <paramref name="payload"/>
    /// left empty, when no occupied slot in that Space holds <paramref name="sequenceNumber"/> — the
    /// sequence was never acquired, or it already was and then released.
    /// </summary>
    /// <remarks>
    /// Like <see cref="TryAcquire"/> and <see cref="Release"/>, this is a linear scan over
    /// <see cref="SlotsPerSpace"/> entries rather than a dictionary keyed by sequence number, for the
    /// same reason: <see cref="SlotsPerSpace"/> is small and bounded, so the scan is cheaper than a
    /// hash lookup and allocates nothing. The returned span aliases the pool's backing buffer
    /// directly — it is exactly <paramref name="payload"/>'s written length, not the whole slot — so
    /// this allocates nothing either.
    /// </remarks>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="ReliableSpaceCount"/>.</param>
    /// <param name="sequenceNumber">The sequence number of the retained fragment to read.</param>
    /// <param name="payload">
    /// On success, the retained bytes as a read-only span of exactly the length written. On a miss,
    /// an empty span.
    /// </param>
    public bool TryRead(int shareHolderId, int spaceIndex, uint sequenceNumber, out ReadOnlySpan<byte> payload)
    {
        int regionBase = ValidateRegion(shareHolderId, spaceIndex);

        for (int slotIndex = 0; slotIndex < SlotsPerSpace; slotIndex++)
        {
            int stateIndex = regionBase + slotIndex;
            if (_occupied[stateIndex] && _slotSequence[stateIndex] == sequenceNumber)
            {
                int offset = stateIndex * FragmentSize;
                payload = _buffer.AsSpan(offset, _slotLength[stateIndex]);
                return true;
            }
        }

        payload = ReadOnlySpan<byte>.Empty;
        return false;
    }

    /// <summary>
    /// Frees every slot <paramref name="shareHolderId"/> holds, in both reliable Spaces, and returns
    /// its retained-byte and <see cref="HighWaterBytes"/> figures to zero — what a Slot a new Peer
    /// reuses needs before it sends anything, mirroring
    /// <see cref="RetentionArena.ResetShareHolder"/> on the stream profile's ring and
    /// <see cref="Session.ReceiveWindowSpaces.Reset"/> on the receive side. Without it a Slot whose
    /// previous occupant left with fragments still retained would refuse the new Peer's very first
    /// reliable Send as <see cref="RetentionSlotOutcome.Exhausted"/> and report the old Peer's
    /// high-water as the new one's.
    /// </summary>
    /// <remarks>
    /// Only the occupancy flags are cleared. <see cref="_slotSequence"/> and
    /// <see cref="_slotLength"/> are read only where <see cref="_occupied"/> is set, and the backing
    /// bytes are never read back except through a slot this pool itself wrote — the same reason
    /// <see cref="RetentionArena.ResetShareHolder"/> resets counters rather than the bytes behind
    /// them.
    /// </remarks>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    public void ResetShareHolder(int shareHolderId)
    {
        ValidateShareHolderId(shareHolderId);

        int firstSlot = shareHolderId * ReliableSpaceCount * SlotsPerSpace;
        Array.Clear(_occupied, firstSlot, ReliableSpaceCount * SlotsPerSpace);

        _retainedBytes[shareHolderId] = 0;
        _highWaterBytes[shareHolderId] = 0;
    }

    /// <summary>
    /// Validates <paramref name="shareHolderId"/> and <paramref name="spaceIndex"/> and returns the
    /// Space run's base index into this pool's per-slot arrays — <see cref="_buffer"/> sliced by
    /// <see cref="FragmentSize"/>, <see cref="_occupied"/>/<see cref="_slotSequence"/>/<see cref="_slotLength"/>
    /// sliced directly — so every caller computes the same region the same way.
    /// </summary>
    private int ValidateRegion(int shareHolderId, int spaceIndex)
    {
        ValidateShareHolderId(shareHolderId);

        if (spaceIndex < 0 || spaceIndex >= ReliableSpaceCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(spaceIndex),
                spaceIndex,
                $"Space {spaceIndex} is outside this pool's reliable Space count of {ReliableSpaceCount}.");
        }

        return ((shareHolderId * ReliableSpaceCount) + spaceIndex) * SlotsPerSpace;
    }

    /// <summary>
    /// Validates <paramref name="shareHolderId"/> alone, for the callers — <see cref="HighWaterBytes"/>
    /// and <see cref="ValidateRegion"/> — that need the bound checked without also naming a Space.
    /// </summary>
    private void ValidateShareHolderId(int shareHolderId)
    {
        if (shareHolderId < 0 || shareHolderId >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shareHolderId),
                shareHolderId,
                $"share-holder {shareHolderId} is outside the pool's capacity of {Capacity}.");
        }
    }
}
