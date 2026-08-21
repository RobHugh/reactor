namespace ReactorSockets.Core;

/// <summary>
/// The send side's uniform-slot preallocation (issue #43, 6.5): one buffer, sliced into
/// fixed-size fragment slots, one run of slots per reliable Space per share-holder. Geometry and
/// addressing only — no acquire, no release; a later ticket in the same slice adds those on top of
/// this type's storage.
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
        if (shareHolderId < 0 || shareHolderId >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shareHolderId),
                shareHolderId,
                $"share-holder {shareHolderId} is outside the pool's capacity of {Capacity}.");
        }

        if (spaceIndex < 0 || spaceIndex >= ReliableSpaceCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(spaceIndex),
                spaceIndex,
                $"Space {spaceIndex} is outside this pool's reliable Space count of {ReliableSpaceCount}.");
        }

        if (slotIndex < 0 || slotIndex >= SlotsPerSpace)
        {
            throw new ArgumentOutOfRangeException(
                nameof(slotIndex),
                slotIndex,
                $"slot {slotIndex} is outside this pool's run of {SlotsPerSpace} slots per Space.");
        }

        int regionStart = ((shareHolderId * ReliableSpaceCount) + spaceIndex) * SlotsPerSpace * FragmentSize;
        int offset = regionStart + (slotIndex * FragmentSize);
        return _buffer.AsSpan(offset, FragmentSize);
    }
}
