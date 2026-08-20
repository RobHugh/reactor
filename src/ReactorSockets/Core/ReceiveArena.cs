namespace ReactorSockets.Core;

/// <summary>
/// The receive side of the memory bill (issue #171), actually allocated: one buffer per Reactor,
/// sliced into one fixed-size region per (Peer, reliable Space) pair, sized exactly
/// <see cref="DatagramSessionOptions.ReceiveBudgetBytes"/> — <c>Capacity × SpaceCount ×
/// FragmentsPerSpace × FragmentSize</c>. Preallocated once at construction and never grown: the
/// receive window is the library's main memory dial, taken once and never seen to grow. Only the two
/// reliable Spaces get window storage here — the unreliable Spaces classify sequences
/// (<see cref="ReceiveWindow"/>) but retain no payload, which is why a caller passes 2 for
/// <c>spaceCount</c> rather than the full 4 (ADR 0009).
/// </summary>
/// <remarks>
/// <para>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/> — a Peer, in the
/// datagram Session profile — the same convention <see cref="FrameArena"/> and
/// <see cref="RetentionArena"/> use. A reliable Space within a share-holder's region is identified by
/// a zero-based index below <see cref="SpaceCount"/>, and a fragment slot within that Space's window
/// by a zero-based index below <see cref="FragmentsPerSpace"/>.
/// </para>
/// <para>
/// This type owns only the storage and the addressing arithmetic over it. It carries no notion of a
/// sequence number, a fragment's offset within a Message, or which slot a given sequence belongs in —
/// classifying a sequence into a slot is <see cref="ReceiveWindow"/>'s job (#170), and routing a
/// parsed record's payload into the slot <see cref="ReceiveWindow.Classify"/> names is #172's. Keeping
/// that knowledge out of this type is what keeps the allocation-and-lifetime concern this ticket
/// (#171) owns separate from the classification-and-routing concern #172 owns, on the same file two
/// branches are editing concurrently.
/// </para>
/// </remarks>
internal sealed class ReceiveArena
{
    private readonly byte[] _buffer;

    /// <summary>
    /// Constructs an arena of <paramref name="capacity"/> share-holders, each holding
    /// <paramref name="spaceCount"/> reliable Spaces, each a window of <paramref name="fragmentsPerSpace"/>
    /// fixed-size slots of <paramref name="fragmentSize"/> bytes.
    /// </summary>
    /// <param name="capacity">The number of share-holders this arena divides its regions across.</param>
    /// <param name="spaceCount">
    /// The number of reliable Spaces each share-holder's region is further divided into. In practice
    /// always 2 — the reliable pair of the four Spaces ADR 0009 defines — passed as a parameter rather
    /// than hardcoded so this type states no protocol knowledge of its own.
    /// </param>
    /// <param name="fragmentsPerSpace">
    /// The number of fragment slots one Space's window holds. Profile-floor validation
    /// (<see cref="DatagramSessionOptions.Validate"/>) is the calling Reactor's responsibility; this
    /// constructor only rejects a non-positive count, which can never hold anything.
    /// </param>
    /// <param name="fragmentSize">The number of bytes one fragment slot holds.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Any parameter is less than 1, or the total byte count this arena would need exceeds
    /// <see cref="Array.MaxLength"/>. The latter is arithmetic the calling Reactor's options already
    /// validated against the same bound (<see cref="DatagramSessionOptions.ReceiveBudgetBytes"/>), so
    /// reaching it here would mean that validation was bypassed.
    /// </exception>
    public ReceiveArena(int capacity, int spaceCount, int fragmentsPerSpace, int fragmentSize)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"capacity of {capacity} admits no share-holders — capacity must be at least 1.");
        }

        if (spaceCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(spaceCount),
                spaceCount,
                $"Space count of {spaceCount} holds no windows — Space count must be at least 1.");
        }

        if (fragmentsPerSpace < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentsPerSpace),
                fragmentsPerSpace,
                $"a window of {fragmentsPerSpace} fragment slots cannot hold anything — it must be at least 1.");
        }

        if (fragmentSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentSize),
                fragmentSize,
                $"a fragment slot of {fragmentSize} bytes cannot hold anything — it must be at least 1.");
        }

        Capacity = capacity;
        SpaceCount = spaceCount;
        FragmentsPerSpace = fragmentsPerSpace;
        FragmentSize = fragmentSize;

        long totalBytes = (long)capacity * spaceCount * fragmentsPerSpace * fragmentSize;
        if (totalBytes > Array.MaxLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                totalBytes,
                $"a receive arena of {capacity} share-holders × {spaceCount} Spaces × {fragmentsPerSpace} " +
                $"fragments × {fragmentSize} bytes = {totalBytes} bytes cannot be allocated — it exceeds " +
                $"Array.MaxLength ({Array.MaxLength}).");
        }

        _buffer = new byte[totalBytes];
    }

    /// <summary>The number of share-holders this arena divides its regions across.</summary>
    public int Capacity { get; }

    /// <summary>The number of reliable Spaces each share-holder's region is divided into.</summary>
    public int SpaceCount { get; }

    /// <summary>The number of fragment slots one Space's window holds.</summary>
    public int FragmentsPerSpace { get; }

    /// <summary>The number of bytes one fragment slot holds.</summary>
    public int FragmentSize { get; }

    /// <summary>
    /// The arena's total size in bytes: <see cref="Capacity"/> × <see cref="SpaceCount"/> ×
    /// <see cref="FragmentsPerSpace"/> × <see cref="FragmentSize"/>, exactly matching
    /// <see cref="DatagramSessionOptions.ReceiveBudgetBytes"/> for the options this arena was built
    /// from.
    /// </summary>
    public int TotalSize => _buffer.Length;

    /// <summary>
    /// The fixed-size slot for <paramref name="fragmentIndex"/> within <paramref name="shareHolderId"/>'s
    /// <paramref name="spaceIndex"/>th Space window. Nothing here clears a slot's previous contents on
    /// reuse; a caller that reuses a share-holder's region for a new connection is responsible for not
    /// trusting stale bytes it never wrote, exactly as <see cref="RetentionArena.ResetShareHolder"/>
    /// resets counters rather than the backing bytes.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="SpaceCount"/>.</param>
    /// <param name="fragmentIndex">A zero-based index below <see cref="FragmentsPerSpace"/>.</param>
    public Span<byte> GetFragmentSlot(int shareHolderId, int spaceIndex, int fragmentIndex)
    {
        if (shareHolderId < 0 || shareHolderId >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shareHolderId),
                shareHolderId,
                $"share-holder {shareHolderId} is outside the arena's capacity of {Capacity}.");
        }

        if (spaceIndex < 0 || spaceIndex >= SpaceCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(spaceIndex),
                spaceIndex,
                $"Space {spaceIndex} is outside this arena's Space count of {SpaceCount}.");
        }

        if (fragmentIndex < 0 || fragmentIndex >= FragmentsPerSpace)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentIndex),
                fragmentIndex,
                $"fragment slot {fragmentIndex} is outside this arena's window of {FragmentsPerSpace} slots per Space.");
        }

        int regionStart = ((shareHolderId * SpaceCount) + spaceIndex) * FragmentsPerSpace * FragmentSize;
        int offset = regionStart + (fragmentIndex * FragmentSize);
        return _buffer.AsSpan(offset, FragmentSize);
    }
}
