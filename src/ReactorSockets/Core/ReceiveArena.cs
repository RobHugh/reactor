namespace ReactorSockets.Core;

/// <summary>
/// The receive side of the memory bill (issue #171), actually allocated: one buffer per Reactor,
/// sized exactly <see cref="DatagramSessionOptions.ReceiveBudgetBytes"/> — <c>Capacity ×
/// FragmentsPerSpace × (RetainingSpaceCount × FragmentSize + ClassifyingSpaceCount)</c> — and sliced
/// into two regions (issue #174, #179). The payload region comes first: fixed-size fragment slots,
/// one per (Peer, retaining Space) pair, for the two Spaces that reassemble and so must hold what
/// they receive. The admission region follows: one byte per slot, one run per (Peer, classifying
/// Space) pair, for all four Spaces, because all four classify whether or not they retain — that
/// per-slot admission record is what <see cref="Session.ReceiveWindow"/> previously allocated for
/// itself, outside this arena. Preallocated once at construction and never grown: the receive window
/// is the library's main memory dial, taken once and never seen to grow.
/// </summary>
/// <remarks>
/// <para>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/> — a Peer, in the
/// datagram Session profile — the same convention <see cref="FrameArena"/> and
/// <see cref="RetentionArena"/> use. A Space within a share-holder's payload region is identified by
/// a zero-based index below <see cref="RetainingSpaceCount"/>, and a Space within a share-holder's
/// admission region by a zero-based index below <see cref="ClassifyingSpaceCount"/>; a fragment slot
/// within either by a zero-based index below <see cref="FragmentsPerSpace"/>.
/// </para>
/// <para>
/// This type owns only the storage and the addressing arithmetic over it. It carries no notion of a
/// sequence number, a fragment's offset within a Message, or which slot a given sequence belongs in —
/// classifying a sequence into a slot is <see cref="Session.ReceiveWindow"/>'s job (#170), and routing a
/// parsed record's payload into the slot <see cref="Session.ReceiveWindow.Classify"/> names is #172's. Keeping
/// that knowledge out of this type is what keeps the allocation-and-lifetime concern this ticket
/// (#171) owns separate from the classification-and-routing concern #172 owns, on the same file two
/// branches are editing concurrently.
/// </para>
/// </remarks>
internal sealed class ReceiveArena
{
    private readonly byte[] _buffer;
    private readonly int _admissionOrigin;

    /// <summary>
    /// Constructs an arena of <paramref name="capacity"/> share-holders, each holding
    /// <paramref name="retainingSpaceCount"/> payload-carrying Spaces and
    /// <paramref name="classifyingSpaceCount"/> admission-tracked Spaces, every Space a window of
    /// <paramref name="fragmentsPerSpace"/> slots — <paramref name="fragmentSize"/> bytes wide for a
    /// payload slot, one byte for an admission slot.
    /// </summary>
    /// <param name="capacity">The number of share-holders this arena divides its regions across.</param>
    /// <param name="retainingSpaceCount">
    /// The number of Spaces each share-holder's payload region is divided into: the Spaces that
    /// reassemble and so must hold what they receive. In practice always 2 — the reliable pair of the
    /// four Spaces ADR 0009 defines.
    /// </param>
    /// <param name="classifyingSpaceCount">
    /// The number of Spaces each share-holder's admission region is divided into: every Space that
    /// classifies an arriving sequence, which is all four — a Space that retains classifies too, so
    /// this can never be less than <paramref name="retainingSpaceCount"/>.
    /// </param>
    /// <param name="fragmentsPerSpace">
    /// The number of fragment slots one Space's window holds, payload or admission alike.
    /// Profile-floor validation (<see cref="DatagramSessionOptions.Validate"/>) is the calling
    /// Reactor's responsibility; this constructor only rejects a non-positive count, which can never
    /// hold anything.
    /// </param>
    /// <param name="fragmentSize">The number of bytes one payload fragment slot holds.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Any parameter is less than 1, <paramref name="classifyingSpaceCount"/> is less than
    /// <paramref name="retainingSpaceCount"/>, or the total byte count this arena would need exceeds
    /// <see cref="Array.MaxLength"/>. The latter is arithmetic the calling Reactor's options already
    /// validated against the same bound (<see cref="DatagramSessionOptions.ReceiveBudgetBytes"/>), so
    /// reaching it here would mean that validation was bypassed.
    /// </exception>
    public ReceiveArena(int capacity, int retainingSpaceCount, int classifyingSpaceCount, int fragmentsPerSpace, int fragmentSize)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"capacity of {capacity} admits no share-holders — capacity must be at least 1.");
        }

        if (retainingSpaceCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retainingSpaceCount),
                retainingSpaceCount,
                $"retaining Space count of {retainingSpaceCount} holds no windows — retaining Space count must be at least 1.");
        }

        if (classifyingSpaceCount < retainingSpaceCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(classifyingSpaceCount),
                classifyingSpaceCount,
                $"classifying Space count of {classifyingSpaceCount} is below the retaining Space count of " +
                $"{retainingSpaceCount} — a Space that retains classifies too, so classifying can never be fewer.");
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
        RetainingSpaceCount = retainingSpaceCount;
        ClassifyingSpaceCount = classifyingSpaceCount;
        FragmentsPerSpace = fragmentsPerSpace;
        FragmentSize = fragmentSize;

        long payloadBytes = (long)capacity * retainingSpaceCount * fragmentsPerSpace * fragmentSize;
        long admissionBytes = (long)capacity * classifyingSpaceCount * fragmentsPerSpace;
        long totalBytes = payloadBytes + admissionBytes;
        if (totalBytes > Array.MaxLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                totalBytes,
                $"a receive arena of {capacity} share-holders × {fragmentsPerSpace} fragments × " +
                $"({retainingSpaceCount} retaining Spaces × {fragmentSize} bytes + {classifyingSpaceCount} " +
                $"classifying Spaces × 1 byte) = {totalBytes} bytes cannot be allocated — it exceeds " +
                $"Array.MaxLength ({Array.MaxLength}).");
        }

        _admissionOrigin = (int)payloadBytes;
        _buffer = new byte[totalBytes];
    }

    /// <summary>The number of share-holders this arena divides its regions across.</summary>
    public int Capacity { get; }

    /// <summary>The number of payload-carrying Spaces each share-holder's payload region is divided into.</summary>
    public int RetainingSpaceCount { get; }

    /// <summary>The number of admission-tracked Spaces each share-holder's admission region is divided into.</summary>
    public int ClassifyingSpaceCount { get; }

    /// <summary>The number of fragment slots one Space's window holds, payload or admission alike.</summary>
    public int FragmentsPerSpace { get; }

    /// <summary>The number of bytes one payload fragment slot holds.</summary>
    public int FragmentSize { get; }

    /// <summary>
    /// The arena's total size in bytes: the payload region — <see cref="Capacity"/> ×
    /// <see cref="RetainingSpaceCount"/> × <see cref="FragmentsPerSpace"/> × <see cref="FragmentSize"/>
    /// — plus the admission region — <see cref="Capacity"/> × <see cref="ClassifyingSpaceCount"/> ×
    /// <see cref="FragmentsPerSpace"/> — exactly matching
    /// <see cref="DatagramSessionOptions.ReceiveBudgetBytes"/> for the options this arena was built
    /// from.
    /// </summary>
    public int TotalSize => _buffer.Length;

    /// <summary>
    /// The fixed-size payload slot for <paramref name="fragmentIndex"/> within
    /// <paramref name="shareHolderId"/>'s <paramref name="spaceIndex"/>th retaining Space window.
    /// Nothing here clears a slot's previous contents on reuse; a caller that reuses a share-holder's
    /// region for a new connection is responsible for not trusting stale bytes it never wrote, exactly
    /// as <see cref="RetentionArena.ResetShareHolder"/> resets counters rather than the backing bytes.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="RetainingSpaceCount"/>.</param>
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

        if (spaceIndex < 0 || spaceIndex >= RetainingSpaceCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(spaceIndex),
                spaceIndex,
                $"Space {spaceIndex} is outside this arena's retaining Space count of {RetainingSpaceCount}.");
        }

        if (fragmentIndex < 0 || fragmentIndex >= FragmentsPerSpace)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentIndex),
                fragmentIndex,
                $"fragment slot {fragmentIndex} is outside this arena's window of {FragmentsPerSpace} slots per Space.");
        }

        int regionStart = ((shareHolderId * RetainingSpaceCount) + spaceIndex) * FragmentsPerSpace * FragmentSize;
        int offset = regionStart + (fragmentIndex * FragmentSize);
        return _buffer.AsSpan(offset, FragmentSize);
    }

    /// <summary>
    /// The admission run for <paramref name="shareHolderId"/>'s <paramref name="spaceIndex"/>th
    /// classifying Space: <see cref="FragmentsPerSpace"/> bytes, one per slot, recording whether that
    /// slot has been admitted. <see cref="Memory{T}"/> rather than <see cref="Span{T}"/> because a
    /// <see cref="Session.ReceiveWindow"/> holds the run it is given for the share-holder's whole
    /// lifetime, not for one call — a <see cref="Span{T}"/> cannot be stored as a field.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="ClassifyingSpaceCount"/>.</param>
    public Memory<byte> GetAdmissionRun(int shareHolderId, int spaceIndex)
    {
        if (shareHolderId < 0 || shareHolderId >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shareHolderId),
                shareHolderId,
                $"share-holder {shareHolderId} is outside the arena's capacity of {Capacity}.");
        }

        if (spaceIndex < 0 || spaceIndex >= ClassifyingSpaceCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(spaceIndex),
                spaceIndex,
                $"Space {spaceIndex} is outside this arena's classifying Space count of {ClassifyingSpaceCount}.");
        }

        int regionStart = _admissionOrigin + (((shareHolderId * ClassifyingSpaceCount) + spaceIndex) * FragmentsPerSpace);
        return _buffer.AsMemory(regionStart, FragmentsPerSpace);
    }
}
