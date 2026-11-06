namespace ReactorSockets.Core;

/// <summary>
/// The receive side of the memory bill, actually allocated: one buffer per Reactor, sized exactly
/// <see cref="DatagramSessionOptions.ReceiveBudgetBytes"/> — <c>Capacity × FragmentsPerSpace ×
/// (RetainingSpaceCount × (FragmentSize + 2) + ClassifyingSpaceCount)</c> — and sliced into three
/// regions. The payload region comes first: fixed-size fragment slots, one per (Peer, retaining Space)
/// pair, for the two Spaces that reassemble and so must hold what they receive. The admission region
/// follows: one byte per slot, one run per (Peer, classifying Space) pair, for all four Spaces, because
/// all four classify whether or not they retain — that per-slot admission record is what
/// <see cref="Session.ReceiveWindow"/> previously allocated for itself, outside this arena. The length
/// region comes last: two bytes per slot, one run per (Peer, retaining Space) pair, for the two Spaces
/// that retain — how many payload bytes the Fragment held in the matching payload slot actually
/// occupies, zero when that slot holds none. This is the storage that lets a Fragment be held in the
/// receive window it already arrived in rather than a separate reassembly buffer (ADR 0004): the
/// window's own admission bytes are cleared the moment the base advances past them, so the length run —
/// which nothing but a completed or reused slot ever clears — is what a later Pump reads to tell a held
/// Fragment from an empty slot. Preallocated once at construction and never grown: the receive window
/// is the library's main memory dial, taken once and never seen to grow.
/// </summary>
/// <remarks>
/// <para>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/> — a Peer, in the
/// datagram Session profile — the same convention <see cref="FrameArena"/> and
/// <see cref="RetentionArena"/> use. A Space within a share-holder's payload region, or its length
/// region, is identified by a zero-based index below <see cref="RetainingSpaceCount"/>, and a Space
/// within a share-holder's admission region by a zero-based index below
/// <see cref="ClassifyingSpaceCount"/>; a fragment slot within any of the three by a zero-based index
/// below <see cref="FragmentsPerSpace"/>.
/// </para>
/// <para>
/// This type owns only the storage and the addressing arithmetic over it. It carries no notion of a
/// sequence number, a fragment's offset within a Message, or which slot a given sequence belongs in —
/// classifying a sequence into a slot is <see cref="Session.ReceiveWindow"/>'s job, and routing a
/// parsed record's payload and length into the slot <see cref="Session.ReceiveWindow.Classify(uint)"/> names is
/// the datagram parse path's. Keeping that knowledge out of this type keeps allocation and lifetime
/// separate from classification and routing.
/// </para>
/// </remarks>
internal sealed class ReceiveArena
{
    /// <summary>
    /// Bytes one fragment-length slot occupies: a <c>u16</c>, wide enough for any
    /// <see cref="DatagramSessionOptions.ResolvedFragmentSize"/> up to the largest a UDP payload can
    /// carry (65 507), which a single byte — the admission run's width — could not hold.
    /// </summary>
    private const int FragmentLengthSize = 2;

    /// <summary>
    /// Bytes one waiting-Fragment-group entry occupies: a <c>u32</c> first sequence and a <c>u16</c>
    /// declared Fragment count, the pair that names a group without a message ID the wire never
    /// carries (<see cref="FragmentGroup"/>).
    /// </summary>
    private const int PendingGroupEntrySize = 6;

    /// <summary>
    /// Bytes one held-Fragment's group-identity entry occupies: a <c>u32</c> first sequence and a
    /// <c>u16</c> declared Fragment count, the same shape as <see cref="PendingGroupEntrySize"/> but one
    /// entry per <see cref="GetFragmentSlot"/> slot rather than per waiting group — what names the set
    /// a held-but-not-yet-complete Fragment belongs to, so that set's held and declared counts can
    /// still be read at the moment its slot is discarded and the length region's own residue is lost.
    /// </summary>
    private const int FragmentGroupEntrySize = 6;

    private readonly byte[] _buffer;
    private readonly int _admissionOrigin;
    private readonly int _lengthOrigin;
    private readonly int _fragmentGroupOrigin;
    private readonly int _pendingGroupOrigin;

    /// <summary>
    /// Constructs an arena of <paramref name="capacity"/> share-holders, each holding
    /// <paramref name="retainingSpaceCount"/> payload-carrying Spaces and
    /// <paramref name="classifyingSpaceCount"/> admission-tracked Spaces, every Space a window of
    /// <paramref name="fragmentsPerSpace"/> slots — <paramref name="fragmentSize"/> bytes wide for a
    /// payload slot, one byte for an admission slot, two bytes for a length slot (retaining Spaces
    /// only).
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

        PendingGroupsPerSpace = fragmentsPerSpace / 2 < 1 ? 1 : fragmentsPerSpace / 2;

        long payloadBytes = (long)capacity * retainingSpaceCount * fragmentsPerSpace * fragmentSize;
        long admissionBytes = (long)capacity * classifyingSpaceCount * fragmentsPerSpace;
        long lengthBytes = (long)capacity * retainingSpaceCount * fragmentsPerSpace * FragmentLengthSize;
        long fragmentGroupBytes = (long)capacity * retainingSpaceCount * fragmentsPerSpace * FragmentGroupEntrySize;
        long pendingGroupBytes = (long)capacity * retainingSpaceCount * PendingGroupsPerSpace * PendingGroupEntrySize;
        long totalBytes = payloadBytes + admissionBytes + lengthBytes + fragmentGroupBytes + pendingGroupBytes;
        if (totalBytes > Array.MaxLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                totalBytes,
                $"a receive arena of {capacity} share-holders × {fragmentsPerSpace} fragments × " +
                $"({retainingSpaceCount} retaining Spaces × ({fragmentSize} payload bytes + {FragmentLengthSize} " +
                $"length bytes + {FragmentGroupEntrySize} group-identity bytes) + {classifyingSpaceCount} " +
                $"classifying Spaces × 1 byte) plus {capacity} × {retainingSpaceCount} retaining Spaces × " +
                $"{PendingGroupsPerSpace} waiting groups × {PendingGroupEntrySize} bytes = {totalBytes} bytes " +
                $"cannot be allocated — it exceeds Array.MaxLength ({Array.MaxLength}).");
        }

        _admissionOrigin = (int)payloadBytes;
        _lengthOrigin = (int)(payloadBytes + admissionBytes);
        _fragmentGroupOrigin = (int)(payloadBytes + admissionBytes + lengthBytes);
        _pendingGroupOrigin = (int)(payloadBytes + admissionBytes + lengthBytes + fragmentGroupBytes);
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
    /// The number of completed-but-undelivered Fragment groups one retaining Space can have waiting at
    /// once: half <see cref="FragmentsPerSpace"/>, and never fewer than one. A group is at least two
    /// Fragments wide and its pieces stay held until it is delivered, so a window of <c>n</c> slots can
    /// never have more than <c>n / 2</c> groups waiting — the bound is a consequence of the window's own
    /// size rather than a limit chosen here.
    /// </summary>
    public int PendingGroupsPerSpace { get; }

    /// <summary>
    /// The arena's total size in bytes: the payload region — <see cref="Capacity"/> ×
    /// <see cref="RetainingSpaceCount"/> × <see cref="FragmentsPerSpace"/> × <see cref="FragmentSize"/>
    /// — plus the admission region — <see cref="Capacity"/> × <see cref="ClassifyingSpaceCount"/> ×
    /// <see cref="FragmentsPerSpace"/> — plus the length region — <see cref="Capacity"/> ×
    /// <see cref="RetainingSpaceCount"/> × <see cref="FragmentsPerSpace"/> × 2 — exactly matching
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

    /// <summary>
    /// The fragment-length run for <paramref name="shareHolderId"/>'s <paramref name="spaceIndex"/>th
    /// retaining Space: <see cref="FragmentsPerSpace"/> slots of two bytes each, one per
    /// <see cref="GetFragmentSlot"/> slot, recording how many payload bytes the Fragment currently held
    /// there occupies — zero when that slot holds no Fragment. <see cref="Memory{T}"/> for the same
    /// reason <see cref="GetAdmissionRun"/> returns one: a <see cref="Session.ReceiveWindow"/> holds
    /// this run for the share-holder's whole lifetime, not for one call. This type only hands back the
    /// storage; interpreting the two bytes of one slot is <see cref="Session.ReceiveWindow"/>'s job,
    /// exactly as classifying a sequence into a slot already is.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="RetainingSpaceCount"/>.</param>
    public Memory<byte> GetFragmentLengthRun(int shareHolderId, int spaceIndex)
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

        int regionStart = _lengthOrigin
            + (((shareHolderId * RetainingSpaceCount) + spaceIndex) * FragmentsPerSpace * FragmentLengthSize);
        return _buffer.AsMemory(regionStart, FragmentsPerSpace * FragmentLengthSize);
    }

    /// <summary>
    /// The Fragment-group run for <paramref name="shareHolderId"/>'s <paramref name="spaceIndex"/>th
    /// retaining Space: <see cref="FragmentsPerSpace"/> entries of six bytes each, one per
    /// <see cref="GetFragmentSlot"/> slot, naming the group — first sequence, then declared Fragment
    /// count — that the Fragment currently held there belongs to. Read alongside
    /// <see cref="GetFragmentLengthRun"/> at the moment a slot's residue would otherwise be lost: the
    /// length region says a piece was held, this region says which set it was a piece of and how large
    /// that set claimed to be, which is what lets an abandoned set's held-versus-declared count still be
    /// answered after its slot is reused. <see cref="Memory{T}"/> for the same reason
    /// <see cref="GetFragmentLengthRun"/> returns one: a <see cref="Session.ReceiveWindow"/> holds this
    /// run for the share-holder's whole lifetime, not for one call.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="RetainingSpaceCount"/>.</param>
    public Memory<byte> GetFragmentGroupRun(int shareHolderId, int spaceIndex)
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

        int regionStart = _fragmentGroupOrigin
            + (((shareHolderId * RetainingSpaceCount) + spaceIndex) * FragmentsPerSpace * FragmentGroupEntrySize);
        return _buffer.AsMemory(regionStart, FragmentsPerSpace * FragmentGroupEntrySize);
    }

    /// <summary>
    /// The waiting-Fragment-group run for <paramref name="shareHolderId"/>'s
    /// <paramref name="spaceIndex"/>th retaining Space: <see cref="PendingGroupsPerSpace"/> entries of
    /// six bytes each, holding the groups that have completed but not yet reached the consumer. Taken
    /// from the arena for the same reason every other per-Peer receive run is: it is sized by the
    /// receive window, so leaving it outside would put a window-proportional cost outside the memory
    /// bill the options state.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="spaceIndex">A zero-based index below <see cref="RetainingSpaceCount"/>.</param>
    public Memory<byte> GetPendingGroupRun(int shareHolderId, int spaceIndex)
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

        int regionStart = _pendingGroupOrigin
            + (((shareHolderId * RetainingSpaceCount) + spaceIndex) * PendingGroupsPerSpace * PendingGroupEntrySize);
        return _buffer.AsMemory(regionStart, PendingGroupsPerSpace * PendingGroupEntrySize);
    }
}
