namespace ReactorSockets.Session;

/// <summary>
/// The ordered delivery queue (issue #214, ADR 0009): the piece that makes <c>ReliableOrdered</c>
/// deliver in send order. It accepts one already-<see cref="ReceiveWindowOutcome.Admitted"/> record's
/// sequence and payload at a time and yields payloads only in unbroken sequence order — a sequence
/// that arrives while an earlier one is still missing is held rather than yielded, and the whole
/// contiguous run behind a gap is released together, in one <see cref="Admit"/> call, the moment the
/// missing sequence finally shows up.
/// </summary>
/// <remarks>
/// <para>
/// <b>Division of labour with <see cref="ReceiveWindow"/>.</b> This type does not itself decide
/// whether a sequence is new, a duplicate, or out of window — that is <see cref="ReceiveWindow"/>'s
/// job, one level up. <see cref="Admit"/> expects to be called only with a sequence its caller's
/// <see cref="ReceiveWindow"/> has already classified <see cref="ReceiveWindowOutcome.Admitted"/> for
/// the same Space; a sequence that window already rejected as
/// <see cref="ReceiveWindowOutcome.Duplicate"/> or <see cref="ReceiveWindowOutcome.OutOfWindow"/> is
/// never passed here at all. What this type adds on top is send-order release: a
/// <see cref="ReceiveWindow"/> alone tells a caller a sequence is new, not that every sequence before
/// it has already been handed to the consumer.
/// </para>
/// <para>
/// <b>Wrap correctness.</b> Sequences are <c>u32</c> and wrap (ADR 0009). Every "is this the next one,
/// or does it come after" decision goes through <see cref="SequenceNumber.IsNewerThan"/>, the same
/// rule <see cref="ReceiveWindow"/> and <see cref="OutstandingReliableSequences"/> use, and every
/// offset from <see cref="NextSequence"/> is computed with unsigned subtraction that wraps exactly the
/// way the sequence space itself does.
/// </para>
/// <para>
/// <b>Storage and allocation.</b> A sequence arriving ahead of a gap is held in a ring, indexed by its
/// offset from <see cref="NextSequence"/>, mirroring the offset-from-base indexing
/// <see cref="ReceiveWindow"/> and <see cref="OutstandingReliableSequences"/> both use. The ring grows
/// by doubling — the same growth <see cref="OutstandingReliableSequences"/>'s ring uses — only when a
/// held sequence's offset does not already fit, which happens only while a gap is open; it never grows
/// on the steady in-order path. <see cref="Admit"/> itself never allocates: a released run is written
/// into a scratch buffer sized alongside the ring and reused across calls, exactly like
/// <see cref="OutstandingReliableSequences.CollectExpired"/>'s own reused result buffer, so calling it
/// for a sequence that arrives in order — the steady path — touches no storage beyond writing one slot
/// of that buffer and returning a slice over it.
/// </para>
/// <para>
/// <b>Scope.</b> Internal, and not yet wired into <see cref="DatagramSessionReactor"/> — wiring this
/// queue into the <c>ReliableOrdered</c> Space is issue #220, a separate ticket. Each Space that needs
/// ordering gets its own instance, so head-of-line blocking is confined to that one queue by
/// construction rather than by convention.
/// </para>
/// </remarks>
internal sealed class OrderedDeliveryQueue
{
    private const int InitialCapacity = 16;

    /// <summary>
    /// Payloads held behind a gap, indexed by ring position. Parallel to <see cref="_occupied"/>.
    /// A slot's contents are meaningless while its <see cref="_occupied"/> entry is
    /// <see langword="false"/>.
    /// </summary>
    private ReadOnlyMemory<byte>[] _held = new ReadOnlyMemory<byte>[InitialCapacity];

    /// <summary>Whether a ring slot currently holds a payload awaiting release. Parallel to <see cref="_held"/>.</summary>
    private bool[] _occupied = new bool[InitialCapacity];

    /// <summary>
    /// The scratch buffer <see cref="Admit"/> writes a released run into, reused by every call and
    /// grown alongside <see cref="_held"/> rather than sized independently. Sized to
    /// <c>_capacity + 1</c>: the ring can hold at most <see cref="_capacity"/> already-held payloads,
    /// plus the one just-arrived payload that triggered the release and never itself occupies a ring
    /// slot. Its contents past the count the most recent <see cref="Admit"/> call returned are
    /// meaningless.
    /// </summary>
    private ReadOnlyMemory<byte>[] _releaseBuffer = new ReadOnlyMemory<byte>[InitialCapacity + 1];

    /// <summary>The number of slots <see cref="_held"/> and <see cref="_occupied"/> currently span.</summary>
    private int _capacity = InitialCapacity;

    /// <summary>The ring position <see cref="NextSequence"/> currently occupies.</summary>
    private int _headSlot;

    /// <summary>
    /// Builds a queue expecting <paramref name="initialSequence"/> next — zero by default, where every
    /// Space's numbering begins (ADR 0009). Allocates its initial ring once, up front; nothing after
    /// this grows it unless a gap opens wide enough to need more room.
    /// </summary>
    /// <param name="initialSequence">The first sequence this queue will release. Defaults to zero.</param>
    public OrderedDeliveryQueue(uint initialSequence = 0)
    {
        NextSequence = initialSequence;
    }

    /// <summary>
    /// The next sequence this queue will release: every sequence below this has already been yielded
    /// by a previous <see cref="Admit"/> call, and this is the one sequence whose arrival would
    /// release the run currently held behind it, if any. Starts at the value the constructor was given
    /// and only ever advances, by exactly one sequence per payload <see cref="Admit"/> releases.
    /// </summary>
    public uint NextSequence { get; private set; }

    /// <summary>
    /// Admits one already-<see cref="ReceiveWindowOutcome.Admitted"/> sequence and its payload.
    /// A sequence equal to <see cref="NextSequence"/> is released immediately together with every
    /// sequence already held that is now contiguous above it, in send order, in this one call. A
    /// sequence newer than <see cref="NextSequence"/> is held and yields nothing, unless it duplicates
    /// a sequence already held, in which case it is dropped and yields nothing. A sequence not newer
    /// than <see cref="NextSequence"/> has already been released by an earlier call and is dropped as a
    /// duplicate, yielding nothing.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <param name="payload">The arriving record's payload, on loan from whatever storage retains it.</param>
    /// <returns>
    /// The run of payloads released by this call, oldest first, empty when <paramref name="sequence"/>
    /// was held behind a gap or was a duplicate. Valid only until the next <see cref="Admit"/> call —
    /// it is a slice over a buffer this queue reuses, not a copy.
    /// </returns>
    public ReadOnlySpan<ReadOnlyMemory<byte>> Admit(uint sequence, ReadOnlyMemory<byte> payload)
    {
        bool isNext = sequence == NextSequence;
        if (!isNext && !SequenceNumber.IsNewerThan(sequence, NextSequence))
        {
            // Not newer than, and not equal to, the next expected sequence: already released by an
            // earlier call. A duplicate of what has already been yielded, never yielded twice.
            return [];
        }

        if (!isNext)
        {
            // Ahead of a gap: hold it. A sequence already held at this offset is a duplicate of what
            // is already waiting and is dropped rather than overwriting the held payload.
            uint offset = unchecked(sequence - NextSequence);
            EnsureCapacity(offset);
            int slot = SlotFor(offset);
            if (!_occupied[slot])
            {
                _occupied[slot] = true;
                _held[slot] = payload;
            }

            return [];
        }

        // The sequence the queue was waiting for: release it, then walk forward releasing every
        // already-held sequence contiguous above it, stopping at the first still-open gap. Bounded by
        // _capacity, the same bound the ring's own growth already established.
        int releasedCount = 0;
        _releaseBuffer[releasedCount++] = payload;
        NextSequence = unchecked(NextSequence + 1);

        while (_occupied[_headSlot])
        {
            _releaseBuffer[releasedCount++] = _held[_headSlot];
            _occupied[_headSlot] = false;
            _held[_headSlot] = default;
            _headSlot = _headSlot + 1 == _capacity ? 0 : _headSlot + 1;
            NextSequence = unchecked(NextSequence + 1);
        }

        return _releaseBuffer.AsSpan(0, releasedCount);
    }

    /// <summary>
    /// Resolves <paramref name="offset"/>, an offset from <see cref="NextSequence"/>, to its ring
    /// slot — relative to <see cref="_headSlot"/>, the slot <see cref="NextSequence"/> itself
    /// currently occupies, exactly like <see cref="ReceiveWindow"/>'s own base-relative addressing.
    /// </summary>
    private int SlotFor(uint offset) => (int)(((uint)_headSlot + offset) % (uint)_capacity);

    /// <summary>
    /// Grows the ring, doubling its capacity until <paramref name="offset"/> fits, when a newly held
    /// sequence would not otherwise have a slot. Growth unwraps the ring into a fresh zero-based array
    /// — <see cref="_headSlot"/> resets to zero — mirroring
    /// <see cref="OutstandingReliableSequences"/>'s own ring growth: a resize already touches every
    /// held slot, so unwrapping costs nothing further. Never called on the steady in-order path, since
    /// nothing is ever held there.
    /// </summary>
    private void EnsureCapacity(uint offset)
    {
        if (offset < (uint)_capacity)
        {
            return;
        }

        int newCapacity = _capacity;
        while (offset >= (uint)newCapacity)
        {
            newCapacity *= 2;
        }

        var newHeld = new ReadOnlyMemory<byte>[newCapacity];
        var newOccupied = new bool[newCapacity];
        for (int i = 0; i < _capacity; i++)
        {
            int oldSlot = (_headSlot + i) % _capacity;
            newHeld[i] = _held[oldSlot];
            newOccupied[i] = _occupied[oldSlot];
        }

        _held = newHeld;
        _occupied = newOccupied;
        _releaseBuffer = new ReadOnlyMemory<byte>[newCapacity + 1];
        _capacity = newCapacity;
        _headSlot = 0;
    }
}
