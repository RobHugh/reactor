namespace ReactorSockets.Session;

/// <summary>
/// The ordered delivery queue (issue #214, ADR 0009): the piece that makes <c>ReliableOrdered</c>
/// deliver in send order. It accepts one already-<see cref="ReceiveWindowOutcome.Admitted"/> record's
/// sequence and payload at a time and yields payloads only in unbroken sequence order — a sequence
/// that arrives while an earlier one is still missing is held rather than yielded, and the whole
/// contiguous run behind a gap is released together, immediately after the <see cref="Admit"/> call
/// the missing sequence finally arrives on.
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
/// <b>Two calls, not one, and why.</b> A record arriving in order must reach the consumer without its
/// bytes being copied anywhere first, so <see cref="Admit"/> takes a <see cref="ReadOnlySpan{T}"/> —
/// the parse loop's own view straight into the received Datagram — and answers only whether the caller
/// should deliver those same bytes now. A record held behind a gap outlives that Datagram, so it, and
/// only it, is copied into storage this queue owns, and comes back out of
/// <see cref="TryReleaseNext"/> afterwards. The steady in-order path therefore copies nothing and
/// allocates nothing: it writes no slot, and returns before touching any storage at all.
/// </para>
/// <para>
/// <b>Wrap correctness.</b> Sequences are <c>u32</c> and wrap (ADR 0009). Every "is this the next one,
/// or does it come after" decision goes through <see cref="SequenceNumber.IsNewerThan"/>, the same
/// rule <see cref="ReceiveWindow"/> and <see cref="OutstandingReliableSequences"/> use, and every
/// offset from <see cref="NextSequence"/> is computed with unsigned subtraction that wraps exactly the
/// way the sequence space itself does.
/// </para>
/// <para>
/// <b>Storage.</b> A sequence arriving ahead of a gap is held in a ring, indexed by its offset from
/// <see cref="NextSequence"/>, mirroring the offset-from-base indexing <see cref="ReceiveWindow"/> and
/// <see cref="OutstandingReliableSequences"/> both use. The ring grows by doubling — the same growth
/// <see cref="OutstandingReliableSequences"/>'s ring uses — only when a held sequence's offset does not
/// already fit, which happens only while a gap is open. A slot's payload buffer is kept and reused
/// across releases rather than freed, so a path that repeatedly opens and fills gaps of a similar shape
/// settles into allocating nothing at all.
/// </para>
/// <para>
/// <b>Scope.</b> Internal, and reached only through <see cref="DatagramSessionReactor"/>'s
/// <c>ReliableOrdered</c> Space (issue #220), never past the L2 seam (ADR 0015). Each Space that needs
/// ordering gets its own instance, so head-of-line blocking is confined to that one queue by
/// construction rather than by convention.
/// </para>
/// </remarks>
internal sealed class OrderedDeliveryQueue
{
    private const int InitialCapacity = 16;

    /// <summary>
    /// Payload storage for sequences held behind a gap, indexed by ring position. Each slot owns its
    /// buffer, which is grown to fit and then reused rather than released, so a slot that has held a
    /// payload once holds a later one without allocating. Parallel to <see cref="_heldLengths"/>, whose
    /// entry says how much of the buffer is meaningful.
    /// </summary>
    private byte[]?[] _held = new byte[InitialCapacity][];

    /// <summary>
    /// How many bytes of the parallel <see cref="_held"/> slot are a held payload, or <c>-1</c> when
    /// that slot holds nothing. Zero is a real length — a zero-byte Message is a Message — so
    /// occupancy cannot be read off the length alone.
    /// </summary>
    private int[] _heldLengths = CreateEmptyLengths(InitialCapacity);

    /// <summary>The number of slots <see cref="_held"/> and <see cref="_heldLengths"/> currently span.</summary>
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
    /// The next sequence this queue will release: every sequence below this has already been yielded,
    /// and this is the one sequence whose arrival would release the run currently held behind it, if
    /// any. Starts at the value the constructor was given and only ever advances, by exactly one
    /// sequence per payload released.
    /// </summary>
    public uint NextSequence { get; private set; }

    /// <summary>
    /// Admits one already-<see cref="ReceiveWindowOutcome.Admitted"/> sequence and its payload.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <param name="payload">The arriving record's payload, on loan from the Datagram it arrived in.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="sequence"/> is the one the queue was waiting for and
    /// the caller should deliver <paramref name="payload"/> now, then drain the run behind it with
    /// <see cref="TryReleaseNext"/>. <see langword="false"/> when the payload was held behind a gap, or
    /// dropped as a duplicate of a sequence already yielded or already held; either way the caller
    /// delivers nothing for this call.
    /// </returns>
    public bool Admit(uint sequence, ReadOnlySpan<byte> payload)
    {
        if (sequence == NextSequence)
        {
            // The sequence the queue was waiting for: the caller delivers these bytes straight from the
            // Datagram they arrived in, and TryReleaseNext walks whatever was held behind them.
            Advance();
            return true;
        }

        if (!SequenceNumber.IsNewerThan(sequence, NextSequence))
        {
            // Older than what the queue is waiting for: already released by an earlier call, so this is
            // a duplicate of something already yielded and is never yielded twice.
            return false;
        }

        // Ahead of a gap: hold it, copied into storage this queue owns, because the Datagram these
        // bytes point into is gone long before the missing sequence arrives. A sequence already held at
        // this offset is a duplicate of what is already waiting and is dropped rather than overwriting
        // it.
        uint offset = unchecked(sequence - NextSequence);
        EnsureCapacity(offset);
        int slot = SlotFor(offset);
        if (_heldLengths[slot] < 0)
        {
            byte[]? buffer = _held[slot];
            if (buffer is null || buffer.Length < payload.Length)
            {
                buffer = new byte[payload.Length];
                _held[slot] = buffer;
            }

            payload.CopyTo(buffer);
            _heldLengths[slot] = payload.Length;
        }

        return false;
    }

    /// <summary>
    /// Releases the next payload held contiguously behind the sequence the last <see cref="Admit"/>
    /// call filled, in send order. Called in a loop until it answers <see langword="false"/>, it drains
    /// the whole run a filled gap unblocked, oldest first.
    /// </summary>
    /// <param name="payload">
    /// The released payload, on loan from this queue's own storage and valid only until the next
    /// <see cref="Admit"/> or <see cref="TryReleaseNext"/> call, which may reuse the buffer behind it.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a held payload was released, <see langword="false"/> when the next
    /// sequence is still missing and nothing more can be released yet.
    /// </returns>
    public bool TryReleaseNext(out ReadOnlyMemory<byte> payload)
    {
        int length = _heldLengths[_headSlot];
        if (length < 0)
        {
            payload = default;
            return false;
        }

        // The buffer stays in its slot to be refilled by a later hold; only the length is cleared, which
        // is what marks the slot empty.
        payload = _held[_headSlot].AsMemory(0, length);
        _heldLengths[_headSlot] = -1;
        Advance();
        return true;
    }

    /// <summary>Steps past the sequence just released: one sequence on, one ring slot on.</summary>
    private void Advance()
    {
        NextSequence = unchecked(NextSequence + 1);
        _headSlot = _headSlot + 1 == _capacity ? 0 : _headSlot + 1;
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

        byte[]?[] newHeld = new byte[newCapacity][];
        int[] newLengths = CreateEmptyLengths(newCapacity);
        for (int i = 0; i < _capacity; i++)
        {
            int oldSlot = (_headSlot + i) % _capacity;
            newHeld[i] = _held[oldSlot];
            newLengths[i] = _heldLengths[oldSlot];
        }

        _held = newHeld;
        _heldLengths = newLengths;
        _capacity = newCapacity;
        _headSlot = 0;
    }

    /// <summary>Builds a length array with every slot marked empty, since <c>-1</c> is not the default.</summary>
    private static int[] CreateEmptyLengths(int capacity)
    {
        int[] lengths = new int[capacity];
        Array.Fill(lengths, -1);
        return lengths;
    }
}
