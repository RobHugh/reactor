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
/// <see cref="TryPeekNext"/> afterwards. The steady in-order path therefore copies nothing and
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
/// <see cref="OutstandingReliableSequences"/>'s ring uses — up to the limit below, and only when a held
/// sequence's offset does not already fit, which happens only while a gap is open. A slot's payload
/// buffer is kept and reused across releases rather than freed, so a path that repeatedly opens and
/// fills gaps of a similar shape settles into allocating nothing at all.
/// </para>
/// <para>
/// <b>Bound (issue #424).</b> The receive window does not bound this ring. A Fragment group that never
/// completes holds <see cref="NextSequence"/> still while the window's base moves past it, so every
/// later record would otherwise be held here, without limit, and acknowledged. <see cref="CanTake"/>
/// therefore limits how far ahead a record is held and how wide it may be, and
/// <see cref="DatagramSessionOptions.HeldRecordBudgetBytes"/> states what those limits cost.
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
    private byte[]?[] _held;

    /// <summary>
    /// How many bytes of the parallel <see cref="_held"/> slot are a held payload, or <c>-1</c> when
    /// that slot holds nothing. Zero is a real length — a zero-byte Message is a Message — so
    /// occupancy cannot be read off the length alone.
    /// </summary>
    private int[] _heldLengths;

    /// <summary>
    /// The number of slots <see cref="_held"/> and <see cref="_heldLengths"/> currently span. Never more
    /// than <see cref="_maxHeldSlots"/>, because a slot keeps its buffer after release and the head walks
    /// every slot, so the slot count is what limits the buffers kept (issue #424).
    /// </summary>
    private int _capacity;

    /// <summary>The ring position <see cref="NextSequence"/> currently occupies.</summary>
    private int _headSlot;

    /// <summary>How many sequences from <see cref="NextSequence"/> onward this queue will hold (issue #424).</summary>
    private readonly int _maxHeldSlots;

    /// <summary>The widest payload this queue will hold, in bytes (issue #424).</summary>
    private readonly int _maxHeldPayloadLength;

    /// <summary>
    /// Builds a queue expecting <paramref name="initialSequence"/> next — zero by default, where every
    /// Space's numbering begins (ADR 0009). Allocates its initial ring once, up front; nothing after
    /// this grows it unless a gap opens wide enough to need more room.
    /// </summary>
    /// <param name="maxHeldSlots">How many sequences from <see cref="NextSequence"/> onward this queue will hold.</param>
    /// <param name="maxHeldPayloadLength">The widest payload this queue will hold, in bytes.</param>
    /// <param name="initialSequence">The first sequence this queue will release. Defaults to zero.</param>
    public OrderedDeliveryQueue(int maxHeldSlots, int maxHeldPayloadLength, uint initialSequence = 0)
    {
        _maxHeldSlots = maxHeldSlots;
        _maxHeldPayloadLength = maxHeldPayloadLength;
        _capacity = Math.Min(InitialCapacity, maxHeldSlots);
        _held = new byte[_capacity][];
        _heldLengths = CreateEmptyLengths(_capacity);
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
    /// Whether <see cref="Admit"/> could take <paramref name="sequence"/> right now (issue #424). The
    /// next sequence is always takeable at any width, because it is delivered rather than held, which is
    /// also why a refusal can never stop a head from moving. An older one is a duplicate that holds
    /// nothing. Changes nothing. A caller asks before its receive window admits the record, so a refused
    /// record stays unacknowledged for the far end to retransmit.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <param name="payloadLength">The arriving record's payload length in bytes.</param>
    public bool CanTake(uint sequence, int payloadLength) =>
        !SequenceNumber.IsNewerThan(sequence, NextSequence)
        || (unchecked(sequence - NextSequence) < (uint)_maxHeldSlots && payloadLength <= _maxHeldPayloadLength);

    /// <summary>
    /// Admits one already-<see cref="ReceiveWindowOutcome.Admitted"/> sequence and its payload. The
    /// caller has already had <see cref="CanTake"/> answer <see langword="true"/> for it.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <param name="payload">The arriving record's payload, on loan from the Datagram it arrived in.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="sequence"/> is the one the queue was waiting for and
    /// the caller should deliver <paramref name="payload"/> now, then drain the run behind it with
    /// <see cref="TryPeekNext"/> and <see cref="ReleaseNext"/>. <see langword="false"/> when the payload was held behind a gap,
    /// dropped as a duplicate of a sequence already yielded or already held, or dropped because
    /// <see cref="CanTake"/> would have refused it; in each case the caller delivers nothing for this call.
    /// </returns>
    public bool Admit(uint sequence, ReadOnlySpan<byte> payload)
    {
        if (sequence == NextSequence)
        {
            // The sequence the queue was waiting for: the caller delivers these bytes straight from the
            // Datagram they arrived in, and TryPeekNext and ReleaseNext walk whatever was held behind them.
            Advance();
            return true;
        }

        if (!SequenceNumber.IsNewerThan(sequence, NextSequence))
        {
            // Older than what the queue is waiting for: already released by an earlier call, so this is
            // a duplicate of something already yielded and is never yielded twice.
            return false;
        }

        // The caller asked CanTake first, so this never drops a record it admitted. It keeps the ring's
        // bound true here however this method is reached.
        if (!CanTake(sequence, payload.Length))
        {
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
    /// Shows the payload held at <see cref="NextSequence"/>, if any, without releasing it. A caller
    /// that delivers it then calls <see cref="ReleaseNext"/>; one that cannot deliver it yet leaves it
    /// held, still first in line, for a later attempt. Called in that pairing until it answers
    /// <see langword="false"/>, it drains the whole run a filled gap unblocked, oldest first.
    /// </summary>
    /// <param name="payload">
    /// The held payload, on loan from this queue's own storage and valid only until the next
    /// <see cref="Admit"/> or <see cref="ReleaseNext"/> call, which may reuse the buffer behind it.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a payload is held at <see cref="NextSequence"/>,
    /// <see langword="false"/> when that sequence is still missing and nothing can be released yet.
    /// </returns>
    public bool TryPeekNext(out ReadOnlyMemory<byte> payload)
    {
        int length = _heldLengths[_headSlot];
        if (length < 0)
        {
            payload = default;
            return false;
        }

        payload = _held[_headSlot].AsMemory(0, length);
        return true;
    }

    /// <summary>
    /// Releases the payload <see cref="TryPeekNext"/> just showed and steps past its sequence. Call it
    /// only after <see cref="TryPeekNext"/> answered <see langword="true"/>.
    /// </summary>
    public void ReleaseNext()
    {
        // The buffer stays in its slot to be refilled by a later hold; only the length is cleared, which
        // is what marks the slot empty.
        _heldLengths[_headSlot] = -1;
        Advance();
    }

    /// <summary>
    /// Advances <see cref="NextSequence"/> straight to <paramref name="newNextSequence"/> without ever
    /// calling <see cref="Admit"/> or <see cref="ReleaseNext"/> for the sequences in between (issue
    /// #259): what a caller uses when a whole contiguous run of sequences — a Fragment group's own
    /// span — is accounted for entirely outside this queue, because the receive window holds and
    /// reassembles a Fragment's bytes itself rather than handing them to this queue one at a time. A
    /// group's sequences are never individually admitted here, so nothing this call steps over can be
    /// mistaken for a held payload later; this is what keeps <c>ReliableOrdered</c>'s send-order rule
    /// applying to a reassembled Message exactly as it does to an unfragmented one, without this queue
    /// ever holding a Fragment's payload twice over.
    /// </summary>
    /// <param name="newNextSequence">
    /// The sequence this queue should expect next. Must not be older than <see cref="NextSequence"/>.
    /// </param>
    public void SkipTo(uint newNextSequence)
    {
        uint distance = unchecked(newNextSequence - NextSequence);
        for (uint i = 0; i < distance; i++)
        {
            Advance();
        }
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
    /// Grows the ring, doubling its capacity up to <see cref="_maxHeldSlots"/> until
    /// <paramref name="offset"/> fits, when a newly held sequence would not otherwise have a slot. The
    /// caller has checked that <paramref name="offset"/> is below that limit. Growth unwraps the ring into a fresh zero-based array
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
            newCapacity = (int)Math.Min(2L * newCapacity, _maxHeldSlots);
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
