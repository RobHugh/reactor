namespace ReactorSockets.Session;

/// <summary>
/// One Peer's tracker of reliable sequences that have been sent and not yet acknowledged (subtask
/// 6.3.3, issue #185; ADR 0009), and the horizon rule that decides which of them are being judged for
/// retransmission at all. Only the two reliable Spaces are tracked —
/// <see cref="PacketRecordMode.ReliableUnordered"/> and <see cref="PacketRecordMode.ReliableOrdered"/>
/// — since the unreliable Spaces are never acknowledged and so are never retransmitted.
/// </summary>
/// <remarks>
/// <para>
/// <b>The horizon rule.</b> A received ack block describes a cumulative base plus a 32-bit bitfield of
/// the sequences above it (subtask 6.1) — it can only ever speak for the span
/// <c>[base, base + 32]</c>. A sequence sent inside that span is judged: <see cref="RecordSent"/> arms
/// its retransmit deadline immediately, taken from the injected <see cref="TimeProvider"/> plus the
/// configured retransmit timeout. A sequence sent beyond that span cannot yet be described by
/// any ack the peer could send, so arming its deadline now would retransmit it the moment the timeout
/// elapsed even though nothing was lost — it is recorded as outstanding but left unjudged, and
/// <see cref="HorizonDeferrals"/> counts the deferral. Only once a later ack advances the base far
/// enough that the horizon slides over it does its deadline arm, from that moment (subtask 6.3.5,
/// issue #187, via an <c>ApplyAck(PacketRecordMode, uint, uint)</c> method 6.3.5 adds to this class).
/// </para>
/// <para>
/// <b><c>ApplyAck</c> is deliberately not declared here.</b> Its shape is pinned by issue #185, but its
/// body depends on how an ack block's bitfield correlates to the sequences this tracker already holds
/// — 6.3.5's decision, not one to guess at from this side. A stub body would only be a placeholder
/// pretending to work, so 6.3.5 adds the member itself when it lands.
/// </para>
/// <para>
/// Without this rule, a Message spanning more than 33 Fragments would have its later Fragments arrive
/// and sit undescribable by either ack block while an early Fragment is still missing, and every one of
/// those later Fragments would be retransmitted once its timeout elapsed — although nothing was lost.
/// This is unconditional machinery on the normal reliable-send path, not an edge case reached only under
/// loss.
/// </para>
/// <para>
/// Deadlines are always computed from the injected <see cref="TimeProvider"/>, never the ambient clock,
/// so a test can advance time deterministically and so a real Reactor's single time source stays the
/// only clock a Peer ever reads. Not thread-safe, matching every other piece of a Peer's state (ADR
/// 0009): a Peer is driven from one Reactor's single synchronous <c>Pump</c>.
/// </para>
/// <para>
/// <b>Allocation.</b> Each reliable Space's outstanding sequences are held in a growable ring indexed by
/// offset from the oldest still-outstanding sequence, mirroring <see cref="ReceiveWindow"/>'s
/// offset-from-base indexing on the receive side. The ring only grows — doubling its backing arrays —
/// when more sequences are outstanding at once than it has ever held before; <see cref="RecordSent"/>,
/// <see cref="IsOutstanding"/> and <see cref="TryGetDeadline"/> allocate nothing once a Space's ring has
/// grown to cover its steady-state outstanding count.
/// </para>
/// </remarks>
internal sealed class OutstandingReliableSequences
{
    private readonly TimeProvider _time;
    private readonly TimeSpan _retransmitTimeout;
    private readonly ReliableSpaceTracker _reliableUnordered = new();
    private readonly ReliableSpaceTracker _reliableOrdered = new();

    /// <summary>
    /// Builds a tracker with both reliable Spaces empty and both horizons at their initial position
    /// — sequence zero, matching every Space's numbering start (ADR 0009) — so before any ack is ever
    /// received, sequences <c>0</c> through <c>32</c> arm and <c>33</c> upward defer.
    /// </summary>
    /// <param name="time">The time source every armed deadline is computed from.</param>
    /// <param name="retransmitTimeout">
    /// How long after being sent an outstanding sequence's deadline falls, once armed.
    /// </param>
    public OutstandingReliableSequences(TimeProvider time, TimeSpan retransmitTimeout)
    {
        ArgumentNullException.ThrowIfNull(time);
        _time = time;
        _retransmitTimeout = retransmitTimeout;
    }

    /// <summary>
    /// The monotonic count of sequences whose retransmit deadline has been withheld by the horizon rule
    /// so far, summed across both reliable Spaces — a sequence recorded by <see cref="RecordSent"/>
    /// while beyond its Space's horizon, once each. Never decremented: a sequence later brought inside
    /// the horizon by a sliding ack arms from that point on, it does not retroactively un-defer.
    /// </summary>
    public ulong HorizonDeferrals { get; private set; }

    /// <summary>
    /// Records that <paramref name="sequence"/> was just sent on <paramref name="mode"/>'s reliable
    /// Space and is now outstanding. Arms its retransmit deadline — <paramref name="sequence"/>'s send
    /// time is not tracked, only the deadline computed at this call — when the Space's current horizon
    /// covers it; otherwise leaves it unjudged and counts the deferral in
    /// <see cref="HorizonDeferrals"/>. Expects the sequences recorded for one Space to arrive in the
    /// order they were allocated (<see cref="OutboundSequenceSpaces.Allocate"/> is the only source),
    /// the same precondition <see cref="ReceiveWindow"/> relies on for its own offset arithmetic.
    /// </summary>
    /// <param name="mode">Which reliable Space <paramref name="sequence"/> was sent on.</param>
    /// <param name="sequence">The <c>u32</c> sequence number that was just sent.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public void RecordSent(PacketRecordMode mode, uint sequence)
    {
        if (SpaceOf(mode).RecordSent(sequence, _time, _retransmitTimeout))
        {
            HorizonDeferrals++;
        }
    }

    /// <summary>
    /// Whether <paramref name="sequence"/> was sent on <paramref name="mode"/>'s reliable Space and has
    /// not yet been released by an acknowledging ack block — regardless of whether its deadline is
    /// currently armed. A deferred-but-outstanding sequence and an armed one both report
    /// <see langword="true"/> here; <see cref="TryGetDeadline"/> is what distinguishes them.
    /// </summary>
    /// <param name="mode">Which reliable Space to query.</param>
    /// <param name="sequence">The <c>u32</c> sequence number to query.</param>
    /// <returns><see langword="true"/> if <paramref name="sequence"/> is still outstanding.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public bool IsOutstanding(PacketRecordMode mode, uint sequence) => SpaceOf(mode).IsOutstanding(sequence);

    /// <summary>
    /// Attempts to read <paramref name="sequence"/>'s current retransmit deadline on
    /// <paramref name="mode"/>'s reliable Space. Fails when <paramref name="sequence"/> is not
    /// outstanding at all, and also fails while it is outstanding but deferred by the horizon rule — the
    /// mechanism the horizon tests assert on, since a deferred sequence has no deadline to report until
    /// a later ack's horizon slide arms one.
    /// </summary>
    /// <param name="mode">Which reliable Space to query.</param>
    /// <param name="sequence">The <c>u32</c> sequence number to query.</param>
    /// <param name="deadline">
    /// The armed retransmit deadline, valid only when this method returns <see langword="true"/>.
    /// </param>
    /// <returns><see langword="true"/> if <paramref name="sequence"/> is outstanding and armed.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public bool TryGetDeadline(PacketRecordMode mode, uint sequence, out DateTimeOffset deadline) =>
        SpaceOf(mode).TryGetDeadline(sequence, out deadline);

    /// <summary>
    /// Resolves <paramref name="mode"/> to its reliable Space's tracker. Only the two reliable modes are
    /// valid — the two unreliable modes are never acknowledged and so are never tracked here — matching
    /// how <see cref="OutboundSequenceSpaces.Allocate"/> rejects a mode it does not recognize.
    /// </summary>
    private ReliableSpaceTracker SpaceOf(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableUnordered => _reliableUnordered,
        PacketRecordMode.ReliableOrdered => _reliableOrdered,
        _ => throw new ArgumentOutOfRangeException(
            nameof(mode), mode, "Only the two reliable delivery modes are tracked for retransmission."),
    };

    /// <summary>
    /// One reliable Space's outstanding sequences and horizon (see the enclosing type's remarks). Holds
    /// a contiguous run of sequences — from the oldest not yet released up to the newest sent — indexed
    /// by their offset from the oldest, in a ring that grows by doubling rather than a fixed capacity,
    /// since nothing bounds how many sequences a Space can have outstanding at once.
    /// </summary>
    private sealed class ReliableSpaceTracker
    {
        /// <summary>
        /// The horizon's width: an ack block describes its base plus this many sequences above it
        /// (subtask 6.1) — the span <see cref="RecordSent"/> tests a sequence against.
        /// </summary>
        private const uint HorizonWidth = 32;

        private const int InitialCapacity = 16;

        private bool[] _armed = new bool[InitialCapacity];
        private DateTimeOffset[] _deadline = new DateTimeOffset[InitialCapacity];
        private int _capacity = InitialCapacity;

        /// <summary>The ring position the oldest tracked sequence currently occupies.</summary>
        private int _headSlot;

        /// <summary>The oldest sequence still tracked. Meaningful only while <see cref="_count"/> is nonzero.</summary>
        private uint _oldest;

        /// <summary>How many sequences, starting at <see cref="_oldest"/>, are currently tracked.</summary>
        private int _count;

        /// <summary>
        /// The base of the last ack block received for this Space — the horizon's lower edge. Zero
        /// until the first ack arrives, matching every Space's numbering start. Assigned only by the
        /// ack-applying member 6.3.5 (issue #187) adds to the enclosing type; explicitly initialized
        /// here so it reads as the deliberate initial horizon rather than an unused field.
        /// </summary>
        private readonly uint _ackBase = 0;

        /// <summary>
        /// Records <paramref name="sequence"/> as newly outstanding, arming its deadline when the
        /// current horizon covers it. Returns whether it was deferred instead, so the caller can fold
        /// that into <see cref="HorizonDeferrals"/> without this tracker needing to know about the
        /// counter it feeds.
        /// </summary>
        public bool RecordSent(uint sequence, TimeProvider time, TimeSpan retransmitTimeout)
        {
            if (_count == 0)
            {
                _oldest = sequence;
                _headSlot = 0;
            }

            uint offset = unchecked(sequence - _oldest);
            EnsureCapacity(offset);
            int slot = SlotFor(offset);

            bool withinHorizon = unchecked(sequence - _ackBase) <= HorizonWidth;
            if (withinHorizon)
            {
                _armed[slot] = true;
                _deadline[slot] = time.GetUtcNow() + retransmitTimeout;
            }
            else
            {
                _armed[slot] = false;
            }

            _count = (int)offset + 1;
            return !withinHorizon;
        }

        /// <summary>Whether <paramref name="sequence"/> is currently tracked as outstanding.</summary>
        public bool IsOutstanding(uint sequence) => TryGetSlot(sequence, out _);

        /// <summary>
        /// Attempts to read <paramref name="sequence"/>'s deadline: fails both when it is not tracked at
        /// all and when it is tracked but not currently armed.
        /// </summary>
        public bool TryGetDeadline(uint sequence, out DateTimeOffset deadline)
        {
            if (TryGetSlot(sequence, out int slot) && _armed[slot])
            {
                deadline = _deadline[slot];
                return true;
            }

            deadline = default;
            return false;
        }

        /// <summary>
        /// Resolves <paramref name="sequence"/> to its ring slot when it lies within
        /// <c>[_oldest, _oldest + _count)</c> — tracked and not yet released. A sequence below
        /// <see cref="_oldest"/> (already released) or at or beyond <c>_oldest + _count</c> (never sent)
        /// reports <see langword="false"/>, mirroring <see cref="ReceiveWindow.Classify"/>'s own bounds
        /// check.
        /// </summary>
        private bool TryGetSlot(uint sequence, out int slot)
        {
            slot = default;

            if (_count == 0)
            {
                return false;
            }

            bool atOrAboveOldest = sequence == _oldest || SequenceNumber.IsNewerThan(sequence, _oldest);
            if (!atOrAboveOldest)
            {
                return false;
            }

            uint offset = unchecked(sequence - _oldest);
            if (offset >= (uint)_count)
            {
                return false;
            }

            slot = SlotFor(offset);
            return true;
        }

        private int SlotFor(uint offset) => (int)((_headSlot + offset) % (uint)_capacity);

        /// <summary>
        /// Grows the ring, doubling its capacity until <paramref name="offset"/> fits, when the newly
        /// recorded sequence would not otherwise have a slot. Growth unwraps the ring into a fresh
        /// zero-based array — <see cref="_headSlot"/> resets to zero — since a resize already touches
        /// every tracked slot, unwrapping costs nothing further.
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

            bool[] newArmed = new bool[newCapacity];
            DateTimeOffset[] newDeadline = new DateTimeOffset[newCapacity];
            for (int i = 0; i < _count; i++)
            {
                int oldSlot = (int)((_headSlot + i) % (uint)_capacity);
                newArmed[i] = _armed[oldSlot];
                newDeadline[i] = _deadline[oldSlot];
            }

            _armed = newArmed;
            _deadline = newDeadline;
            _capacity = newCapacity;
            _headSlot = 0;
        }
    }
}
