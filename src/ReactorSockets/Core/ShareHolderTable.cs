namespace ReactorSockets.Core;

/// <summary>
/// The fixed-size table of Share-holders a Reactor admits: Capacity Slots, each either empty or
/// holding one occupant, addressed by a generational <see cref="SlotHandle"/> that goes stale when
/// its Slot is reused. This is the identity half of a Reactor and nothing else — registering a Handle
/// with the dispatcher, disposing an occupant and queuing an Event all stay with the profile that
/// owns those concerns.
/// </summary>
/// <remarks>
/// <para>
/// Capacity counts Share-holders — Links in Raw, Peers in a Session (ADR 0014) — which is why this
/// module is generic over its occupant and named for the thing capacity counts rather than for
/// either profile's identity. All three Reactors need this table; only the occupant type differs.
/// </para>
/// <para>
/// <b>Mechanism, not policy.</b> <see cref="TryAdd"/> answers <see langword="null"/> when the table
/// is full and never throws. Raw turns that into a throw on <c>Bind</c> and into a deferral on
/// accept — two policies over one mechanism, and neither belongs at L0, where a message naming "the
/// Reactor" would be a layer talking about something above it.
/// </para>
/// <para>
/// <b>Generations begin at 1.</b> A Slot's generation increments on every removal, so a handle
/// outliving its occupant compares unequal to the Slot's current generation. Starting at 1 rather
/// than 0 is what makes <see langword="default"/> unresolvable: a defaulted handle carries generation
/// 0, which no live Slot ever holds. Nothing has to check for it — and the counter is
/// <see langword="ulong"/> precisely so that nothing ever has to: 2^64 removals on one Slot is
/// unreachable by any process, so the generation cannot count back round through 0 and turn the
/// sentinel into an alias. An <see langword="int"/> would have wrapped in days under connection
/// churn. ADR 0015 records why the width was widened rather than the wrap guarded against.
/// </para>
/// <para>
/// <b>Not thread-safe, deliberately.</b> A Reactor is owned by one thread (ADR 0002) and enforces
/// that on every mutating member before delegating here, so a second check would only duplicate the
/// first. That single-threading is also what makes <see cref="HasFreeSlot"/> followed by
/// <see cref="TryAdd"/> sound: nothing can take the Slot in between.
/// </para>
/// </remarks>
/// <typeparam name="T">The occupant type — a Link's state in Raw, a Peer's in a Session.</typeparam>
internal sealed class ShareHolderTable<T>
    where T : class
{
    private readonly T?[] _slots;
    private readonly ulong[] _generations;

    /// <summary>
    /// Constructs a table of <paramref name="capacity"/> Slots, every generation starting at 1.
    /// </summary>
    /// <param name="capacity">
    /// How many Share-holders this table admits. Validated here as a floor rather than against any
    /// profile's rules, which <see cref="ReactorOptions.Validate"/> already applied before this runs.
    /// </param>
    public ShareHolderTable(int capacity)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"capacity of {capacity} admits no share-holders — capacity must be at least 1.");
        }

        _slots = new T?[capacity];
        _generations = new ulong[capacity];
        Array.Fill(_generations, 1UL);
    }

    /// <summary>The number of Slots this table holds, occupied or not.</summary>
    public int Capacity => _slots.Length;

    /// <summary>The number of Slots currently occupied.</summary>
    public int Count { get; private set; }

    /// <summary>
    /// Whether <see cref="TryAdd"/> would succeed right now. Exists for the accept path, which must
    /// know there is room <i>before</i> it has an occupant to add — a connection this table has no
    /// Slot for is left in the OS backlog untouched rather than accepted and then dropped (ADR 0007).
    /// </summary>
    public bool HasFreeSlot => Count < _slots.Length;

    /// <summary>
    /// Admits <paramref name="value"/> to the first free Slot and returns the handle addressing it,
    /// or <see langword="null"/> if every Slot is occupied.
    /// </summary>
    public SlotHandle? TryAdd(T value)
    {
        ArgumentNullException.ThrowIfNull(value);

        for (int index = 0; index < _slots.Length; index++)
        {
            if (_slots[index] is null)
            {
                _slots[index] = value;
                Count++;
                return new SlotHandle(index, _generations[index]);
            }
        }

        return null;
    }

    /// <summary>
    /// The occupant <paramref name="handle"/> addresses, or <see langword="null"/> if it addresses
    /// nothing: out of range, an empty Slot, or a Slot whose generation has moved on since. A
    /// <see langword="default"/> handle always answers <see langword="null"/>, because its zero
    /// generation matches no live Slot.
    /// </summary>
    public T? TryResolve(SlotHandle handle) =>
        handle.Index >= 0
        && handle.Index < _slots.Length
        && _generations[handle.Index] == handle.Generation
            ? _slots[handle.Index]
            : null;

    /// <summary>
    /// Empties the Slot <paramref name="handle"/> addresses and returns its occupant so the caller
    /// can release whatever it holds, or <see langword="null"/> if the handle addressed nothing. The
    /// Slot's generation advances, so <paramref name="handle"/> and every copy of it are stale from
    /// here on — including against whatever is admitted to this Slot next.
    /// </summary>
    public T? Remove(SlotHandle handle)
    {
        if (TryResolve(handle) is not T occupant)
        {
            return null;
        }

        _slots[handle.Index] = null;
        _generations[handle.Index]++;
        Count--;
        return occupant;
    }

    /// <summary>
    /// Empties every Slot and returns what they held, so a caller tearing the table down can release
    /// each occupant. Every emptied Slot's generation advances exactly as <see cref="Remove"/> would,
    /// leaving the table reusable and every outstanding handle stale.
    /// </summary>
    /// <remarks>
    /// Materialised rather than lazily yielded: a caller that stopped enumerating an iterator halfway
    /// would leave a table that is neither drained nor intact, which is a sharper edge than the one
    /// allocation this costs. It runs once per table teardown.
    /// </remarks>
    public IReadOnlyList<T> DrainAll()
    {
        List<T> drained = new(Count);
        for (int index = 0; index < _slots.Length; index++)
        {
            if (_slots[index] is T occupant)
            {
                drained.Add(occupant);
                _slots[index] = null;
                _generations[index]++;
            }
        }

        Count = 0;
        return drained;
    }
}
