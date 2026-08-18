namespace ReactorSockets;

/// <summary>
/// A Link's identity: the Reactor that issued it, a zero-based slot index, and a generation counter
/// that increments when the slot is reused, so a stale <see cref="LinkId"/> held past a Link's close
/// compares unequal to whatever now occupies that slot (ADR 0014) rather than aliasing it.
/// </summary>
/// <remarks>
/// The generation is <see langword="ulong"/> rather than <see langword="int"/> so that it cannot
/// wrap within the life of any process, which is what lets generation 0 stay a reserved sentinel
/// without a check guarding it. ADR 0015 records the reasoning and the alternatives declined.
/// <para>
/// The Origin extends that same guarantee across Reactors. Index and generation are unique only
/// within one Reactor's table, and every table's first Link is Slot 0 generation 1, so without it a
/// <see cref="LinkId"/> from one <see cref="RawReactor"/> addressed a live Link of another.
/// </para>
/// </remarks>
/// <param name="Origin">
/// Which Reactor issued this <see cref="LinkId"/>. Opaque and process-local — compared for equality
/// and nothing else, ordering nothing and meaning nothing outside this process.
/// </param>
/// <param name="Index">The zero-based Slot this Link occupies.</param>
/// <param name="Generation">
/// The generation that Slot was on when this Link was admitted. Live Slots begin at 1, so a
/// <see langword="default"/> <see cref="LinkId"/> — generation 0 — addresses nothing.
/// </param>
public readonly record struct LinkId(ulong Origin, int Index, ulong Generation);
