namespace ReactorSockets;

/// <summary>
/// A Link's identity: a zero-based slot index plus a generation counter that increments when the
/// slot is reused, so a stale <see cref="LinkId"/> held past a Link's close compares unequal to
/// whatever now occupies that slot (ADR 0014) rather than aliasing it.
/// </summary>
public readonly record struct LinkId(int Index, int Generation);
