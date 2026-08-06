namespace ReactorSockets.Core;

/// <summary>
/// L0's identity for one occupant of a <see cref="ShareHolderTable{T}"/>: the Slot it sits in, plus
/// the generation that Slot was on when the occupant was admitted. Stale-handle safety is the whole
/// point — a handle held past its occupant's removal compares unequal to whatever occupies that Slot
/// next, rather than aliasing it.
/// </summary>
/// <remarks>
/// This is the internal counterpart to the profiles' public identities, which carry the same two
/// fields under their own names: <see cref="LinkId"/> in Raw, and a <c>PeerId</c> in a Session
/// (ADR 0007). It stays internal, and each profile converts at its own public edge, so L0 never
/// learns whether it is holding Links or Peers (ADR 0003).
/// <para>
/// <see langword="default"/> is never a valid handle. Generations begin at 1, so the zero generation
/// a default value carries cannot match any live Slot — which is what stops <c>default(LinkId)</c>
/// from addressing the first thing admitted to Slot 0.
/// </para>
/// </remarks>
internal readonly record struct SlotHandle(int Index, int Generation);
