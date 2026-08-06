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
/// <para>
/// <b>Why the generation is <see langword="ulong"/>.</b> That sentinel argument only holds while a
/// generation cannot count back round to 0. An <see langword="int"/> generation wraps after 2^31
/// removals on one Slot — days of uptime for a server churning short-lived connections — and passes
/// through 0 on the way, at which point <c>default(LinkId)</c> begins aliasing a live Link. At
/// <see langword="ulong"/> the counter cannot wrap in the life of any process, so the sentinel is
/// safe by unreachability rather than by a check on the increment. A skip-zero branch was declined
/// deliberately: it would run on every removal and no test could ever reach it. ADR 0015 records
/// this, and the testing policy that made it the right shape of fix.
/// </para>
/// </remarks>
/// <param name="Index">The zero-based Slot the occupant sits in.</param>
/// <param name="Generation">The generation that Slot was on when the occupant was admitted.</param>
internal readonly record struct SlotHandle(int Index, ulong Generation);
