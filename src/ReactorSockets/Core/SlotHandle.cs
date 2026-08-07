namespace ReactorSockets.Core;

/// <summary>
/// L0's identity for one occupant of a <see cref="ShareHolderTable{T}"/>: the table that issued it,
/// the Slot it sits in, and the generation that Slot was on when the occupant was admitted.
/// Stale-handle safety is the whole point — a handle held past its occupant's removal compares
/// unequal to whatever occupies that Slot next, rather than aliasing it.
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
/// <para>
/// <b>Why the Origin.</b> Index and generation alone are only unique <i>within</i> one table, and
/// every table starts life identically: the first occupant of any of them is Slot 0, generation 1.
/// A handle from one Reactor therefore resolved cleanly against another's table and addressed a
/// Share-holder it had never named — the consumer's mistake, but silent, and it closed a live Peer.
/// Stamping each table with a process-unique number makes the two tables' handles disjoint, so the
/// mistake resolves to nothing and is answered by whatever the caller's <c>Close</c> does with an id
/// that addresses nothing.
/// </para>
/// </remarks>
/// <param name="Origin">
/// The <see cref="ShareHolderTable{T}.Origin"/> of the table that issued this handle. Opaque and
/// process-local: it orders nothing and means nothing outside this process, and it exists only to
/// be compared for equality.
/// </param>
/// <param name="Index">The zero-based Slot the occupant sits in.</param>
/// <param name="Generation">The generation that Slot was on when the occupant was admitted.</param>
internal readonly record struct SlotHandle(ulong Origin, int Index, ulong Generation);
