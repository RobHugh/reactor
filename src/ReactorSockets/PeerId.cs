namespace ReactorSockets;

/// <summary>
/// A Peer's identity inside this process: the Reactor that issued it, a zero-based Slot index, and a
/// generation counter that increments when the Slot is reused, so a stale <see cref="PeerId"/> held
/// past a Peer's close compares unequal to whatever now occupies that Slot (ADR 0007) rather than
/// aliasing it. Shared by both Session profiles (ADR 0010), exactly as <see cref="LinkId"/> is the
/// Raw profile's.
/// </summary>
/// <remarks>
/// This is deliberately not the identity a datagram Session puts on the wire. A Slot index is dense,
/// small and guessable, and a remote that can guess one can address a Peer it was never introduced
/// to; the wire identity is instead a receiver-assigned random 32-bit value the two ends exchange
/// during the handshake, and it never appears on this type. The generation is
/// <see langword="ulong"/> rather than <see langword="int"/> so that it cannot wrap within the life
/// of any process, which is what lets generation 0 stay a reserved sentinel without a check guarding
/// it (ADR 0015).
/// <para>
/// The Origin is what makes a <see cref="PeerId"/> mean something to exactly one Reactor. Without
/// it, index and generation are only unique within a single Reactor's table, and every table's first
/// Peer is Slot 0 generation 1 — so one Reactor's <see cref="PeerId"/> passed to another's
/// <c>Close</c> named a live Peer of that second Reactor and closed it. A consumer running two
/// Reactors gets the mistake back as nothing happening, which is what an id addressing no Peer has
/// always meant here.
/// </para>
/// </remarks>
/// <param name="Origin">
/// Which Reactor issued this <see cref="PeerId"/>. Opaque and process-local — it is compared for
/// equality and nothing else, orders nothing, and never appears on the wire. Not a
/// <see cref="Source"/>, which names a remote address; this names a Reactor of this process.
/// </param>
/// <param name="Index">The zero-based Slot this Peer occupies.</param>
/// <param name="Generation">
/// The generation that Slot was on when this Peer was admitted. Live Slots begin at 1, so a
/// <see langword="default"/> <see cref="PeerId"/> — generation 0 — addresses nothing.
/// </param>
public readonly record struct PeerId(ulong Origin, int Index, ulong Generation);
