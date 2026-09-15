namespace ReactorSockets.Core;

/// <summary>
/// The one source of Origins in this process: the value a Reactor stamps into every
/// <see cref="SlotHandle"/> its <see cref="ShareHolderTable{T}"/> issues, so that an identity from
/// one Reactor resolves to nothing in another (ADR 0016).
/// </summary>
/// <remarks>
/// <b>This is the only process-wide mutable state in the library, and a monotonic counter is the
/// only thing that may ever live here.</b> A Reactor owns no threads and shares nothing else across
/// instances (ADR 0002); the sole reason this exists is that an Origin is meaningless unless every
/// Reactor in the process draws from the same sequence. It was previously a static on
/// <see cref="ShareHolderTable{T}"/>, which gave each closed generic type its own counter — three of
/// them, all starting at 1 — so the two Session profiles issued equal
/// <see cref="ReactorSockets.PeerId"/>s for unrelated Peers. A non-generic home makes the single
/// sequence a property of one file rather than an accident of instantiation.
/// </remarks>
internal static class ReactorOrigin
{
    private static uint _last;

    /// <summary>
    /// The next Origin, unique within this process. Never returns 0, which is what keeps a
    /// <see langword="default"/> identity unresolvable by its Origin as well as by its generation.
    /// </summary>
    /// <remarks>
    /// Incremented under <see cref="Interlocked"/> because two Reactors need not be constructed on
    /// the same thread, even though each is owned by one thereafter (ADR 0002).
    /// <para>
    /// <b>The wrap is not guarded.</b> Exhausting 32 bits means constructing 4,294,967,296 Reactors
    /// in one process, and every one of them binds a socket and allocates its arenas up front — the
    /// process fails on those resources long before the counter is interesting. This is ADR 0015's
    /// rule applied again: make the invariant unreachable rather than guard a case that cannot
    /// arise. A throw here would also be the least useful response available, firing in the
    /// constructor of a Reactor whose only fault is being the four-billionth.
    /// </para>
    /// </remarks>
    public static uint Next() => Interlocked.Increment(ref _last);
}
