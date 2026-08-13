namespace ReactorSockets.Core;

/// <summary>
/// The outcome of a single <see cref="RetentionArena.TryAppend"/> call.
/// </summary>
internal enum RetentionOutcome
{
    /// <summary>
    /// The requested bytes were copied into the share-holder's region and its running totals and
    /// high-water mark were advanced.
    /// </summary>
    Committed,

    /// <summary>
    /// The share-holder's region has no room left for the request — the retained bytes plus the
    /// request would exceed <see cref="RetentionArena.BoundPerShareHolder"/>. Refused without
    /// touching the arena.
    /// </summary>
    Exhausted,
}
