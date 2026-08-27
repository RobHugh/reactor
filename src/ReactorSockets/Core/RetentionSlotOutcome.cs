namespace ReactorSockets.Core;

/// <summary>
/// The outcome of a single <see cref="RetentionPool.TryAcquire"/> call.
/// </summary>
internal enum RetentionSlotOutcome
{
    /// <summary>
    /// The payload was copied into a free slot in the Space's run, and that slot now records the
    /// sequence number and byte length written.
    /// </summary>
    Committed,

    /// <summary>
    /// Every slot in the Space's run is already occupied. Refused without touching the pool.
    /// </summary>
    Exhausted,
}
