namespace ReactorSockets.Core;

/// <summary>
/// The outcome of one <see cref="RetentionArena.ReleaseThroughMessageCount"/> call.
/// </summary>
internal enum MessageReleaseOutcome
{
    /// <summary>
    /// The share-holder's released total now covers everything through the requested Message —
    /// whether this call was the one that advanced it, or it already did, because the same peer
    /// counter rides both emission triggers and repeating one is not an error.
    /// </summary>
    Released,

    /// <summary>
    /// The requested Message count exceeds what the share-holder has ever appended. Refused without
    /// touching the arena — the peer is claiming to have received Messages that were never sent,
    /// which is a protocol failure for the caller to act on, not something this type papers over.
    /// </summary>
    Refused,
}
