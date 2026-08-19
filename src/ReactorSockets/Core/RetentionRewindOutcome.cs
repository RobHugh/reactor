namespace ReactorSockets.Core;

/// <summary>
/// The outcome of one <see cref="RetentionArena.RewindSentToMessageCount"/> call.
/// </summary>
internal enum RetentionRewindOutcome
{
    /// <summary>
    /// The share-holder's sent total now sits at the byte boundary the requested Message count names
    /// — moved back to it if it was ahead, left alone if the count named a Message this end had not
    /// yet sent that far past (never pushed forward). The bytes between the new sent total and the
    /// old one are, by construction, still held in the ring and ready for <see cref="RetentionArena.GetUnsent"/>
    /// to hand to a replacement transport unchanged.
    /// </summary>
    Rewound,

    /// <summary>
    /// The requested Message count names a Message already below the share-holder's released head —
    /// bytes this end no longer retains, because a peer's earlier confirmation already freed them.
    /// Refused without touching the arena: resuming to a point this ring has no bytes for would leave
    /// a hole in the stream rather than a rewind. Issue #130's own gap case.
    /// </summary>
    BelowReleasedHead,

    /// <summary>
    /// The requested Message count exceeds what the share-holder has ever appended. Refused without
    /// touching the arena, on the same footing <see cref="MessageReleaseOutcome.Refused"/> refuses a
    /// count past the appended total: the peer is claiming a position this end never sent to.
    /// </summary>
    AheadOfAppended,
}
