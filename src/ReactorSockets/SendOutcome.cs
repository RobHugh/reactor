namespace ReactorSockets;

/// <summary>
/// The outcome of one <see cref="StreamSessionReactor.Send(PeerId, ReadOnlySpan{byte})"/> call. A bare
/// enum rather than a result struct (unlike <see cref="SendResult"/>): a Session-level send has no
/// partial-completion quantity worth reporting back — the framed Message either fit in the Peer's
/// retention region whole, or it did not, so the outcome is the whole story.
/// </summary>
/// <remarks>
/// Deliberately its own type rather than folded into <see cref="DisconnectReason"/>: exhausting
/// retention does not end the Session, so it belongs on the return of the call that can produce it
/// rather than mixed into the roster of reasons a Peer is gone.
/// </remarks>
public enum SendOutcome
{
    /// <summary>The framed Message was copied whole into the Peer's retention region.</summary>
    Sent,

    /// <summary>
    /// The Peer's retention region had no room for the framed Message. Nothing was copied and nothing
    /// about the Session changed — the caller may retry once bytes have been released, or otherwise.
    /// </summary>
    RetentionExhausted,

    /// <summary>
    /// This end already sent its own Goodbye for this Peer (<c>Close</c>, issue #23). Nothing was
    /// copied: a Message queued after the Goodbye that announces no more are coming would either have
    /// to jump the Goodbye in retention or follow it, and neither is a Message the far end has any
    /// business receiving.
    /// </summary>
    Closed,
}
