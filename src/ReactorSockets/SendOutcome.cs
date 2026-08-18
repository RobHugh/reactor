namespace ReactorSockets;

/// <summary>
/// The outcome of one <see cref="StreamSessionReactor.Send(PeerId, ReadOnlySpan{byte})"/> call. A bare
/// enum rather than a result struct (unlike <see cref="SendResult"/>): a Session-level send has no
/// partial-completion quantity worth reporting back — the framed Message either fit in the Peer's
/// retention region whole, or it did not, so the outcome is the whole story.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately its own type rather than folded into <see cref="DisconnectReason"/>: exhausting
/// retention does not end the Session, so it belongs on the return of the call that can produce it
/// rather than mixed into the roster of reasons a Peer is gone.
/// </para>
/// <para>
/// Explicit values throughout, the same house style as <see cref="DisconnectReason"/>: a member is
/// added at the end with the next free number, never inserted, so a value a consumer has already
/// persisted keeps its meaning across a library upgrade.
/// </para>
/// </remarks>
public enum SendOutcome
{
    /// <summary>The framed Message was copied whole into the Peer's retention region.</summary>
    Sent = 0,

    /// <summary>
    /// The Peer's retention region had no room for the framed Message. Nothing was copied and nothing
    /// about the Session changed — the caller may retry once bytes have been released, or otherwise.
    /// </summary>
    RetentionExhausted = 1,

    /// <summary>
    /// A Goodbye for this Peer has already passed in one direction or the other — this end sent its
    /// own (<c>Close</c>, issue #23), or the far end's arrived and the Peer is <c>Closing</c>. Either
    /// way the Session is ending and nothing was copied: a Message queued after a Goodbye would have
    /// to either jump it in retention or follow it, and neither is a Message the far end has any
    /// business receiving.
    /// </summary>
    Closed = 2,
}
