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
    /// own (<c>Close</c>), or the far end's arrived and the Peer is <c>Closing</c>. Either way the
    /// Session is ending and nothing was copied: a Message queued after a Goodbye would have to either
    /// jump it in retention or follow it, and neither is a Message the far end has any business
    /// receiving.
    /// </summary>
    Closed = 2,

    /// <summary>
    /// The Message is wider than one Datagram can carry, and the Space it was sent on cannot
    /// fragment it. On the datagram profile that cap is the Peer's send Fragment size: the smaller of
    /// this end's <see cref="DatagramSessionOptions.ResolvedFragmentSize"/> and the Fragment size the
    /// peer announced (ADR 0024). Nothing was copied. Deliberately its own value rather than folded into
    /// <see cref="RetentionExhausted"/>: retention exhaustion is backpressure the caller can retry
    /// once bytes are released, while an oversized Message will never fit however long the caller
    /// waits — a consumer that cannot tell the two apart cannot tell "retry later" from "split it or
    /// use a reliable mode".
    /// </summary>
    MessageTooLarge = 3,

    /// <summary>
    /// A Message, in any delivery mode, exceeds the <c>MaxMessageSize</c> the remote end announced during
    /// the handshake, or a reliable Message needs more Fragments than the receive window the remote end
    /// announced (ADR 0024). Nothing was copied, and the caller may retry immediately with a smaller payload.
    /// An unreliable Message over both limits reports <see cref="MessageTooLarge"/> instead. Distinct
    /// from <see cref="MessageTooLarge"/> by which limit was exceeded: that value means the
    /// single-Datagram cap an unreliable mode cannot exceed, which a smaller peer Fragment size lowers;
    /// this one means the far end's announced receive limit, its <c>MaxMessageSize</c> or its receive
    /// window.
    /// </summary>
    RemoteMessageTooLarge = 4,

    /// <summary>
    /// The Peer's handshake has not completed: a dial is still connecting, or waiting for the far end's
    /// handshake record. Nothing was copied and nothing about the Session changed. Wait for
    /// <see cref="SessionEventKind.PeerConnected"/> for this Peer before Sending. Refusing here rather
    /// than queueing keeps a Message from reaching a socket whose connect has not resolved, or going
    /// out before this end knows the far end's version and announced <c>MaxMessageSize</c>.
    /// </summary>
    NotEstablished = 5,
}
