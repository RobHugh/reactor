namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="SessionEvent"/> was drained, and therefore which of its fields carry
/// meaning. Shared by both Session profiles (ADR 0010).
/// </summary>
/// <remarks>
/// The roster grows with Resume across later slices. Members are appended, never inserted.
/// </remarks>
public enum SessionEventKind
{
    /// <summary>
    /// A Session is established and the Peer is addressable. Raised on both ends: on the dialler when
    /// the Accept arrives, on the accepting end when a Hello is admitted.
    /// </summary>
    PeerConnected = 1,

    /// <summary>
    /// A Session ended, or a dial never became one. <see cref="SessionEvent.Reason"/> says why, and
    /// the <see cref="PeerId"/> is dead from this event onwards.
    /// </summary>
    PeerClosed = 2,

    /// <summary>
    /// A whole Message was reassembled from the Peer's stream and copied into the frame arena.
    /// <see cref="SessionEvent.Payload"/> carries it, valid only until the next Pump (ADR 0008).
    /// </summary>
    MessageReceived = 3,

    /// <summary>
    /// The Peer's Goodbye arrived while something was still outstanding to deliver: either Messages
    /// this end had already reassembled from it but the consumer had not yet drained (issue #23 review
    /// finding — a Message queued ahead of the Goodbye in the same read counts, even though it reached
    /// <see cref="SessionEventKind.MessageReceived"/> first), or bytes still buffered in its receive
    /// share awaiting arena room (#81), or this end's own outbound retention for it still being
    /// flushed to the socket. The Peer accepts no further <c>Send</c>s worth making from here (issue
    /// #23) and a <see cref="SessionEventKind.PeerClosed"/> carrying
    /// <see cref="DisconnectReason.PeerGoodbye"/> follows once all of that is exhausted. Never raised
    /// when nothing was outstanding on either side — a Goodbye that finds the Peer already caught up
    /// closes it directly, with no zero-Frame Closing observable in between.
    /// </summary>
    PeerClosing = 4,

    /// <summary>
    /// The stream profile's transport for this Peer is gone, but the Peer itself is not: its
    /// <see cref="PeerId"/> — including generation — stays live, <see cref="StreamSessionReactor.CountersOf"/>
    /// keeps answering for it, and its undelivered inbound Messages and outbound retention ring are
    /// held rather than discarded (issue #119, ADR 0010). Raised whenever an established Peer's
    /// transport ends without a Goodbye first: an inbound FIN carrying nothing further, a reset, a
    /// local write failure, or this end's own receive-idle deadline expiring (revising issue #117,
    /// which closed the Peer outright before this kind existed). <b>Distinct from
    /// <see cref="PeerClosing"/></b>: Closing means a Goodbye arrived and the Session is ending on
    /// purpose, with <see cref="PeerClosed"/> to follow once both sides drain — nothing here is ever
    /// resumable. Interrupted means the opposite — no Goodbye was ever seen, the Session is not
    /// ending, and the Peer sits ready for a Resume (a later slice) for as long as Linger allows
    /// (issue #121). A Peer that sent Goodbye and then a bare FIN reaches <see cref="PeerClosing"/> or
    /// <see cref="PeerClosed"/>, never this. Carries only <see cref="SessionEvent.Peer"/> — there is no
    /// <see cref="DisconnectReason"/> to report, because nothing has ended.
    /// </summary>
    PeerInterrupted = 5,
}
