namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="RawEvent"/> a drained event is (ADR 0008). <see cref="Accepted"/> comes
/// from a listening Link, <see cref="Connected"/> from dialling, <see cref="SegmentReceived"/> and
/// <see cref="EndOfStream"/> from stream reads, and <see cref="Writable"/> from partial sends.
/// </summary>
public enum RawEventKind
{
    /// <summary>
    /// A datagram arrived on a bound UDP Link. <see cref="RawEvent.Link"/>, <see cref="RawEvent.Source"/>
    /// and <see cref="RawEvent.Payload"/> are populated.
    /// </summary>
    DatagramReceived,

    /// <summary>A Link closed. <see cref="RawEvent.Link"/> and <see cref="RawEvent.Reason"/> are populated.</summary>
    Closed,

    /// <summary>
    /// A connection was accepted on a listening Link, inside the Pump that found it ready.
    /// <see cref="RawEvent.ListeningLink"/> names the listening Link that fired; <see cref="RawEvent.Link"/>
    /// names the new connection, already registered and consuming a capacity slot of its own.
    /// </summary>
    Accepted,

    /// <summary>
    /// An outbound dial completed successfully, inside the Pump that found the connecting socket
    /// writable. <see cref="RawEvent.Link"/> is populated; a dial that fails instead produces
    /// <see cref="Closed"/> with <see cref="CloseReason.Refused"/> or <see cref="CloseReason.Unreachable"/>.
    /// </summary>
    Connected,

    /// <summary>
    /// A run of bytes arrived on a stream Link. <see cref="RawEvent.Link"/> and
    /// <see cref="RawEvent.Payload"/> are populated. Carries no claim of being a whole anything — a
    /// counterparty's single write may be drained as one or several of these, and framing is the
    /// consumer's decision, not this library's.
    /// </summary>
    SegmentReceived,

    /// <summary>
    /// The remote end of a stream Link will send nothing further while this Link remains open for
    /// sending — TCP's own half-close, kept separate from <see cref="Closed"/> because collapsing them
    /// would decide on the consumer's behalf that their protocol cannot use half-close.
    /// <see cref="RawEvent.Link"/> is populated. A consumer that never calls <c>Close</c> after this
    /// holds the Link and its slot until the Reactor is disposed — there is no Linger in Raw to
    /// reclaim it.
    /// </summary>
    EndOfStream,

    /// <summary>
    /// A stream Link that took fewer bytes than offered on a previous <c>Send</c> has become writable
    /// again. <see cref="RawEvent.Link"/> is populated. Armed, not level-triggered: exactly one of
    /// these follows a short send, and a Link never over-sent on produces none at all — a reply to
    /// something the consumer did, not a status the Reactor volunteers every Pump a socket happens to
    /// be writable (which is nearly always).
    /// </summary>
    Writable,
}
