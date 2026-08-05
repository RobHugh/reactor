namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="RawEvent"/> a drained event is. This slice adds the stream-relevant kinds
/// to the datagram-only roster shipped earlier (ADR 0008); <see cref="Accepted"/> lands with the
/// listening-Link work, <see cref="Connected"/> with dialling, <see cref="SegmentReceived"/> and
/// <see cref="EndOfStream"/> with stream reads, and <c>Writable</c> joins as the rest of slice 2 does.
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
    /// <see cref="RawEvent.Listener"/> names the listening Link that fired; <see cref="RawEvent.Link"/>
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
}
