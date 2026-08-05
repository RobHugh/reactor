namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="RawEvent"/> a drained event is. This slice adds the stream-relevant kinds
/// to the datagram-only roster shipped earlier (ADR 0008); <see cref="Accepted"/> lands with the
/// listening-Link work, <see cref="Connected"/> with dialling, and <c>SegmentReceived</c>,
/// <c>Writable</c> and <c>EndOfStream</c> join as the rest of slice 2 does.
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
}
