namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="RawEvent"/> a drained event is. This slice implements the datagram-relevant
/// kinds only; the stream-relevant kinds join this roster in slice 2 (ADR 0008).
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
}
