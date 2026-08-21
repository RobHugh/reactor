namespace ReactorSockets;

/// <summary>
/// Which of a datagram Session's four delivery modes carries a Message sent through
/// <see cref="DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan{byte})"/> (ADR 0009,
/// issue #207). See <c>docs/delivery-modes.md</c> for the consumer-facing guide to choosing one; the
/// full guarantees each mode makes — ordering, duplicate suppression, what a late arrival does — land
/// in #44, and fragmentation of a Message wider than one Datagram lands in #45. This ticket only
/// routes a mode to its own Space.
/// </summary>
/// <remarks>
/// Explicit values throughout, the same house style as <see cref="DisconnectReason"/> and
/// <see cref="SendOutcome"/>: a member is added at the end with the next free number, never inserted,
/// so a value a consumer has already persisted keeps its meaning across a library upgrade. The values
/// match the wire's own mode bits (ADR 0009) so the two never need a lookup table to agree, though
/// this type is deliberately not the wire encoding itself — that is
/// <c>Session.PacketRecordMode</c>, internal to this library.
/// </remarks>
public enum DeliveryMode
{
    /// <summary>
    /// Delivered at most once per send, in whatever order it arrives, deduplicated within a bounded
    /// sliding window. Nothing under this mode is ever retransmitted.
    /// </summary>
    Unreliable = 0,

    /// <summary>
    /// Delivered only if it is strictly newer than the last Message this mode delivered; a Message
    /// superseded before it arrives is discarded rather than delivered late. Nothing under this mode
    /// is ever retransmitted.
    /// </summary>
    UnreliableSequenced = 1,

    /// <summary>
    /// Always delivered, in whatever order it arrives. Retransmitted until acknowledged, retaining
    /// what was sent in this mode's own reliable Space.
    /// </summary>
    ReliableUnordered = 2,

    /// <summary>
    /// Always delivered, in the order it was sent — a Message held up in transit blocks every later
    /// Message in this mode's Space until it arrives. Retransmitted until acknowledged, retaining what
    /// was sent in this mode's own reliable Space.
    /// </summary>
    ReliableOrdered = 3,
}
