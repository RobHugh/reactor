namespace ReactorSockets;

/// <summary>
/// A closed, permanent enumeration of diagnostic causes with explicit values in reserved blocks —
/// common 1-99, Raw 100-199, datagram session 200-299, stream session 300-399 (ADR 0013) — so a
/// future insertion cannot silently renumber an id a consumer has persisted. There is no
/// <c>None</c> or <c>Unknown</c> member and zero is never used: every <see cref="DiagnosticRecord"/>
/// has a cause by construction.
/// </summary>
public enum DiagnosticId
{
    /// <summary>
    /// The configured admission cap was hit this Pump while a connection was still waiting in the
    /// OS backlog. No subject. <c>Value0</c> is the cap, <c>Value1</c> is the number of deferrals —
    /// listening Links that still had a pending connection when the cap ran out, not the number of
    /// connections left waiting. That depth is unknowable without accepting, which is exactly what
    /// hitting the cap stops the Reactor from doing, so it is deliberately not claimed here.
    /// </summary>
    AdmissionCapReached = 1,

    /// <summary>
    /// The frame arena had no room left for a request, regardless of the read-share escape.
    /// Subject is the Link that made the request. <c>Value0</c> is the bytes requested,
    /// <c>Value1</c> is the arena's bytes free at the moment of the request.
    /// </summary>
    ArenaExhausted = 2,

    /// <summary>
    /// Every capacity slot was occupied this Pump while a connection was still waiting in the OS
    /// backlog — a different exhaustion from <see cref="AdmissionCapReached"/>, reachable because the
    /// admission cap can be configured above <see cref="ReactorOptions.Capacity"/>. No subject.
    /// <c>Value0</c> is the capacity, <c>Value1</c> is the number of deferrals, counted the same way
    /// and with the same limit as <see cref="AdmissionCapReached"/>'s.
    /// </summary>
    CapacityExhausted = 3,

    /// <summary>
    /// A registered Link failed and closed. Subject is the Link. <c>Value0</c> carries the
    /// platform <see cref="System.Net.Sockets.SocketError"/> as its underlying integer value;
    /// <c>Value1</c> is unused.
    /// </summary>
    LinkFailed = 100,

    /// <summary>
    /// An ICMP port-unreachable (or platform equivalent) was reported for a send. Subject is the
    /// Link. <c>Value0</c> carries the platform <see cref="System.Net.Sockets.SocketError"/> as its
    /// underlying integer value; <c>Value1</c> is unused.
    /// </summary>
    DestinationUnreachable = 101,

    /// <summary>
    /// A datagram exceeded the Reactor's scratch buffer and was truncated. Subject is the claimed
    /// Source. <c>Value0</c> is the scratch buffer size in bytes; <c>Value1</c> is unused.
    /// </summary>
    DatagramTruncated = 102,
}
