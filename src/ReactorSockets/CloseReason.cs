namespace ReactorSockets;

/// <summary>
/// Why a Link closed, defined and kept stable by this library rather than borrowed from a platform
/// enumeration (ADR 0008): a consumer's permanent logic switches on this instead of a
/// <see cref="System.Net.Sockets.SocketError"/> that differs between Windows and Unix. The exact
/// <see cref="System.Net.Sockets.SocketError"/> travels in the <see cref="DiagnosticRecord"/> emitted
/// at the same moment, linked to this only by the Link and the Pump it happened in.
/// </summary>
public enum CloseReason
{
    /// <summary>The consumer called <c>Close</c> on this Link.</summary>
    LocalClose,

    /// <summary>The peer reset the connection.</summary>
    ResetByPeer,

    /// <summary>The peer refused the connection.</summary>
    Refused,

    /// <summary>The destination was reported unreachable.</summary>
    Unreachable,

    /// <summary>The Link failed for a reason not covered by the other members.</summary>
    Failed,
}
