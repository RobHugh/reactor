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

    /// <summary>
    /// The remote end reset the connection. Named for the remote rather than a "peer" deliberately:
    /// Peer is a Session word, and Raw leaves it free for whatever a consumer's own protocol layer
    /// calls its remote party. This is the same "remote" <see cref="RawReactor.RemoteOf"/> answers for.
    /// </summary>
    ResetByRemote,

    /// <summary>The remote end refused the connection.</summary>
    Refused,

    /// <summary>The destination was reported unreachable.</summary>
    Unreachable,

    /// <summary>The Link failed for a reason not covered by the other members.</summary>
    Failed,
}
