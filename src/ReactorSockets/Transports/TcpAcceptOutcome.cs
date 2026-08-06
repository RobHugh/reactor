namespace ReactorSockets.Transports;

/// <summary>
/// The outcome of a single <see cref="TcpListenLink.TryAccept"/> call: nothing was waiting, a
/// connection was pulled off the backlog, or the accept itself failed. The listening socket keeps
/// running for all three — see <see cref="TcpListenLink.TryAccept"/> for why an accept failure never
/// closes the listening socket.
/// </summary>
internal enum TcpAcceptOutcome
{
    /// <summary>No connection was waiting in the backlog at the moment of the call.</summary>
    NoPendingConnection,

    /// <summary>A connection was accepted; the new socket travels on the result.</summary>
    Accepted,

    /// <summary>The accept attempt itself failed; the platform error travels on the result.</summary>
    Failed,
}
