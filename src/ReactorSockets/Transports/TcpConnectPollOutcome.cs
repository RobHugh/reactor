namespace ReactorSockets.Transports;

/// <summary>
/// The outcome of a single <see cref="TcpLink.PollConnect"/> call: the handshake is still in flight,
/// it finished successfully, or it failed. A dialling socket produces exactly one of
/// <see cref="Connected"/> or <see cref="Failed"/>, never both, and never more than once — once either
/// lands, the caller stops polling that Link.
/// </summary>
internal enum TcpConnectPollOutcome
{
    /// <summary>Neither writable nor errored yet; the OS has not finished the handshake.</summary>
    Pending,

    /// <summary>The handshake completed; the socket is ready to use as an ordinary connected stream.</summary>
    Connected,

    /// <summary>The handshake failed; the platform error travels on the result.</summary>
    Failed,
}
