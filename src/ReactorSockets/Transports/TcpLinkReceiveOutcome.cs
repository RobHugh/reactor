namespace ReactorSockets.Transports;

/// <summary>
/// The outcome of a single <see cref="TcpLink.Receive"/> call, separating the three cases L2 must
/// treat differently: bytes actually arrived, the remote will send nothing further while this Link
/// stays open for sending, or the read failed outright.
/// </summary>
internal enum TcpLinkReceiveOutcome
{
    /// <summary>A run of bytes arrived and was copied into the caller's scratch buffer.</summary>
    Delivered,

    /// <summary>
    /// The read returned zero bytes — TCP's signal that the remote shut down its send half (a FIN),
    /// whether via an explicit half-close or as the first half of a full close. This Link's own send
    /// half is unaffected; L2 reports this as <c>EndOfStream</c>, never as <c>Closed</c>.
    /// </summary>
    EndOfStream,

    /// <summary>The read failed for a reason that ends the Link.</summary>
    Failed,
}
