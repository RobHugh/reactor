namespace ReactorSockets.Session;

/// <summary>
/// The four values each end announces about itself during the handshake, carried in the same order
/// by both a <see cref="DatagramControlType.HelloWithCookie"/> (the dialler's) and an
/// <see cref="DatagramControlType.Accept"/> (the accepting end's). <see cref="ControlDatagramCodec"/>
/// writes and reads them as one value so the two datagrams cannot drift apart on these fields.
/// </summary>
/// <param name="SessionIdForMe">The session id the announcing end wants stamped on datagrams sent to it.</param>
/// <param name="MaxMessageSize">The largest Message the announcing end will accept, in bytes. Never zero once read.</param>
/// <param name="SendIdleIntervalMilliseconds">
/// The announcing end's send-idle interval, which the other end needs to compute its receive-idle
/// deadline. Never zero once read.
/// </param>
/// <param name="LingerMilliseconds">
/// The announcing end's Linger, which the other end negotiates down to the minimum of the two. Never
/// zero once read.
/// </param>
internal readonly record struct AnnouncedSessionTerms(
    uint SessionIdForMe,
    uint MaxMessageSize,
    ulong SendIdleIntervalMilliseconds,
    ulong LingerMilliseconds);
