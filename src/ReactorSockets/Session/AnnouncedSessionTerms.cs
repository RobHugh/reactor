namespace ReactorSockets.Session;

/// <summary>
/// What each end announces about itself during the datagram handshake, carried in the same order
/// by both a <see cref="DatagramControlType.HelloWithCookie"/> (the dialler's) and an
/// <see cref="DatagramControlType.Accept"/> (the accepting end's). <see cref="ControlDatagramCodec"/>
/// writes and reads them as one value so the two datagrams cannot drift apart on these fields.
/// </summary>
/// <param name="SessionIdForMe">The session id the announcing end wants stamped on datagrams sent to it.</param>
/// <param name="Limits">
/// The announcing end's limits, the same block a stream Hello carries. Once read, they are
/// <see cref="AnnouncedLimits.IsLegal">legal</see> for the version they arrived with.
/// </param>
internal readonly record struct AnnouncedSessionTerms(uint SessionIdForMe, AnnouncedLimits Limits);
