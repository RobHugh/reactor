namespace ReactorSockets;

/// <summary>
/// One reliable Space's acknowledgement as it rides in a data datagram header: a cumulative base and a
/// 32-bit field whose bit <c>n</c> (0-based) is set exactly when <c>Base + n + 1</c> has been received.
/// </summary>
/// <remarks>
/// Internal, like the <see cref="ClosedSessionAck"/> that carries two of them (ADR 0022).
/// </remarks>
/// <param name="Base">The next sequence not yet received in order.</param>
/// <param name="Bits">Which of the 32 sequences above <paramref name="Base"/> have been received.</param>
internal readonly record struct AckBlock(uint Base, uint Bits);
