namespace ReactorSockets.Core;

/// <summary>
/// The result of a single <see cref="FrameArena.Reserve(int, ReadOnlySpan{byte})"/> or
/// <see cref="FrameArena.Reserve(int, int)"/> call. A <see langword="ref struct"/>
/// because <see cref="Payload"/> aliases the arena's backing memory directly — it must never
/// outlive the Frame, and a <see langword="ref struct"/> is the compiler-enforced way to say so.
/// </summary>
internal readonly ref struct ArenaReservation
{
    internal ArenaReservation(ArenaReservationOutcome outcome, Span<byte> payload, int bytesRequested, int bytesFree)
    {
        Outcome = outcome;
        Payload = payload;
        BytesRequested = bytesRequested;
        BytesFree = bytesFree;
    }

    /// <summary>Which of the three outcomes this reservation resolved to.</summary>
    public ArenaReservationOutcome Outcome { get; }

    /// <summary>
    /// The committed bytes, valid only until the next <see cref="FrameArena.Reset"/>. Empty unless
    /// <see cref="Outcome"/> is <see cref="ArenaReservationOutcome.Committed"/>.
    /// </summary>
    public Span<byte> Payload { get; }

    /// <summary>The size of the request that produced this reservation.</summary>
    public int BytesRequested { get; }

    /// <summary>
    /// The arena's free space at the moment this reservation was decided, before any bytes from
    /// this request were committed. What an <c>ArenaExhausted</c> diagnostic (ADR 0013, id 2)
    /// reports alongside <see cref="BytesRequested"/>.
    /// </summary>
    public int BytesFree { get; }
}
