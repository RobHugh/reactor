namespace ReactorSockets.Core;

/// <summary>
/// The outcome of a single <see cref="FrameArena.Reserve"/> call.
/// </summary>
internal enum ArenaReservationOutcome
{
    /// <summary>
    /// The requested bytes were copied into the arena. The returned payload is valid only until
    /// the next <see cref="FrameArena.Reset"/>.
    /// </summary>
    Committed,

    /// <summary>
    /// The share-holder already delivered its fair share this Frame (ADR 0012, as corrected by
    /// ADR 0014); refused before touching the arena so a loud share-holder cannot starve a quiet
    /// one in the same Frame.
    /// </summary>
    ShareExhausted,

    /// <summary>
    /// The arena itself has no room left for the request, regardless of the escape. The caller
    /// records <c>ArenaExhausted</c> (ADR 0013, id 2) with the reservation's
    /// <see cref="ArenaReservation.BytesRequested"/> and <see cref="ArenaReservation.BytesFree"/>.
    /// </summary>
    ArenaExhausted,
}
