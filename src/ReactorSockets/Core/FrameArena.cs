namespace ReactorSockets.Core;

/// <summary>
/// The Frame arena (ADR 0012, as corrected by ADR 0014): one buffer per Reactor, wiped every Pump,
/// that every payload span the Reactor hands out is carved from. Bounds two things at once — the
/// arena's own total size, and each share-holder's derived fair share of it (<see cref="ReadShare"/>
/// = <see cref="ArenaSize"/> ÷ <see cref="Capacity"/>), with the escape that a share-holder may
/// always deliver one whole unit even when that exceeds its share. The escape never weakens the
/// arena's own bound — reading still stops when the arena fills.
/// </summary>
/// <remarks>
/// A share-holder is identified by a zero-based index below <see cref="Capacity"/> — a Link in Raw,
/// a Peer in a Session (ADR 0014). Per-share-holder usage is tracked in a preallocated array sized
/// once at construction, so no Frame allocates.
/// </remarks>
internal sealed class FrameArena
{
    private readonly byte[] _buffer;
    private readonly int[] _bytesUsedByShareHolder;

    /// <summary>
    /// Constructs an arena of <paramref name="arenaSize"/> bytes shared fairly across
    /// <paramref name="capacity"/> share-holders.
    /// </summary>
    /// <param name="arenaSize">
    /// The arena's total size in bytes. Profile-floor validation (ADR 0012) is the calling
    /// Reactor's responsibility via <see cref="ReactorOptions.ResolveArenaSize"/>; this constructor
    /// only rejects a non-positive size, which can never hold anything.
    /// </param>
    /// <param name="capacity">The number of share-holders this arena divides its size across.</param>
    public FrameArena(int arenaSize, int capacity)
    {
        if (arenaSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(arenaSize),
                arenaSize,
                $"arena of {arenaSize} bytes cannot hold anything — arena size must be at least 1.");
        }

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacity),
                capacity,
                $"capacity of {capacity} admits no share-holders — capacity must be at least 1.");
        }

        ArenaSize = arenaSize;
        Capacity = capacity;
        ReadShare = arenaSize / capacity;
        _buffer = new byte[arenaSize];
        _bytesUsedByShareHolder = new int[capacity];
    }

    /// <summary>The arena's total size in bytes.</summary>
    public int ArenaSize { get; }

    /// <summary>The number of share-holders this arena divides its size across.</summary>
    public int Capacity { get; }

    /// <summary>
    /// The derived per-share-holder byte fair-share for one Frame: <see cref="ArenaSize"/> ÷
    /// <see cref="Capacity"/> (ADR 0012, as corrected by ADR 0014). Not a knob — it falls out of
    /// the arena size and capacity a consumer already chose.
    /// </summary>
    public int ReadShare { get; }

    /// <summary>Bytes committed so far this Frame, across every share-holder.</summary>
    public int BytesUsed { get; private set; }

    /// <summary>The arena's remaining room this Frame: <see cref="ArenaSize"/> − <see cref="BytesUsed"/>.</summary>
    public int BytesFree => ArenaSize - BytesUsed;

    /// <summary>
    /// Attempts to commit <paramref name="data"/> into the arena on behalf of
    /// <paramref name="shareHolderId"/>. A share-holder's first request this Frame always escapes
    /// its fair share (though never the arena's own free space); a second request that would push
    /// it past <see cref="ReadShare"/> is refused as <see cref="ArenaReservationOutcome.ShareExhausted"/>
    /// without touching the arena. A request that does not fit in <see cref="BytesFree"/> — by
    /// either share-holder — is refused as <see cref="ArenaReservationOutcome.ArenaExhausted"/>.
    /// </summary>
    /// <param name="shareHolderId">A zero-based index below <see cref="Capacity"/>.</param>
    /// <param name="data">The bytes to commit — already read off the wire elsewhere.</param>
    public ArenaReservation Reserve(int shareHolderId, ReadOnlySpan<byte> data)
    {
        if (shareHolderId < 0 || shareHolderId >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(shareHolderId),
                shareHolderId,
                $"share-holder {shareHolderId} is outside the arena's capacity of {Capacity}.");
        }

        int bytesFreeAtRequest = BytesFree;
        if (data.Length > bytesFreeAtRequest)
        {
            return new ArenaReservation(ArenaReservationOutcome.ArenaExhausted, default, data.Length, bytesFreeAtRequest);
        }

        int bytesUsedByHolder = _bytesUsedByShareHolder[shareHolderId];
        if (bytesUsedByHolder > 0 && bytesUsedByHolder + data.Length > ReadShare)
        {
            return new ArenaReservation(ArenaReservationOutcome.ShareExhausted, default, data.Length, bytesFreeAtRequest);
        }

        Span<byte> destination = _buffer.AsSpan(BytesUsed, data.Length);
        data.CopyTo(destination);
        BytesUsed += data.Length;
        _bytesUsedByShareHolder[shareHolderId] = bytesUsedByHolder + data.Length;

        return new ArenaReservation(ArenaReservationOutcome.Committed, destination, data.Length, bytesFreeAtRequest);
    }

    /// <summary>
    /// Returns the span previously committed at <paramref name="offset"/> for <paramref name="length"/>
    /// bytes, so a caller that recorded a reservation's position — a <c>RawReactor</c> queuing an event
    /// for a later <c>Drain</c> within the same Frame — can hand it back out without holding the
    /// original <see cref="ArenaReservation"/> <see langword="ref struct"/> past its own stack frame.
    /// Valid only until the next <see cref="Reset"/>.
    /// </summary>
    public Span<byte> GetCommitted(int offset, int length) => _buffer.AsSpan(offset, length);

    /// <summary>
    /// Wipes the arena for the next Frame: every previously committed payload span is invalidated
    /// (the backing memory is reused, not cleared) and every share-holder's usage resets to zero.
    /// </summary>
    public void Reset()
    {
        BytesUsed = 0;
        Array.Clear(_bytesUsedByShareHolder);
    }
}
