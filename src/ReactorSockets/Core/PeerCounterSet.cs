namespace ReactorSockets.Core;

/// <summary>
/// The mutable per-Peer counter storage a Peer holds for its own lifetime, incremented on the hot
/// path with a single add and read only through <see cref="Snapshot"/> (ADR 0013). Mirrors
/// <see cref="LinkCounterSet"/>'s Raw-profile shape.
/// </summary>
internal sealed class PeerCounterSet
{
    private ulong _messagesReceived;

    /// <summary>
    /// Records that one whole Message was handed to the frame arena for delivery. Called exactly
    /// once per delivered Message, at the point the arena reservation commits — never when the
    /// Message's bytes finish arriving in the per-Peer receive buffer and never while it sits
    /// buffered waiting on a read share or arena room, so a Message held back this Frame does not
    /// move this figure until the Frame that actually delivers it.
    /// </summary>
    public void RecordMessageReceived() => _messagesReceived++;

    /// <summary>Takes a point-in-time snapshot of every counter belonging to this Peer.</summary>
    public PeerCounters Snapshot() => new(_messagesReceived);
}
