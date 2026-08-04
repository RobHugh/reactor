namespace ReactorSockets;

/// <summary>
/// The mutable per-Link counter storage a Link holds for its own lifetime, incremented on the hot
/// path with a single add and read only through <see cref="Snapshot"/> (ADR 0013). A Link's counters
/// live as long as its identity does; a later Resume in a Session profile does not reset them, since
/// a count spanning multiple transports is what shows a flapping path.
/// </summary>
public sealed class LinkCounterSet
{
    private ulong _bytesSent;
    private ulong _bytesReceived;
    private ulong _readShareExhaustions;

    /// <summary>Records that <paramref name="byteCount"/> bytes were sent on this Link.</summary>
    public void RecordBytesSent(int byteCount) => _bytesSent += (ulong)byteCount;

    /// <summary>Records that <paramref name="byteCount"/> bytes were received on this Link.</summary>
    public void RecordBytesReceived(int byteCount) => _bytesReceived += (ulong)byteCount;

    /// <summary>Records that this Link's read share was exhausted for the current Frame.</summary>
    public void RecordReadShareExhaustion() => _readShareExhaustions++;

    /// <summary>Takes a point-in-time snapshot of every counter belonging to this Link.</summary>
    public LinkCounters Snapshot() => new(_bytesSent, _bytesReceived, _readShareExhaustions);
}
