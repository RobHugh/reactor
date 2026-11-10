namespace ReactorSockets.Session;

/// <summary>
/// Walks the packet records of one datagram body in order, one <see cref="PacketRecordCodec.TryRead"/>
/// per record. The single place a body is stepped through record by record: every loop that reads a
/// body uses this rather than re-slicing past each record itself.
/// </summary>
/// <remarks>
/// <see cref="TryReadNext"/> returns <see langword="false"/> both at the end of the body and at a record
/// that does not parse, and stops advancing either way. <see cref="IsAtEnd"/> tells the two apart, and
/// <see cref="Remaining"/> is positioned at the record that failed.
/// </remarks>
internal ref struct PacketRecordReader
{
    /// <summary>Creates a reader positioned at the first record of <paramref name="body"/>.</summary>
    /// <param name="body">The packet records of one datagram, after its header.</param>
    public PacketRecordReader(ReadOnlySpan<byte> body)
    {
        Remaining = body;
    }

    /// <summary>
    /// The bytes not yet read. After <see cref="TryReadNext"/> fails before the end, these start at the
    /// record that did not parse.
    /// </summary>
    public ReadOnlySpan<byte> Remaining { get; private set; }

    /// <summary>Whether every byte of the body has been read as a whole record.</summary>
    public readonly bool IsAtEnd => Remaining.IsEmpty;

    /// <summary>
    /// Reads the next record and advances past it. Returns <see langword="false"/> without advancing
    /// when the body is exhausted or the next record does not parse.
    /// </summary>
    /// <param name="record">The record read, valid only when this method returns <see langword="true"/>.</param>
    public bool TryReadNext(out PacketRecord record)
    {
        if (Remaining.IsEmpty || !PacketRecordCodec.TryRead(Remaining, out record))
        {
            record = default;
            return false;
        }

        Remaining = Remaining[record.Size..];
        return true;
    }
}
