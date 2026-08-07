using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The single place the stream Session handshake's wire format is encoded and decoded. Unlike the
/// datagram profile's multi-flight exchange, a stream handshake is symmetric: both ends send exactly
/// one fixed-size record the instant their TCP connection exists — the dialler once <c>Connect</c>
/// resolves, the accepting end once it accepts — and each is established once it has both sent its own
/// record and read the other's in full. There is no request/response role because TCP has already paid
/// for the connection; nothing here waits for the far end to speak first.
/// </summary>
/// <remarks>
/// Allocation-free by construction: every member works over <see cref="Span{T}"/> or
/// <see cref="ReadOnlySpan{T}"/> supplied by the caller. Multi-byte integers are big-endian throughout.
/// Durations travel as whole milliseconds rather than as <see cref="TimeSpan"/> ticks, so the wire
/// format never depends on a platform's tick resolution.
/// </remarks>
internal static class StreamHandshakeCodec
{
    /// <summary>The exact size in bytes of one handshake record.</summary>
    public const int RecordSize =
        sizeof(uint) // version
        + sizeof(uint) // maxMessageSize
        + sizeof(ulong) // sendIdleIntervalMilliseconds
        + sizeof(ulong) // lingerMilliseconds
        + StreamSessionProtocol.ResumeSecretLength;

    /// <summary>
    /// Writes one handshake record: this end's version, its own <c>MaxMessageSize</c> announced as a
    /// receive limit, its send-idle interval and Linger, and the resume secret it will require before
    /// resuming this Session.
    /// </summary>
    public static void Write(
        Span<byte> destination,
        uint version,
        uint maxMessageSize,
        ulong sendIdleIntervalMilliseconds,
        ulong lingerMilliseconds,
        ReadOnlySpan<byte> resumeSecret)
    {
        if (destination.Length != RecordSize)
        {
            throw new ArgumentException($"A handshake record buffer must be exactly {RecordSize} bytes.", nameof(destination));
        }

        if (resumeSecret.Length != StreamSessionProtocol.ResumeSecretLength)
        {
            throw new ArgumentException(
                $"Must be exactly {StreamSessionProtocol.ResumeSecretLength} bytes.", nameof(resumeSecret));
        }

        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], version);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], maxMessageSize);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], sendIdleIntervalMilliseconds);
        offset += sizeof(ulong);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], lingerMilliseconds);
        offset += sizeof(ulong);
        resumeSecret.CopyTo(destination.Slice(offset, StreamSessionProtocol.ResumeSecretLength));
    }

    /// <summary>
    /// Reads one handshake record. Fails when <paramref name="record"/> is not exactly
    /// <see cref="RecordSize"/> bytes or <paramref name="resumeSecret"/> is not exactly its wire
    /// length, since a caller that cannot hold a full secret could not use one correctly regardless of
    /// what the record contains.
    /// </summary>
    public static bool TryRead(
        ReadOnlySpan<byte> record,
        out uint version,
        out uint maxMessageSize,
        out ulong sendIdleIntervalMilliseconds,
        out ulong lingerMilliseconds,
        Span<byte> resumeSecret)
    {
        if (record.Length != RecordSize || resumeSecret.Length != StreamSessionProtocol.ResumeSecretLength)
        {
            version = default;
            maxMessageSize = default;
            sendIdleIntervalMilliseconds = default;
            lingerMilliseconds = default;
            return false;
        }

        int offset = 0;
        version = BinaryPrimitives.ReadUInt32BigEndian(record[offset..]);
        offset += sizeof(uint);
        maxMessageSize = BinaryPrimitives.ReadUInt32BigEndian(record[offset..]);
        offset += sizeof(uint);
        sendIdleIntervalMilliseconds = BinaryPrimitives.ReadUInt64BigEndian(record[offset..]);
        offset += sizeof(ulong);
        lingerMilliseconds = BinaryPrimitives.ReadUInt64BigEndian(record[offset..]);
        offset += sizeof(ulong);
        record.Slice(offset, StreamSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }
}
