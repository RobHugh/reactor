using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// Which of the two records a stream handshake message on the wire carries. A one-byte tag prefixing
/// every record, so a Peer already waiting for a plain Hello can also recognise the in-protocol
/// Refusal that takes its place when the far end declines (subtask 3.3).
/// </summary>
internal enum StreamHandshakeMessageType : byte
{
    /// <summary>The ordinary handshake record: version, announced parameters and the resume secret.</summary>
    Hello = 1,

    /// <summary>An in-protocol decline, carrying a <see cref="RefusalReason"/> and up to two values.</summary>
    Refusal = 2,
}

/// <summary>
/// The single place the stream Session handshake's wire format is encoded and decoded. Unlike the
/// datagram profile's multi-flight exchange, a stream handshake is symmetric: both ends send exactly
/// one fixed-size record the instant their TCP connection exists — the dialler once <c>Connect</c>
/// resolves, the accepting end once it accepts — and each is established once it has both sent its own
/// record and read the other's in full. There is no request/response role because TCP has already paid
/// for the connection; nothing here waits for the far end to speak first.
/// </summary>
/// <remarks>
/// <para>
/// Allocation-free by construction: every member works over <see cref="Span{T}"/> or
/// <see cref="ReadOnlySpan{T}"/> supplied by the caller. Multi-byte integers are big-endian throughout.
/// Durations travel as whole milliseconds rather than as <see cref="TimeSpan"/> ticks, so the wire
/// format never depends on a platform's tick resolution.
/// </para>
/// <para>
/// A Refusal is deliberately the same <see cref="RecordSize"/> as a Hello, padded with zero bytes past
/// its own shorter payload, so a Peer waiting on a fixed-size handshake record never needs to know
/// which kind is coming before it has read the whole thing — only <see cref="TryReadMessageType"/>'s
/// first byte tells it apart.
/// </para>
/// </remarks>
internal static class StreamHandshakeCodec
{
    private const int HelloPayloadSize =
        sizeof(uint) // version
        + sizeof(uint) // maxMessageSize
        + sizeof(ulong) // sendIdleIntervalMilliseconds
        + sizeof(ulong) // lingerMilliseconds
        + StreamSessionProtocol.ResumeSecretLength;

    private const int RefusalPayloadSize =
        sizeof(byte) // reason
        + sizeof(uint) // value0
        + sizeof(uint); // value1

    /// <summary>
    /// The exact size in bytes of one handshake record on the wire, whether it carries a Hello or a
    /// Refusal — one message-type byte plus the larger of the two payloads.
    /// </summary>
    public const int RecordSize = sizeof(byte) + HelloPayloadSize;

    /// <summary>
    /// Reads the one byte that tells a Hello from a Refusal, without interpreting anything else. Fails
    /// when <paramref name="record"/> is not exactly <see cref="RecordSize"/> bytes or its first byte
    /// names no member of <see cref="StreamHandshakeMessageType"/> — the discard arm refuses.
    /// </summary>
    public static bool TryReadMessageType(ReadOnlySpan<byte> record, out StreamHandshakeMessageType type)
    {
        if (record.Length != RecordSize)
        {
            type = default;
            return false;
        }

        type = (StreamHandshakeMessageType)record[0];
        return type is StreamHandshakeMessageType.Hello or StreamHandshakeMessageType.Refusal;
    }

    /// <summary>
    /// Writes one Hello record: this end's version, its own <c>MaxMessageSize</c> announced as a
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

        destination[0] = (byte)StreamHandshakeMessageType.Hello;
        int offset = 1;
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
    /// Reads one Hello record's payload. Fails when <paramref name="record"/> is not exactly
    /// <see cref="RecordSize"/> bytes, its message type is not <see cref="StreamHandshakeMessageType.Hello"/>,
    /// or <paramref name="resumeSecret"/> is not exactly its wire length, since a caller that cannot
    /// hold a full secret could not use one correctly regardless of what the record contains.
    /// </summary>
    public static bool TryRead(
        ReadOnlySpan<byte> record,
        out uint version,
        out uint maxMessageSize,
        out ulong sendIdleIntervalMilliseconds,
        out ulong lingerMilliseconds,
        Span<byte> resumeSecret)
    {
        if (record.Length != RecordSize
            || resumeSecret.Length != StreamSessionProtocol.ResumeSecretLength
            || record[0] != (byte)StreamHandshakeMessageType.Hello)
        {
            version = default;
            maxMessageSize = default;
            sendIdleIntervalMilliseconds = default;
            lingerMilliseconds = default;
            return false;
        }

        int offset = 1;
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

    /// <summary>
    /// Writes one Refusal record naming <paramref name="reason"/> and up to two values, padded with
    /// zero bytes to <see cref="RecordSize"/> so it occupies exactly the same span a Hello would.
    /// </summary>
    public static void WriteRefusal(Span<byte> destination, RefusalReason reason, uint value0, uint value1)
    {
        if (destination.Length != RecordSize)
        {
            throw new ArgumentException($"A handshake record buffer must be exactly {RecordSize} bytes.", nameof(destination));
        }

        destination[0] = (byte)StreamHandshakeMessageType.Refusal;
        destination[1] = (byte)reason;
        BinaryPrimitives.WriteUInt32BigEndian(destination[2..], value0);
        BinaryPrimitives.WriteUInt32BigEndian(destination[6..], value1);
        destination[(1 + RefusalPayloadSize)..].Clear();
    }

    /// <summary>
    /// Reads one Refusal record's payload. Fails when <paramref name="record"/> is not exactly
    /// <see cref="RecordSize"/> bytes or its message type is not
    /// <see cref="StreamHandshakeMessageType.Refusal"/>.
    /// </summary>
    public static bool TryReadRefusal(ReadOnlySpan<byte> record, out RefusalReason reason, out uint value0, out uint value1)
    {
        if (record.Length != RecordSize || record[0] != (byte)StreamHandshakeMessageType.Refusal)
        {
            reason = default;
            value0 = default;
            value1 = default;
            return false;
        }

        reason = (RefusalReason)record[1];
        value0 = BinaryPrimitives.ReadUInt32BigEndian(record[2..]);
        value1 = BinaryPrimitives.ReadUInt32BigEndian(record[6..]);
        return true;
    }
}
