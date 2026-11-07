using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// Which of the three records a stream handshake message on the wire carries. A one-byte tag prefixing
/// every record, so a Peer already waiting for a plain Hello can also recognise the in-protocol
/// Refusal that takes its place when the far end declines, or the Resume a re-dialling end sends in its
/// place to claim a Session the far end may still be holding.
/// </summary>
internal enum StreamHandshakeMessageType : byte
{
    /// <summary>The ordinary handshake record: version, announced parameters and the resume secret.</summary>
    Hello = 1,

    /// <summary>An in-protocol decline, carrying a <see cref="RefusalReason"/> and up to two values.</summary>
    Refusal = 2,

    /// <summary>
    /// A re-dialling end's claim on a Session the far end may still be holding, carrying only the
    /// resume secret and this end's delivery counter.
    /// </summary>
    Resume = 3,
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
/// A Refusal and a Resume are deliberately the same <see cref="RecordSize"/> as a Hello, padded with
/// zero bytes past their own shorter payloads, so a Peer waiting on a fixed-size handshake record never
/// needs to know which kind is coming before it has read the whole thing — only
/// <see cref="TryReadMessageType"/>'s first byte tells them apart.
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

    private const int ResumePayloadSize =
        StreamSessionProtocol.ResumeSecretLength
        + sizeof(ulong); // deliveryCount

    /// <summary>
    /// The exact size in bytes of one handshake record on the wire, whichever kind it carries — one
    /// message-type byte plus the largest of the three payloads, which is a Hello's.
    /// </summary>
    public const int RecordSize = sizeof(byte) + HelloPayloadSize;

    /// <summary>
    /// Rejects a destination buffer that is not exactly one record — every writer here fills a whole
    /// record or none of it, mirroring <see cref="ControlDatagramCodec"/>'s guard of the same shape.
    /// </summary>
    private static void ValidateDestinationSize(Span<byte> destination)
    {
        if (destination.Length != RecordSize)
        {
            throw new ArgumentException($"A handshake record buffer must be exactly {RecordSize} bytes.", nameof(destination));
        }
    }

    /// <summary>
    /// Rejects a caller-supplied span that is not exactly the fixed wire length its field occupies —
    /// a caller who cannot hold a full secret could not use one correctly.
    /// </summary>
    private static void ValidateFixedLength(ReadOnlySpan<byte> value, int expectedLength, string paramName)
    {
        if (value.Length != expectedLength)
        {
            throw new ArgumentException($"Must be exactly {expectedLength} bytes.", paramName);
        }
    }

    /// <summary>
    /// Whether every byte of <paramref name="record"/> past a payload of
    /// <paramref name="payloadSize"/> bytes is zero — the padding a writer here always clears. Non-zero
    /// padding fails the read, as it does in <see cref="ControlDatagramCodec.TryReadControlType"/>.
    /// </summary>
    private static bool IsPaddingZero(ReadOnlySpan<byte> record, int payloadSize) =>
        record[(1 + payloadSize)..].IndexOfAnyExcept((byte)0) == -1;

    /// <summary>
    /// Reads the one byte that tells a Hello, a Refusal and a Resume apart, without interpreting
    /// anything else. Fails
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
        return type is StreamHandshakeMessageType.Hello
            or StreamHandshakeMessageType.Refusal
            or StreamHandshakeMessageType.Resume;
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
        ValidateDestinationSize(destination);
        ValidateFixedLength(resumeSecret, StreamSessionProtocol.ResumeSecretLength, nameof(resumeSecret));

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
    /// hold a full secret could not use one correctly regardless of what the record contains. Also fails
    /// when the announced values are not <see cref="HandshakeLegality.IsLegalAnnouncement">legal</see>,
    /// the rule <see cref="ControlDatagramCodec.TryReadHelloWithCookie"/> also applies. The interval
    /// rule applies only to this end's <see cref="StreamSessionProtocol.Version"/>.
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

        if (!HandshakeLegality.IsLegalAnnouncement(
                speaksVersion: version == StreamSessionProtocol.Version,
                maxMessageSize,
                sendIdleIntervalMilliseconds,
                lingerMilliseconds))
        {
            version = default;
            maxMessageSize = default;
            sendIdleIntervalMilliseconds = default;
            lingerMilliseconds = default;
            return false;
        }

        record.Slice(offset, StreamSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }

    /// <summary>
    /// Writes one Refusal record naming <paramref name="reason"/> and up to two values, padded with
    /// zero bytes to <see cref="RecordSize"/> so it occupies exactly the same span a Hello would.
    /// </summary>
    public static void WriteRefusal(Span<byte> destination, RefusalReason reason, uint value0, uint value1)
    {
        ValidateDestinationSize(destination);

        destination[0] = (byte)StreamHandshakeMessageType.Refusal;
        destination[1] = (byte)reason;
        BinaryPrimitives.WriteUInt32BigEndian(destination[2..], value0);
        BinaryPrimitives.WriteUInt32BigEndian(destination[6..], value1);
        destination[(1 + RefusalPayloadSize)..].Clear();
    }

    /// <summary>
    /// Reads one Refusal record's payload. Fails when <paramref name="record"/> is not exactly
    /// <see cref="RecordSize"/> bytes, its message type is not
    /// <see cref="StreamHandshakeMessageType.Refusal"/>, its reason byte is not a
    /// <see cref="HandshakeLegality.IsLegalRefusalReason">legal reason</see>, or its padding is not
    /// zero. Those are the rules <see cref="ControlDatagramCodec.TryReadRefusal"/> applies.
    /// </summary>
    public static bool TryReadRefusal(ReadOnlySpan<byte> record, out RefusalReason reason, out uint value0, out uint value1)
    {
        if (record.Length != RecordSize
            || record[0] != (byte)StreamHandshakeMessageType.Refusal
            || !HandshakeLegality.IsLegalRefusalReason((RefusalReason)record[1])
            || !IsPaddingZero(record, RefusalPayloadSize))
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

    /// <summary>
    /// Writes one Resume record: the resume secret the far end issued for this Session, and this end's
    /// cumulative delivery counter, padded with zero bytes to <see cref="RecordSize"/> so it occupies
    /// exactly the same span a Hello would.
    /// </summary>
    /// <remarks>
    /// There is deliberately no field for version, <c>MaxMessageSize</c>, the send-idle interval or
    /// Linger. Those are announced once in a Hello and are immutable for the Session's lifetime, so a
    /// Resume has nothing to re-announce: only a process still holding the Session's state can present
    /// a valid secret, since the secret, the retention ring and the counter all live in memory and die
    /// with it. A process that restarted dials fresh and negotiates fresh, which is how a Session's
    /// parameters change. The secret authorises a match; it never selects one, so it is not a routing
    /// field.
    /// </remarks>
    public static void WriteResume(Span<byte> destination, ReadOnlySpan<byte> resumeSecret, ulong deliveryCount)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(resumeSecret, StreamSessionProtocol.ResumeSecretLength, nameof(resumeSecret));

        destination[0] = (byte)StreamHandshakeMessageType.Resume;
        int offset = 1;
        resumeSecret.CopyTo(destination.Slice(offset, StreamSessionProtocol.ResumeSecretLength));
        offset += StreamSessionProtocol.ResumeSecretLength;
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], deliveryCount);
        destination[(1 + ResumePayloadSize)..].Clear();
    }

    /// <summary>
    /// Reads one Resume record's payload. Fails when <paramref name="record"/> is not exactly
    /// <see cref="RecordSize"/> bytes, its message type is not
    /// <see cref="StreamHandshakeMessageType.Resume"/>, its padding is not zero, or
    /// <paramref name="resumeSecret"/> is not exactly its wire length, since a caller that cannot hold a
    /// full secret could not use one correctly regardless of what the record contains.
    /// </summary>
    public static bool TryReadResume(ReadOnlySpan<byte> record, Span<byte> resumeSecret, out ulong deliveryCount)
    {
        if (record.Length != RecordSize
            || resumeSecret.Length != StreamSessionProtocol.ResumeSecretLength
            || record[0] != (byte)StreamHandshakeMessageType.Resume
            || !IsPaddingZero(record, ResumePayloadSize))
        {
            deliveryCount = default;
            return false;
        }

        int offset = 1;
        record.Slice(offset, StreamSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        offset += StreamSessionProtocol.ResumeSecretLength;
        deliveryCount = BinaryPrimitives.ReadUInt64BigEndian(record[offset..]);
        return true;
    }
}
