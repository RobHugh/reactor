using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The single place the pre-Session control datagram wire format (ADR 0009) is encoded and decoded.
/// Every other piece of the handshake reasons about a <see cref="Hello"/>, a <see cref="Cookie"/> and
/// so on as fields; only this class knows where those fields live inside the 256 bytes on the wire, so
/// a layout change touches one file instead of every caller that peeks at an offset.
/// </summary>
/// <remarks>
/// Allocation-free by construction: every member works over <see cref="Span{T}"/> or
/// <see cref="ReadOnlySpan{T}"/> supplied by the caller, because a control datagram is answered to
/// traffic from an unverified sender and the whole point of the cookie exchange (ADR 0009) is to
/// avoid spending anything — including a heap allocation — before that sender has proven it can
/// receive at the address it claims. Multi-byte integers are big-endian throughout, matched by every
/// reader and writer here rather than left to whatever the host's native order happens to be.
/// </remarks>
internal static class ControlDatagramCodec
{
    /// <summary>
    /// Throws when <paramref name="destination"/> is not exactly
    /// <see cref="DatagramSessionProtocol.ControlDatagramSize"/> bytes. Every writer is handed a
    /// buffer this class's own caller sized, never bytes a remote sent, so a mismatch here is our own
    /// bug and an exception is the right way to say so.
    /// </summary>
    private static void ValidateDestinationSize(Span<byte> destination)
    {
        if (destination.Length != DatagramSessionProtocol.ControlDatagramSize)
        {
            throw new ArgumentException(
                $"A control datagram buffer must be exactly {DatagramSessionProtocol.ControlDatagramSize} bytes.",
                nameof(destination));
        }
    }

    /// <summary>
    /// Throws when <paramref name="value"/> is not exactly <paramref name="expectedLength"/> bytes.
    /// Shared by the cookie and resume-secret parameters, which like the destination buffer are
    /// supplied by our own code and so are validated by exception rather than by a bool return.
    /// </summary>
    private static void ValidateFixedLength(ReadOnlySpan<byte> value, int expectedLength, string paramName)
    {
        if (value.Length != expectedLength)
        {
            throw new ArgumentException($"Must be exactly {expectedLength} bytes.", paramName);
        }
    }

    /// <summary>
    /// Writes the four-byte session id and the type byte common to every control datagram, after
    /// clearing <paramref name="destination"/> to zero so the padding rule holds without every writer
    /// having to zero its own unused tail.
    /// </summary>
    private static void WriteHeader(Span<byte> destination, DatagramControlType type)
    {
        destination.Clear();
        BinaryPrimitives.WriteUInt32BigEndian(destination, DatagramSessionProtocol.ControlSession);
        destination[DatagramSessionProtocol.ControlTypeOffset] = (byte)type;
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.Hello"/>: a dialler's opening bid, carrying only what
    /// must survive to be echoed back, since a Hello is answered before any state is committed to it.
    /// </summary>
    public static void WriteHello(Span<byte> destination, uint version, ulong clientNonce)
    {
        ValidateDestinationSize(destination);
        WriteHeader(destination, DatagramControlType.Hello);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], version);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.Cookie"/>: the stateless authenticator a dialler must
    /// echo back before this Reactor commits any resources to it.
    /// </summary>
    public static void WriteCookie(Span<byte> destination, ulong clientNonce, long epoch, ReadOnlySpan<byte> cookie)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(cookie, DatagramSessionProtocol.CookieLength, nameof(cookie));
        WriteHeader(destination, DatagramControlType.Cookie);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
        offset += sizeof(ulong);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], unchecked((ulong)epoch));
        offset += sizeof(ulong);
        cookie.CopyTo(destination.Slice(offset, DatagramSessionProtocol.CookieLength));
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.HelloWithCookie"/>: the Hello repeated with the Cookie
    /// echoed, carrying everything a Session needs to exist now that the round trip is paid.
    /// </summary>
    public static void WriteHelloWithCookie(
        Span<byte> destination,
        uint version,
        ulong clientNonce,
        long epoch,
        ReadOnlySpan<byte> cookie,
        uint sessionIdForMe,
        uint maxMessageSize,
        ReadOnlySpan<byte> resumeSecret)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(cookie, DatagramSessionProtocol.CookieLength, nameof(cookie));
        ValidateFixedLength(resumeSecret, DatagramSessionProtocol.ResumeSecretLength, nameof(resumeSecret));
        WriteHeader(destination, DatagramControlType.HelloWithCookie);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], version);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
        offset += sizeof(ulong);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], unchecked((ulong)epoch));
        offset += sizeof(ulong);
        cookie.CopyTo(destination.Slice(offset, DatagramSessionProtocol.CookieLength));
        offset += DatagramSessionProtocol.CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], sessionIdForMe);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], maxMessageSize);
        offset += sizeof(uint);
        resumeSecret.CopyTo(destination.Slice(offset, DatagramSessionProtocol.ResumeSecretLength));
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.Accept"/>: the accepting end's agreement, carrying the
    /// session id it wants stamped on datagrams sent to it.
    /// </summary>
    public static void WriteAccept(
        Span<byte> destination,
        ulong clientNonce,
        uint sessionIdForMe,
        uint maxMessageSize,
        ReadOnlySpan<byte> resumeSecret)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(resumeSecret, DatagramSessionProtocol.ResumeSecretLength, nameof(resumeSecret));
        WriteHeader(destination, DatagramControlType.Accept);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
        offset += sizeof(ulong);
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], sessionIdForMe);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], maxMessageSize);
        offset += sizeof(uint);
        resumeSecret.CopyTo(destination.Slice(offset, DatagramSessionProtocol.ResumeSecretLength));
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.Refusal"/>: the reply to a cookie-validated
    /// <see cref="DatagramControlType.HelloWithCookie"/> this Reactor will not answer with a Session.
    /// The version fields are meaningful only for a <see cref="RefusalReason.Version"/> refusal — the
    /// caller passes zero for both otherwise, matching <see cref="ControlDatagramLegality"/>'s note
    /// that this is one type carrying two reasons rather than two types.
    /// </summary>
    public static void WriteRefusal(
        Span<byte> destination,
        ulong clientNonce,
        RefusalReason reason,
        uint versionOffered,
        uint versionRequired)
    {
        ValidateDestinationSize(destination);
        WriteHeader(destination, DatagramControlType.Refusal);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
        offset += sizeof(ulong);
        destination[offset] = (byte)reason;
        offset += sizeof(byte);
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], versionOffered);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], versionRequired);
    }

    /// <summary>
    /// Reads the session id from the first four bytes of any datagram, control or data. Used before a
    /// datagram's kind is even known, so it asks nothing of the bytes beyond having four of them.
    /// </summary>
    public static bool TryReadSessionId(ReadOnlySpan<byte> datagram, out uint session)
    {
        if (datagram.Length < sizeof(uint))
        {
            session = default;
            return false;
        }

        session = BinaryPrimitives.ReadUInt32BigEndian(datagram);
        return true;
    }

    /// <summary>
    /// Reads and validates a control datagram's type in one pass: exact size, the reserved control
    /// session id, a type this profile processes, and zero padding from the end of the body to the
    /// end of the datagram. Every <c>TryRead*</c> body reader calls this first, so none of them can be
    /// fed bytes that only look like their type.
    /// </summary>
    public static bool TryReadControlType(ReadOnlySpan<byte> datagram, out DatagramControlType type)
    {
        if (datagram.Length != DatagramSessionProtocol.ControlDatagramSize)
        {
            type = default;
            return false;
        }

        uint session = BinaryPrimitives.ReadUInt32BigEndian(datagram);
        if (session != DatagramSessionProtocol.ControlSession)
        {
            type = default;
            return false;
        }

        DatagramControlType candidate = (DatagramControlType)datagram[DatagramSessionProtocol.ControlTypeOffset];
        ControlDatagramShape shape = ControlDatagramLegality.ShapeOf(candidate);
        if (!shape.IsProcessed)
        {
            type = default;
            return false;
        }

        int paddingStart = DatagramSessionProtocol.ControlBodyOffset + shape.BodyLength;
        if (datagram[paddingStart..].IndexOfAnyExcept((byte)0) != -1)
        {
            type = default;
            return false;
        }

        type = candidate;
        return true;
    }

    /// <summary>Reads a <see cref="DatagramControlType.Hello"/>'s body.</summary>
    public static bool TryReadHello(ReadOnlySpan<byte> datagram, out uint version, out ulong clientNonce)
    {
        if (!TryReadControlType(datagram, out DatagramControlType type) || type != DatagramControlType.Hello)
        {
            version = default;
            clientNonce = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        version = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]);
        return true;
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.Cookie"/>'s body. Fails when <paramref name="cookie"/>
    /// is not exactly <see cref="DatagramSessionProtocol.CookieLength"/> bytes, since a caller that
    /// cannot hold a full cookie could not use one correctly regardless of what the datagram contains.
    /// </summary>
    public static bool TryReadCookie(ReadOnlySpan<byte> datagram, out ulong clientNonce, out long epoch, Span<byte> cookie)
    {
        if (cookie.Length != DatagramSessionProtocol.CookieLength
            || !TryReadControlType(datagram, out DatagramControlType type)
            || type != DatagramControlType.Cookie)
        {
            clientNonce = default;
            epoch = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]);
        offset += sizeof(ulong);
        epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]));
        offset += sizeof(ulong);
        datagram.Slice(offset, DatagramSessionProtocol.CookieLength).CopyTo(cookie);
        return true;
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.HelloWithCookie"/>'s body. Fails when
    /// <paramref name="cookie"/> or <paramref name="resumeSecret"/> is not exactly its wire length,
    /// for the same reason <see cref="TryReadCookie"/> does.
    /// </summary>
    public static bool TryReadHelloWithCookie(
        ReadOnlySpan<byte> datagram,
        out uint version,
        out ulong clientNonce,
        out long epoch,
        Span<byte> cookie,
        out uint sessionIdForMe,
        out uint maxMessageSize,
        Span<byte> resumeSecret)
    {
        if (cookie.Length != DatagramSessionProtocol.CookieLength
            || resumeSecret.Length != DatagramSessionProtocol.ResumeSecretLength
            || !TryReadControlType(datagram, out DatagramControlType type)
            || type != DatagramControlType.HelloWithCookie)
        {
            version = default;
            clientNonce = default;
            epoch = default;
            sessionIdForMe = default;
            maxMessageSize = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        version = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]);
        offset += sizeof(ulong);
        epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]));
        offset += sizeof(ulong);
        datagram.Slice(offset, DatagramSessionProtocol.CookieLength).CopyTo(cookie);
        offset += DatagramSessionProtocol.CookieLength;
        sessionIdForMe = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        maxMessageSize = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        datagram.Slice(offset, DatagramSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.Accept"/>'s body. Fails when
    /// <paramref name="resumeSecret"/> is not exactly its wire length, for the same reason
    /// <see cref="TryReadCookie"/> does for a cookie.
    /// </summary>
    public static bool TryReadAccept(
        ReadOnlySpan<byte> datagram,
        out ulong clientNonce,
        out uint sessionIdForMe,
        out uint maxMessageSize,
        Span<byte> resumeSecret)
    {
        if (resumeSecret.Length != DatagramSessionProtocol.ResumeSecretLength
            || !TryReadControlType(datagram, out DatagramControlType type)
            || type != DatagramControlType.Accept)
        {
            clientNonce = default;
            sessionIdForMe = default;
            maxMessageSize = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]);
        offset += sizeof(ulong);
        sessionIdForMe = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        maxMessageSize = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        datagram.Slice(offset, DatagramSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.Refusal"/>'s body. A reason byte that
    /// <see cref="RefusalReasonMap.DisconnectReasonFor"/> does not understand fails the whole read —
    /// an unreadable reason is indistinguishable from a forged Refusal, so nothing about the datagram
    /// can be trusted, not just the reason field.
    /// </summary>
    public static bool TryReadRefusal(
        ReadOnlySpan<byte> datagram,
        out ulong clientNonce,
        out RefusalReason reason,
        out uint versionOffered,
        out uint versionRequired)
    {
        if (!TryReadControlType(datagram, out DatagramControlType type) || type != DatagramControlType.Refusal)
        {
            clientNonce = default;
            reason = default;
            versionOffered = default;
            versionRequired = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        ulong candidateClientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]);
        offset += sizeof(ulong);
        RefusalReason candidateReason = (RefusalReason)datagram[offset];
        offset += sizeof(byte);
        uint candidateVersionOffered = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        uint candidateVersionRequired = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);

        if (RefusalReasonMap.DisconnectReasonFor(candidateReason) is null)
        {
            clientNonce = default;
            reason = default;
            versionOffered = default;
            versionRequired = default;
            return false;
        }

        clientNonce = candidateClientNonce;
        reason = candidateReason;
        versionOffered = candidateVersionOffered;
        versionRequired = candidateVersionRequired;
        return true;
    }
}
