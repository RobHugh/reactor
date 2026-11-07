using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The single place the pre-Session control datagram wire format (ADR 0009) is encoded and decoded.
/// Every other piece of the handshake reasons about a <see cref="DatagramControlType.Hello"/>, a <see cref="DatagramControlType.Cookie"/> and
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
    /// echoed, carrying everything a Session needs to exist now that the round trip is paid, including
    /// the dialler's own <paramref name="terms"/>.
    /// </summary>
    public static void WriteHelloWithCookie(
        Span<byte> destination,
        uint version,
        ulong clientNonce,
        long epoch,
        ReadOnlySpan<byte> cookie,
        AnnouncedSessionTerms terms,
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
        offset = WriteTerms(destination, offset, terms);
        resumeSecret.CopyTo(destination.Slice(offset, DatagramSessionProtocol.ResumeSecretLength));
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.Accept"/>: the accepting end's agreement, carrying its
    /// own <paramref name="terms"/>, the same terms a Hello-with-cookie carries for the dialler.
    /// </summary>
    public static void WriteAccept(
        Span<byte> destination,
        ulong clientNonce,
        AnnouncedSessionTerms terms,
        ReadOnlySpan<byte> resumeSecret)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(resumeSecret, DatagramSessionProtocol.ResumeSecretLength, nameof(resumeSecret));
        WriteHeader(destination, DatagramControlType.Accept);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
        offset += sizeof(ulong);
        offset = WriteTerms(destination, offset, terms);
        resumeSecret.CopyTo(destination.Slice(offset, DatagramSessionProtocol.ResumeSecretLength));
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.Resume"/>: a dialler's request to take over the Session
    /// the receiver knows as <paramref name="targetSessionId"/>. That id is the receiver's own local
    /// session id and is the only field that routes the datagram to a Session. The
    /// <paramref name="resumeSecret"/> is the only field that authorises the takeover and is never used
    /// to route. The session id is not an authenticator (ADR 0009), so neither field stands in for the other.
    /// </summary>
    public static void WriteResume(Span<byte> destination, uint targetSessionId, ReadOnlySpan<byte> resumeSecret)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(resumeSecret, DatagramSessionProtocol.ResumeSecretLength, nameof(resumeSecret));
        WriteHeader(destination, DatagramControlType.Resume);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], targetSessionId);
        offset += sizeof(uint);
        resumeSecret.CopyTo(destination.Slice(offset, DatagramSessionProtocol.ResumeSecretLength));
    }

    /// <summary>
    /// Writes a <see cref="DatagramControlType.PathChallenge"/> or a <see cref="DatagramControlType.PathResponse"/>
    /// (ADR 0021). The two share a body: <paramref name="targetSessionId"/>, the receiver's own local
    /// session id, which routes the datagram, then <paramref name="nonce"/>.
    /// </summary>
    public static void WritePathProbe(Span<byte> destination, DatagramControlType type, uint targetSessionId, ReadOnlySpan<byte> nonce)
    {
        ValidateDestinationSize(destination);
        ValidateFixedLength(nonce, DatagramSessionProtocol.PathNonceLength, nameof(nonce));
        if (type is not (DatagramControlType.PathChallenge or DatagramControlType.PathResponse))
        {
            throw new ArgumentOutOfRangeException(nameof(type), type, "Only a PathChallenge or a PathResponse carries a path nonce.");
        }

        WriteHeader(destination, type);

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], targetSessionId);
        offset += sizeof(uint);
        nonce.CopyTo(destination.Slice(offset, DatagramSessionProtocol.PathNonceLength));
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
    /// for the same reason <see cref="TryReadCookie"/> does, and when the announced terms are not
    /// <see cref="AnnouncedLimits.IsLegal">legal</see> for the version the record announces.
    /// </summary>
    public static bool TryReadHelloWithCookie(
        ReadOnlySpan<byte> datagram,
        out uint version,
        out ulong clientNonce,
        out long epoch,
        Span<byte> cookie,
        out AnnouncedSessionTerms terms,
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
            terms = default;
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
        terms = ReadTerms(datagram, ref offset);

        if (!terms.Limits.IsLegal(speaksVersion: version == DatagramSessionProtocol.Version))
        {
            version = default;
            clientNonce = default;
            epoch = default;
            terms = default;
            return false;
        }

        datagram.Slice(offset, DatagramSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.Accept"/>'s body. Fails when
    /// <paramref name="resumeSecret"/> is not exactly its wire length, for the same reason
    /// <see cref="TryReadCookie"/> does for a cookie, and when the announced terms are not
    /// <see cref="AnnouncedLimits.IsLegal">legal</see>. An Accept carries no version: only
    /// an end that speaks this end's version sends one, so its intervals are always judged.
    /// </summary>
    public static bool TryReadAccept(
        ReadOnlySpan<byte> datagram,
        out ulong clientNonce,
        out AnnouncedSessionTerms terms,
        Span<byte> resumeSecret)
    {
        if (resumeSecret.Length != DatagramSessionProtocol.ResumeSecretLength
            || !TryReadControlType(datagram, out DatagramControlType type)
            || type != DatagramControlType.Accept)
        {
            clientNonce = default;
            terms = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram[offset..]);
        offset += sizeof(ulong);
        terms = ReadTerms(datagram, ref offset);

        if (!terms.Limits.IsLegal(speaksVersion: true))
        {
            clientNonce = default;
            terms = default;
            return false;
        }

        datagram.Slice(offset, DatagramSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }

    /// <summary>
    /// Writes <paramref name="terms"/> at <paramref name="offset"/> in wire order and returns the
    /// offset just past them. The one layout both a Hello-with-cookie and an Accept use.
    /// </summary>
    private static int WriteTerms(Span<byte> destination, int offset, AnnouncedSessionTerms terms)
    {
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], terms.SessionIdForMe);
        offset += sizeof(uint);
        terms.Limits.Write(destination[offset..]);
        offset += AnnouncedLimits.WireSize;
        return offset;
    }

    /// <summary>
    /// Reads the terms <see cref="WriteTerms"/> writes, starting at <paramref name="offset"/> and
    /// advancing it past them. Judges nothing: each caller applies
    /// <see cref="AnnouncedLimits.IsLegal"/> to the limits.
    /// <see cref="AnnouncedSessionTerms.SessionIdForMe"/> is never judged: every value is a usable
    /// session id.
    /// </summary>
    private static AnnouncedSessionTerms ReadTerms(ReadOnlySpan<byte> datagram, ref int offset)
    {
        uint sessionIdForMe = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        AnnouncedLimits limits = AnnouncedLimits.Read(datagram[offset..]);
        offset += AnnouncedLimits.WireSize;
        return new(sessionIdForMe, limits);
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.Resume"/>'s body. <paramref name="targetSessionId"/> routes
    /// the datagram to a Session and <paramref name="resumeSecret"/> authorises the takeover, and
    /// neither is used for the other's job (ADR 0009). Fails when <paramref name="resumeSecret"/> is
    /// not exactly <see cref="DatagramSessionProtocol.ResumeSecretLength"/> bytes, and, through
    /// <see cref="TryReadControlType"/>, on any datagram whose length is not the control size or whose
    /// bytes past the 20-byte body are not zero, so a body one byte long is never read. A body one byte
    /// short, or empty, cannot be told apart: zero padding makes it the same bytes as a full body whose
    /// trailing bytes are zero, and it reads as one.
    /// </summary>
    public static bool TryReadResume(ReadOnlySpan<byte> datagram, out uint targetSessionId, Span<byte> resumeSecret)
    {
        if (resumeSecret.Length != DatagramSessionProtocol.ResumeSecretLength
            || !TryReadControlType(datagram, out DatagramControlType type)
            || type != DatagramControlType.Resume)
        {
            targetSessionId = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        targetSessionId = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        datagram.Slice(offset, DatagramSessionProtocol.ResumeSecretLength).CopyTo(resumeSecret);
        return true;
    }

    /// <summary>
    /// Reads the body of a <see cref="DatagramControlType.PathChallenge"/> or a
    /// <see cref="DatagramControlType.PathResponse"/>, whichever <paramref name="type"/> names (ADR 0021).
    /// Fails on a datagram of any other type, and when <paramref name="nonce"/> is not exactly
    /// <see cref="DatagramSessionProtocol.PathNonceLength"/> bytes.
    /// </summary>
    public static bool TryReadPathProbe(ReadOnlySpan<byte> datagram, DatagramControlType type, out uint targetSessionId, Span<byte> nonce)
    {
        if (type is not (DatagramControlType.PathChallenge or DatagramControlType.PathResponse)
            || nonce.Length != DatagramSessionProtocol.PathNonceLength
            || !TryReadControlType(datagram, out DatagramControlType read)
            || read != type)
        {
            targetSessionId = default;
            return false;
        }

        int offset = DatagramSessionProtocol.ControlBodyOffset;
        targetSessionId = BinaryPrimitives.ReadUInt32BigEndian(datagram[offset..]);
        offset += sizeof(uint);
        datagram.Slice(offset, DatagramSessionProtocol.PathNonceLength).CopyTo(nonce);
        return true;
    }

    /// <summary>
    /// Reads a <see cref="DatagramControlType.Refusal"/>'s body. A reason byte that is not a
    /// <see cref="HandshakeLegality.IsLegalRefusalReason">legal reason</see> fails the whole read, so
    /// nothing about the datagram is trusted, not just the reason field.
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

        if (!HandshakeLegality.IsLegalRefusalReason(candidateReason))
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
