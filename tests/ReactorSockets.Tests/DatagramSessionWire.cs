using System.Buffers.Binary;

namespace ReactorSockets.Tests;

/// <summary>
/// Hand-builds datagram Session control datagrams for tests that inject traffic from a raw socket.
/// Every test that crafts a Hello-with-cookie builds it here, so a change to the handshake wire layout
/// is made in <see cref="WriteHelloWithCookieBody"/> and <see cref="WriteTerms"/> alone.
/// </summary>
internal static class DatagramSessionWire
{
    /// <summary>The datagram Session protocol version a well-formed Hello-with-cookie offers.</summary>
    public const uint ProtocolVersion = 1;

    /// <summary>The control type byte of a Hello-with-cookie, at offset 4 of the datagram.</summary>
    public const byte HelloWithCookieType = 3;

    /// <summary>The size of a control datagram, in bytes: the 5-byte header, the body, then zero padding.</summary>
    public const int ControlDatagramSize = 256;

    /// <summary>The offset of a control datagram's body: 4 bytes of session id (zero) and the control type byte.</summary>
    public const int ControlBodyOffset = 5;

    /// <summary>The length of the Cookie a Hello-with-cookie echoes, in bytes.</summary>
    public const int CookieLength = 32;

    /// <summary>The length of the dialler's resume secret, in bytes.</summary>
    public const int ResumeSecretLength = 16;

    /// <summary>The session id the dialler announces unless a test overrides it.</summary>
    public const uint DefaultSessionIdForMe = 0xCAFE_BABE;

    /// <summary>The largest Message the dialler announces it will accept unless a test overrides it.</summary>
    public const uint DefaultMaxMessageSize = 4096;

    /// <summary>The send-idle interval, in milliseconds, the dialler announces unless a test overrides it.</summary>
    public const ulong DefaultSendIdleIntervalMilliseconds = 60_000;

    /// <summary>The Linger, in milliseconds, the dialler announces unless a test overrides it.</summary>
    public const ulong DefaultLingerMilliseconds = 30_000;

    /// <summary>
    /// The length of a Hello-with-cookie body, in bytes: version, nonce, epoch, Cookie, the announced
    /// terms and the resume secret.
    /// </summary>
    public const int HelloWithCookieBodyLength = 4 + 8 + 8 + CookieLength + TermsLength + ResumeSecretLength;

    /// <summary>The length of the announced terms, in bytes: session id, MaxMessageSize, send-idle interval, Linger.</summary>
    public const int TermsLength = 4 + 4 + 8 + 8;

    /// <summary>
    /// Builds a 256-byte Hello-with-cookie datagram. Control datagram offsets: 0 session id (zero),
    /// 4 control type (<see cref="HelloWithCookieType"/>), 5 version (u32), 9 client nonce (u64), 17 epoch (u64),
    /// 25 Cookie (32 bytes), then the terms from offset 57 (see <see cref="WriteTerms"/>) and the resume secret
    /// (16 bytes). The rest is zero padding.
    /// </summary>
    /// <param name="clientNonce">The nonce the dialler's Hello carried.</param>
    /// <param name="epoch">The Cookie epoch the dialler echoes.</param>
    /// <param name="cookie">The Cookie the dialler echoes: <see cref="CookieLength"/> bytes. Longer is cut, shorter is zero-extended.</param>
    /// <param name="sessionIdForMe">The session id the dialler asks to be addressed by.</param>
    /// <param name="maxMessageSize">The largest Message the dialler announces it will accept.</param>
    /// <param name="sendIdleIntervalMilliseconds">The send-idle interval the dialler announces.</param>
    /// <param name="lingerMilliseconds">The Linger the dialler announces.</param>
    /// <param name="resumeSecret">The dialler's resume secret: <see cref="ResumeSecretLength"/> bytes, or empty for all zero.</param>
    /// <param name="version">The protocol version the Hello-with-cookie offers.</param>
    /// <returns>The datagram.</returns>
    public static byte[] BuildHelloWithCookie(
        ulong clientNonce,
        long epoch,
        ReadOnlySpan<byte> cookie,
        uint sessionIdForMe = DefaultSessionIdForMe,
        uint maxMessageSize = DefaultMaxMessageSize,
        ulong sendIdleIntervalMilliseconds = DefaultSendIdleIntervalMilliseconds,
        ulong lingerMilliseconds = DefaultLingerMilliseconds,
        ReadOnlySpan<byte> resumeSecret = default,
        uint version = ProtocolVersion)
    {
        byte[] body = new byte[HelloWithCookieBodyLength];
        WriteHelloWithCookieBody(
            body,
            version,
            clientNonce,
            epoch,
            cookie,
            sessionIdForMe,
            maxMessageSize,
            sendIdleIntervalMilliseconds,
            lingerMilliseconds,
            resumeSecret);
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>
    /// Wraps <paramref name="body"/> in a <see cref="ControlDatagramSize"/>-byte control datagram:
    /// session id zero at offset 0, <paramref name="controlType"/> at offset 4, the body from
    /// <see cref="ControlBodyOffset"/>, then zero padding.
    /// </summary>
    /// <param name="controlType">The control type byte.</param>
    /// <param name="body">The body, at most <c>ControlDatagramSize - ControlBodyOffset</c> bytes.</param>
    /// <returns>The datagram.</returns>
    public static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    /// <summary>
    /// Writes a Hello-with-cookie body into <paramref name="body"/>, which is
    /// <see cref="HelloWithCookieBodyLength"/> bytes. Body offsets: 0 version (u32), 4 client nonce (u64),
    /// 12 epoch (u64), 20 Cookie (32 bytes), 52 terms (see <see cref="WriteTerms"/>), then the resume secret
    /// (16 bytes). The one writer of the Hello-with-cookie body layout.
    /// </summary>
    /// <param name="body">The body to fill.</param>
    /// <param name="version">The protocol version offered.</param>
    /// <param name="clientNonce">The nonce the dialler's Hello carried.</param>
    /// <param name="epoch">The Cookie epoch the dialler echoes.</param>
    /// <param name="cookie">The Cookie the dialler echoes.</param>
    /// <param name="sessionIdForMe">The session id the dialler asks to be addressed by.</param>
    /// <param name="maxMessageSize">The largest Message the dialler announces it will accept.</param>
    /// <param name="sendIdleIntervalMilliseconds">The send-idle interval the dialler announces.</param>
    /// <param name="lingerMilliseconds">The Linger the dialler announces.</param>
    /// <param name="resumeSecret">The dialler's resume secret, or empty for all zero.</param>
    public static void WriteHelloWithCookieBody(
        Span<byte> body,
        uint version,
        ulong clientNonce,
        long epoch,
        ReadOnlySpan<byte> cookie,
        uint sessionIdForMe,
        uint maxMessageSize,
        ulong sendIdleIntervalMilliseconds,
        ulong lingerMilliseconds,
        ReadOnlySpan<byte> resumeSecret)
    {
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.Slice(offset, 4), version);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.Slice(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.Slice(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie[..Math.Min(cookie.Length, CookieLength)].CopyTo(body.Slice(offset, CookieLength));
        offset += CookieLength;
        offset = WriteTerms(body, offset, sessionIdForMe, maxMessageSize, sendIdleIntervalMilliseconds, lingerMilliseconds);
        if (!resumeSecret.IsEmpty)
        {
            resumeSecret.CopyTo(body.Slice(offset, ResumeSecretLength));
        }
    }

    /// <summary>
    /// Writes the announced terms at <paramref name="offset"/> and returns the offset just past them.
    /// Terms offsets: 0 session id for me (u32), 4 MaxMessageSize (u32), 8 send-idle interval in
    /// milliseconds (u64), 16 Linger in milliseconds (u64). <see cref="TermsLength"/> bytes in all.
    /// </summary>
    /// <param name="destination">The buffer to write into.</param>
    /// <param name="offset">Where the terms start.</param>
    /// <param name="sessionIdForMe">The session id the sender asks to be addressed by.</param>
    /// <param name="maxMessageSize">The largest Message the sender announces it will accept.</param>
    /// <param name="sendIdleIntervalMilliseconds">The send-idle interval the sender announces.</param>
    /// <param name="lingerMilliseconds">The Linger the sender announces.</param>
    /// <returns>The offset just past the terms.</returns>
    public static int WriteTerms(
        Span<byte> destination,
        int offset,
        uint sessionIdForMe,
        uint maxMessageSize,
        ulong sendIdleIntervalMilliseconds,
        ulong lingerMilliseconds)
    {
        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(offset, 4), sessionIdForMe);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(offset, 8), sendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(offset, 8), lingerMilliseconds);
        return offset + 8;
    }
}
