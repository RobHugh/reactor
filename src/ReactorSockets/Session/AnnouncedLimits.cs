using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The limits each end announces about itself in a handshake, in both Session profiles. A
/// datagram Hello-with-cookie, a datagram Accept and a stream Hello all carry them as one contiguous
/// block of <see cref="WireSize"/> bytes, written by <see cref="Write"/> and read by <see cref="Read"/>,
/// and <see cref="HandshakeLegality.IsLegalAnnouncement"/> judges them. A field added here is encoded,
/// decoded and judged without either codec changing.
/// </summary>
/// <param name="MaxMessageSize">The largest Message the announcing end will accept, in bytes.</param>
/// <param name="SendIdleIntervalMilliseconds">
/// The announcing end's send-idle interval, which the other end needs to compute its receive-idle
/// deadline.
/// </param>
/// <param name="LingerMilliseconds">
/// The announcing end's Linger, which the other end negotiates down to the minimum of the two.
/// </param>
internal readonly record struct AnnouncedLimits(
    uint MaxMessageSize,
    ulong SendIdleIntervalMilliseconds,
    ulong LingerMilliseconds)
{
    /// <summary>The exact number of bytes the limits occupy on the wire.</summary>
    public const int WireSize =
        sizeof(uint) // maxMessageSize
        + sizeof(ulong) // sendIdleIntervalMilliseconds
        + sizeof(ulong); // lingerMilliseconds

    /// <summary>
    /// Writes these limits big-endian at the start of <paramref name="destination"/>, which must hold
    /// at least <see cref="WireSize"/> bytes.
    /// </summary>
    public void Write(Span<byte> destination)
    {
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(destination[offset..], MaxMessageSize);
        offset += sizeof(uint);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], SendIdleIntervalMilliseconds);
        offset += sizeof(ulong);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], LingerMilliseconds);
    }

    /// <summary>
    /// Reads the limits <see cref="Write"/> writes from the start of <paramref name="source"/>, which
    /// must hold at least <see cref="WireSize"/> bytes. Judges nothing: the caller asks
    /// <see cref="HandshakeLegality.IsLegalAnnouncement"/>.
    /// </summary>
    public static AnnouncedLimits Read(ReadOnlySpan<byte> source)
    {
        int offset = 0;
        uint maxMessageSize = BinaryPrimitives.ReadUInt32BigEndian(source[offset..]);
        offset += sizeof(uint);
        ulong sendIdleIntervalMilliseconds = BinaryPrimitives.ReadUInt64BigEndian(source[offset..]);
        offset += sizeof(ulong);
        ulong lingerMilliseconds = BinaryPrimitives.ReadUInt64BigEndian(source[offset..]);
        return new(maxMessageSize, sendIdleIntervalMilliseconds, lingerMilliseconds);
    }
}
