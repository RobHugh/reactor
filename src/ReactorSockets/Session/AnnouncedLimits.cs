using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The limits each end announces about itself in a handshake, in both Session profiles. A
/// datagram Hello-with-cookie, a datagram Accept and a stream Hello all carry them as one contiguous
/// block of <see cref="WireSize"/> bytes, written by <see cref="Write"/>, read by <see cref="Read"/> and
/// judged by <see cref="IsLegal"/>. Both codecs use these three members for the block, so its fields,
/// encoding and legality rule are stated only here.
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
    /// <see cref="IsLegal"/>.
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

    /// <summary>
    /// Whether these limits are a legal announcement. A <see cref="MaxMessageSize"/> of zero is illegal
    /// whatever the version, as it is in the local options. The intervals are judged by
    /// <see cref="HandshakeLegality.IsLegalInterval"/> only when <paramref name="speaksVersion"/> is
    /// <see langword="true"/>: a handshake of a version this end does not speak is left for the version
    /// Refusal, because this end cannot judge the fields of that version.
    /// </summary>
    public bool IsLegal(bool speaksVersion) =>
        MaxMessageSize != 0
        && (!speaksVersion
            || (HandshakeLegality.IsLegalInterval(SendIdleIntervalMilliseconds)
                && HandshakeLegality.IsLegalInterval(LingerMilliseconds)));
}
