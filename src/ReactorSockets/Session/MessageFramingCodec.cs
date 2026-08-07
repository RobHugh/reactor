using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The single place the stream Session data plane's wire framing is encoded and decoded: every
/// Message on the wire is a <see cref="LengthPrefixSize"/>-byte big-endian <c>u32</c> length prefix
/// followed by exactly that many payload bytes (ADR 0010). Allocation-free by construction — every
/// member works over a <see cref="Span{T}"/> or <see cref="ReadOnlySpan{T}"/> the caller already owns.
/// </summary>
internal static class MessageFramingCodec
{
    /// <summary>The width of the length prefix in bytes.</summary>
    public const int LengthPrefixSize = sizeof(uint);

    /// <summary>
    /// Reads the <c>u32</c> length prefix from the front of <paramref name="buffer"/>.
    /// Returns <see langword="false"/> without touching <paramref name="length"/> when fewer than
    /// <see cref="LengthPrefixSize"/> bytes are available — the prefix itself has not fully arrived
    /// yet, which is an ordinary, expected state while a receive buffer is filling.
    /// </summary>
    public static bool TryReadLengthPrefix(ReadOnlySpan<byte> buffer, out uint length)
    {
        if (buffer.Length < LengthPrefixSize)
        {
            length = 0;
            return false;
        }

        length = BinaryPrimitives.ReadUInt32BigEndian(buffer);
        return true;
    }

    /// <summary>
    /// Writes a Message's <c>u32</c> big-endian length prefix into the front of
    /// <paramref name="destination"/>, which must be at least <see cref="LengthPrefixSize"/> bytes.
    /// </summary>
    public static void WriteLengthPrefix(Span<byte> destination, uint length) =>
        BinaryPrimitives.WriteUInt32BigEndian(destination, length);
}
