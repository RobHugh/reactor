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
    /// The reserved length-prefix value announcing a Goodbye control record rather than a Message
    /// (ADR 0010/issue #23) — no legal Message length can ever equal it, because
    /// <see cref="StreamSessionOptions.MaxMessageSize"/> is a signed <c>int</c> and so is always below
    /// the value this sets, whatever it is configured to. Reusing the same four-byte length-prefix
    /// slot rather than a new tagged record keeps the data plane's wire shape exactly what ADR 0010
    /// already describes — one <c>u32</c> in front of every record — with nothing extra to parse
    /// before knowing which kind a record is. A Goodbye carries no body: the sentinel itself is the
    /// whole record.
    /// </summary>
    public const uint GoodbyeSentinel = uint.MaxValue;

    /// <summary>
    /// The reserved length-prefix value announcing a delivery-counter control record rather than a
    /// Message (ADR 0010, issue #109) — the cumulative <c>u64</c> count of Messages this end has fully
    /// received, sent so the peer can advance retention. Distinct from <see cref="GoodbyeSentinel"/> by
    /// construction (each is its own literal), and, exactly like it, unreachable by any legal Message
    /// length because <see cref="StreamSessionOptions.MaxMessageSize"/> is a signed <c>int</c> and so is
    /// always below the value this sets, whatever it is configured to. Reusing the same four-byte
    /// length-prefix slot keeps the wire shape ADR 0010 already describes — one <c>u32</c> in front of
    /// every record — rather than adding a tag byte read before knowing which kind a record is. Unlike a
    /// Goodbye, this record has a body: <see cref="DeliveryCounterBodySize"/> bytes of counter follow the
    /// sentinel, so a receiver that has only seen the sentinel is not yet holding a whole record.
    /// </summary>
    public const uint DeliveryCounterSentinel = uint.MaxValue - 2;

    /// <summary>The width, in bytes, of a delivery-counter record's <c>u64</c> body.</summary>
    public const int DeliveryCounterBodySize = sizeof(ulong);

    /// <summary>
    /// The total size of a whole delivery-counter record on the wire: the <see cref="LengthPrefixSize"/>-byte
    /// sentinel plus its <see cref="DeliveryCounterBodySize"/>-byte <c>u64</c> body.
    /// </summary>
    public const int DeliveryCounterRecordSize = LengthPrefixSize + DeliveryCounterBodySize;

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

    /// <summary>
    /// Writes the whole of a Goodbye control record — <see cref="GoodbyeSentinel"/> and nothing
    /// else — into the front of <paramref name="destination"/>, which must be at least
    /// <see cref="LengthPrefixSize"/> bytes.
    /// </summary>
    public static void WriteGoodbye(Span<byte> destination) => WriteLengthPrefix(destination, GoodbyeSentinel);

    /// <summary>
    /// Writes the whole of a delivery-counter control record — <see cref="DeliveryCounterSentinel"/>
    /// followed by <paramref name="count"/> as a big-endian <c>u64</c> — into the front of
    /// <paramref name="destination"/>, which must be at least <see cref="DeliveryCounterRecordSize"/>
    /// bytes.
    /// </summary>
    public static void WriteDeliveryCounter(Span<byte> destination, ulong count)
    {
        WriteLengthPrefix(destination, DeliveryCounterSentinel);
        BinaryPrimitives.WriteUInt64BigEndian(destination[LengthPrefixSize..], count);
    }

    /// <summary>
    /// Reads the <c>u64</c> cumulative count from a delivery-counter record's body — the
    /// <see cref="DeliveryCounterBodySize"/> bytes immediately behind the sentinel. Trusts that
    /// <paramref name="body"/> is at least that long rather than re-checking, exactly as
    /// <see cref="MessageFramingLegality.Classify"/>'s <c>Ready</c> outcome leaves Message-body
    /// extraction to its caller: the caller only reaches here once
    /// <see cref="MessageFramingLegality.Classify"/> has reported
    /// <see cref="MessageFramingOutcome.DeliveryCounterReceived"/>, which is the guarantee that the whole
    /// body has already arrived.
    /// </summary>
    public static ulong ReadDeliveryCounter(ReadOnlySpan<byte> body) => BinaryPrimitives.ReadUInt64BigEndian(body);
}
