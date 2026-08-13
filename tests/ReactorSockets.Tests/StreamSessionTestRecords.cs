using System.Buffers.Binary;

namespace ReactorSockets.Tests;

/// <summary>
/// The stream Session profile's wire-record builders and handshake-pumping loop, shared by every test
/// file that hand-builds a Hello/Refusal record or frames a Message over a raw counterparty socket
/// (issue #102). Copy-pasted near-verbatim across 11 files before this existed — this is the one place
/// that shape is stated, mirroring how <see cref="MessageFramingLegality"/> states the framing table
/// once in production code rather than a branch at a time. Deliberately not shared with the datagram
/// profile's own <c>BuildHello</c>-shaped helpers: the two profiles' Hello records are a different wire
/// format entirely (ADR 0008), so a file testing the datagram profile keeps its own.
/// </summary>
internal static class StreamSessionTestRecords
{
    /// <summary>1 type byte + 40-byte Hello payload. A Refusal is padded to the same size.</summary>
    public const int HandshakeRecordSize = 41;

    /// <summary>The wire type byte a Hello record's first byte carries.</summary>
    public const byte HelloRecordType = 1;

    /// <summary>The protocol version this test suite's hand-built Hello records offer by default.</summary>
    public const uint ProtocolVersion = 1;

    /// <summary>Builds a <see cref="HandshakeRecordSize"/>-byte Hello record. The trailing 16-byte resume secret is left zeroed.</summary>
    public static byte[] BuildHello(uint version = ProtocolVersion, uint maxMessageSize = 4096, ulong sendIdleMs = 1000, ulong lingerMs = 1000)
    {
        byte[] record = new byte[HandshakeRecordSize];
        record[0] = HelloRecordType;
        int offset = 1;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), version);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), sendIdleMs);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), lingerMs);
        return record;
    }

    /// <summary>Frames <paramref name="payload"/> behind its 4-byte big-endian length prefix.</summary>
    public static byte[] BuildFrame(byte[] payload)
    {
        byte[] frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)payload.Length);
        payload.CopyTo(frame.AsSpan(4));
        return frame;
    }

    /// <summary>Builds a bare 4-byte big-endian length prefix, with no Message body behind it.</summary>
    public static byte[] BuildPrefix(uint length)
    {
        byte[] prefix = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(prefix, length);
        return prefix;
    }

    /// <summary>
    /// The reserved length-prefix value announcing a delivery-counter control record rather than a
    /// Message (ADR 0010, issue #109). Hand-rolled here rather than reaching into the internal
    /// <c>MessageFramingCodec</c> that owns this value in production — the same reason
    /// <see cref="BuildFrame"/> and <see cref="BuildPrefix"/> hand-roll the ordinary length prefix
    /// instead of calling it: L0/L1 are untested-through (tests/CLAUDE.md, ADR 0015), so a hostile or
    /// merely hand-built counterparty's bytes are constructed independently of the production encoder
    /// they are meant to probe.
    /// </summary>
    public const uint DeliveryCounterSentinel = uint.MaxValue - 2;

    /// <summary>
    /// Builds a whole delivery-counter control record: the 4-byte <see cref="DeliveryCounterSentinel"/>
    /// prefix followed by <paramref name="count"/> as an 8-byte big-endian <c>u64</c> body.
    /// </summary>
    public static byte[] BuildDeliveryCounterRecord(ulong count)
    {
        byte[] record = new byte[12];
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(0, 4), DeliveryCounterSentinel);
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(4, 8), count);
        return record;
    }

    /// <summary>
    /// Decodes a whole 12-byte delivery-counter record read off the wire — the counterpart to
    /// <see cref="BuildDeliveryCounterRecord"/>, for tests reading a record <see cref="StreamSessionReactor"/>
    /// itself emitted (issue #116) rather than building one to send. Hand-rolled independently of the
    /// internal <c>MessageFramingCodec</c> that owns this encoding in production, for the same L0/L1
    /// untested-through reason every other builder in this file is. Returns <see langword="false"/>
    /// without touching <paramref name="count"/> if <paramref name="record"/> is not exactly 12 bytes or
    /// its first four do not carry <see cref="DeliveryCounterSentinel"/>.
    /// </summary>
    public static bool TryReadDeliveryCounterRecord(byte[] record, out ulong count)
    {
        if (record.Length != 12 || BinaryPrimitives.ReadUInt32BigEndian(record.AsSpan(0, 4)) != DeliveryCounterSentinel)
        {
            count = 0;
            return false;
        }

        count = BinaryPrimitives.ReadUInt64BigEndian(record.AsSpan(4, 8));
        return true;
    }

    /// <summary>Builds a payload of <paramref name="length"/> bytes, every byte set to <paramref name="marker"/>.</summary>
    public static byte[] PayloadOf(int length, byte marker)
    {
        byte[] payload = new byte[length];
        Array.Fill(payload, marker);
        return payload;
    }

    /// <summary>
    /// Sends a default Hello via <paramref name="sendHello"/> and pumps <paramref name="accepter"/>
    /// until it has drained a <see cref="SessionEventKind.PeerConnected"/> event, returning the
    /// established Peer's id. Callers whose raw counterparty peer also needs to drain the accepter's
    /// own Hello reply off the wire do so themselves right after this returns — what that reply's
    /// bytes are read into varies by file, so it is not this method's job.
    /// </summary>
    public static PeerId EstablishSession(StreamSessionReactorHarness accepter, Action<byte[]> sendHello, int maxPumps = 20)
    {
        sendHello(BuildHello());
        for (int i = 0; i < maxPumps; i++)
        {
            accepter.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload) evt in accepter.DrainToListWithPayload())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    return evt.Peer;
                }
            }
        }

        throw new InvalidOperationException("The Session never established within the Pump budget.");
    }
}
