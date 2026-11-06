using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The 20-byte header and the 7-bytes-plus-payload packet record shape (ADR 0009), proven from
/// datagrams a real <see cref="WireTap"/> captured off a live
/// dialogue between two Reactors, asserting on byte offsets directly rather than only on
/// <see cref="DatagramDecoder"/>'s decoded output — the decoder is a convenience used here as a
/// cross-check, not the thing under test.
/// </summary>
public class DatagramSessionWireFormatTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int RecordSequenceOffset = 1;
    private const int RecordLengthOffset = 5;

    // Offsets into a 256-byte control datagram's body (ControlBodyOffset = 5), per
    // ControlDatagramCodec.WriteHelloWithCookie / WriteAccept: each carries the session id its
    // sender wants stamped on datagrams sent back to it.
    private const int ControlDatagramSize = 256;
    private const int ControlTypeOffset = 4;
    private const byte ControlTypeHelloWithCookie = 3;
    private const byte ControlTypeAccept = 4;
    private const int HelloWithCookieSessionIdForMeOffset = 5 + 4 + 8 + 8 + 32; // version, nonce, epoch, cookie
    private const int AcceptSessionIdForMeOffset = 5 + 8; // nonce

    // The dialler's send-idle interval (issue #347), immediately after HelloWithCookie's maxMessageSize
    // (itself immediately after sessionIdForMe).
    private const int HelloWithCookieSendIdleIntervalOffset = HelloWithCookieSessionIdForMeOffset + 4 + 4;

    // The accepter's send-idle interval (issue #348), immediately after Accept's maxMessageSize
    // (itself immediately after sessionIdForMe).
    private const int AcceptSendIdleIntervalOffset = AcceptSessionIdForMeOffset + 4 + 4;

    // The accepter's Linger (issue #362), immediately after Accept's send-idle interval.
    private const int AcceptLingerOffset = AcceptSendIdleIntervalOffset + 8;

    // The dialler's Linger (issue #361), immediately after HelloWithCookie's send-idle interval.
    private const int HelloWithCookieLingerOffset = HelloWithCookieSendIdleIntervalOffset + 8;

    [Fact]
    public void Send_LiveSessionDatagram_FirstTwentyBytesAreTheHeaderSessionIdThenBothAckBlocks()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — one Send from the dialler, flushed as one data Datagram to the accepter.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1, 2, 3]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] datagram = FirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — the first 20 bytes, read directly off the captured bytes at their documented
        // offsets, are the session id followed by both ack blocks (ADR 0009). Cross-checked
        // against the independent decoder, but the byte positions themselves are asserted first.
        datagram.Length.Should().BeGreaterThanOrEqualTo(DatagramHeaderSize, "a data Datagram always carries the full header");

        uint sessionFromOffsetZero = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4));
        uint reliableOrderedAckBase = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(4, 4));
        uint reliableOrderedAckBits = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(8, 4));
        uint reliableUnorderedAckBase = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(12, 4));
        uint reliableUnorderedAckBits = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(16, 4));

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        decoded.IsSuccess.Should().BeTrue();
        sessionFromOffsetZero.Should().Be(decoded.Datagram!.Session, "bytes [0,4) are the session id the decoder also reads from there");
        reliableOrderedAckBase.Should().Be(decoded.Datagram!.ReliableOrderedAck.Base, "bytes [4,8) are the ReliableOrdered ack base");
        reliableOrderedAckBits.Should().Be(decoded.Datagram!.ReliableOrderedAck.Bits, "bytes [8,12) are the ReliableOrdered ack bits");
        reliableUnorderedAckBase.Should().Be(decoded.Datagram!.ReliableUnorderedAck.Base, "bytes [12,16) are the ReliableUnordered ack base");
        reliableUnorderedAckBits.Should().Be(decoded.Datagram!.ReliableUnorderedAck.Bits, "bytes [16,20) are the ReliableUnordered ack bits");

        // And byte 20 onward is the first record's own flags byte, not more header — proven
        // directly by the total length equalling exactly the header plus one 7-plus-3-byte record.
        datagram.Length.Should().Be(DatagramHeaderSize + UnfragmentedRecordSize + 3, "one 3-byte-payload record follows the 20-byte header with nothing between");
    }

    [Fact]
    public void Send_LiveSessionDatagram_OneRecordOccupiesExactlySevenBytesPlusItsPayloadLength()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] message = [10, 20, 30, 40, 50];

        // Act
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, message);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] datagram = FirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — read the record's fixed fields directly off the captured bytes at the header's
        // end: the datagram's total length must equal the header plus exactly 7 bytes plus the
        // payload, with the length field agreeing and the payload bytes landing exactly where the
        // fixed fields say they do.
        int recordOffset = DatagramHeaderSize;
        byte flags = datagram[recordOffset];
        uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(recordOffset + RecordSequenceOffset, 4));
        ushort declaredLength = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(recordOffset + RecordLengthOffset, 2));

        declaredLength.Should().Be((ushort)message.Length, "the record's length field names its own payload's length");
        datagram.Length.Should().Be(
            DatagramHeaderSize + UnfragmentedRecordSize + message.Length,
            "the whole Datagram is exactly the header plus this one record's 7 fixed bytes plus its payload — nothing more");

        byte[] payloadBytes = datagram.AsSpan(recordOffset + UnfragmentedRecordSize, message.Length).ToArray();
        payloadBytes.Should().Equal(message, "the payload starts exactly 7 bytes after the record's own start");

        (flags & 0b0000_0011).Should().Be((byte)0b0000_0010, "ReliableUnordered's wire mode bits");
        sequence.Should().Be(0u, "the first sequence allocated in this Space");
    }

    [Fact]
    public void Send_LiveSessionDatagram_SessionIdIsTheOneTheFarEndAskedToBeStampedOnDatagramsSentToIt()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // The handshake's own control datagrams each carry the session id their sender is asking
        // to be stamped on datagrams sent back to it (ControlDatagramCodec.WriteHelloWithCookie /
        // WriteAccept) — read straight off the captured bytes, independent of any internal type.
        byte[] helloWithCookie = tap.Captures.Single(
            c => c.Direction == TapDirection.DiallerToAccepter
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeHelloWithCookie).Payload;
        byte[] accept = tap.Captures.Single(
            c => c.Direction == TapDirection.AccepterToDialler
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeAccept).Payload;

        uint sessionIdTheDiallerAskedFor = BinaryPrimitives.ReadUInt32BigEndian(
            helloWithCookie.AsSpan(HelloWithCookieSessionIdForMeOffset, 4));
        uint sessionIdTheAccepterAskedFor = BinaryPrimitives.ReadUInt32BigEndian(
            accept.AsSpan(AcceptSessionIdForMeOffset, 4));

        // Act — a Send in each direction, so both Peers flush a data Datagram to the wire.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1]);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, [2]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] toAccepter = FirstDataDatagram(tap, TapDirection.DiallerToAccepter);
        byte[] toDialler = FirstDataDatagram(tap, TapDirection.AccepterToDialler);

        uint sessionIdStampedOnDatagramsToAccepter = BinaryPrimitives.ReadUInt32BigEndian(toAccepter.AsSpan(0, 4));
        uint sessionIdStampedOnDatagramsToDialler = BinaryPrimitives.ReadUInt32BigEndian(toDialler.AsSpan(0, 4));

        // Assert
        sessionIdStampedOnDatagramsToAccepter.Should().Be(
            sessionIdTheAccepterAskedFor, "a datagram sent to the accepter must carry the session id the accepter's own Accept asked for");
        sessionIdStampedOnDatagramsToDialler.Should().Be(
            sessionIdTheDiallerAskedFor, "a datagram sent to the dialler must carry the session id the dialler's own HelloWithCookie asked for");
    }

    [Fact]
    public void Send_LiveSessionDatagram_SeveralRecordsAccountForEveryByteWithNoPaddingBetweenThem()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[][] messages = [[1], [2, 2], [3, 3, 3], []];

        // Act — several Sends queued before one Pump coalesce into one Datagram, in Send order
        // (issue #216).
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, messages[0]);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.UnreliableSequenced, messages[1]);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, messages[2]);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, messages[3]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] datagram = FirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — walk the bytes after the header by hand, record by record: each step consumes
        // exactly 7 bytes plus that record's own declared payload length, and the walk lands
        // exactly on the end of the Datagram with nothing left over and nothing skipped, which is
        // what "no padding between records" and "accounts for every byte" mean at the byte level.
        int offset = DatagramHeaderSize;
        List<byte[]> payloadsWalked = [];
        while (offset < datagram.Length)
        {
            (offset + UnfragmentedRecordSize).Should().BeLessThanOrEqualTo(
                datagram.Length, "a record's 7 fixed bytes must fit in what remains — no partial record at the end");

            ushort declaredLength = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + RecordLengthOffset, 2));
            int payloadOffset = offset + UnfragmentedRecordSize;

            (payloadOffset + declaredLength).Should().BeLessThanOrEqualTo(
                datagram.Length, "a record's declared payload must fit in what remains — no record claiming bytes past the Datagram's end");

            payloadsWalked.Add(datagram.AsSpan(payloadOffset, declaredLength).ToArray());
            offset = payloadOffset + declaredLength;
        }

        offset.Should().Be(datagram.Length, "the walk consumes every byte after the header exactly once, with no gap and no leftover");
        payloadsWalked.Should().HaveCount(messages.Length, "every queued Send packed into this one coalesced Datagram");
        payloadsWalked.Should().Equal(messages, (walked, sent) => walked.SequenceEqual(sent), "each record's payload, walked in wire order, matches what was Sent in that order");
    }

    [Fact]
    public void Connect_HelloWithCookie_CarriesTheDiallersConfiguredSendIdleIntervalInMilliseconds()
    {
        // Arrange — a dialler configured with a distinctive, easily-misread send-idle interval, so
        // this proves the actual configured value made it to the wire rather than some other field's
        // value that happens to look right.
        TimeSpan configuredSendIdleInterval = TimeSpan.FromMilliseconds(12_345);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, sendIdleInterval: configuredSendIdleInterval);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — read the interval straight off the captured Hello-with-cookie's own bytes.
        byte[] helloWithCookie = tap.Captures.Single(
            c => c.Direction == TapDirection.DiallerToAccepter
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeHelloWithCookie).Payload;
        ulong sendIdleIntervalMillisecondsOnWire = BinaryPrimitives.ReadUInt64BigEndian(
            helloWithCookie.AsSpan(HelloWithCookieSendIdleIntervalOffset, 8));

        // Assert
        sendIdleIntervalMillisecondsOnWire.Should().Be((ulong)configuredSendIdleInterval.TotalMilliseconds);
    }

    [Fact]
    public void Connect_Accept_CarriesTheAccepterConfiguredSendIdleIntervalInMilliseconds()
    {
        // Arrange — an accepter configured with a distinctive, easily-misread send-idle interval, so
        // this proves the actual configured value made it to the wire rather than some other field's
        // value that happens to look right.
        TimeSpan configuredSendIdleInterval = TimeSpan.FromMilliseconds(54_321);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new(sendIdleInterval: configuredSendIdleInterval);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — read the interval straight off the captured Accept's own bytes.
        byte[] accept = tap.Captures.Single(
            c => c.Direction == TapDirection.AccepterToDialler
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeAccept).Payload;
        ulong sendIdleIntervalMillisecondsOnWire = BinaryPrimitives.ReadUInt64BigEndian(
            accept.AsSpan(AcceptSendIdleIntervalOffset, 8));

        // Assert
        sendIdleIntervalMillisecondsOnWire.Should().Be((ulong)configuredSendIdleInterval.TotalMilliseconds);
    }

    [Fact]
    public void Connect_Accept_CarriesTheAccepterConfiguredLingerInMilliseconds()
    {
        // Arrange — an accepter configured with a distinctive, easily-misread Linger, so this proves
        // the actual configured value made it to the wire rather than some other field's value that
        // happens to look right (issue #362).
        TimeSpan configuredLinger = TimeSpan.FromMilliseconds(98_765);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new(linger: configuredLinger);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — read the Linger straight off the captured Accept's own bytes.
        byte[] accept = tap.Captures.Single(
            c => c.Direction == TapDirection.AccepterToDialler
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeAccept).Payload;
        ulong lingerMillisecondsOnWire = BinaryPrimitives.ReadUInt64BigEndian(
            accept.AsSpan(AcceptLingerOffset, 8));

        // Assert
        lingerMillisecondsOnWire.Should().Be((ulong)configuredLinger.TotalMilliseconds);
    }

    [Fact]
    public void Connect_HelloWithCookie_CarriesTheDiallersConfiguredLingerInMilliseconds()
    {
        // Arrange — a dialler configured with a distinctive, easily-misread Linger, so this proves the
        // actual configured value made it to the wire rather than some other field's value that
        // happens to look right (issue #361).
        TimeSpan configuredLinger = TimeSpan.FromMilliseconds(98_765);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: configuredLinger);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — read the Linger straight off the captured Hello-with-cookie's own bytes.
        byte[] helloWithCookie = tap.Captures.Single(
            c => c.Direction == TapDirection.DiallerToAccepter
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeHelloWithCookie).Payload;
        ulong lingerMillisecondsOnWire = BinaryPrimitives.ReadUInt64BigEndian(
            helloWithCookie.AsSpan(HelloWithCookieLingerOffset, 8));

        // Assert
        lingerMillisecondsOnWire.Should().Be((ulong)configuredLinger.TotalMilliseconds);
    }

    /// <summary>The first datagram captured travelling in <paramref name="direction"/> whose session id names a live Session rather than the reserved control session id — filters out the handshake's own control datagrams.</summary>
    private static byte[] FirstDataDatagram(WireTap tap, TapDirection direction) =>
        tap.PayloadsInDirection(direction).First(payload => BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(0, 4)) != 0);
}
