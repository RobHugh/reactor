using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Fragment size is decided once, at <see cref="DatagramSessionReactor.Send"/>, captured
/// per-Peer at admission rather than read afresh from <see cref="DatagramSessionOptions"/> on every
/// call. It is announced in the handshake (ADR 0024). Every test here drives a live Session through
/// <see cref="WireTap"/> and decodes the captured bytes by hand, the same proof style
/// <see cref="DatagramSessionSendFragmentationTests"/> uses.
/// </summary>
public class DatagramSessionFragmentSizeIsFixedAtSendTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;

    /// <summary>The first record's declared length, i.e. the size the first Fragment of a data datagram was cut to.</summary>
    private static ushort FirstRecordLength(byte[] datagram) =>
        BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(DatagramHeaderSize + 5, 2));

    /// <summary>How many packet records one datagram carries, walking the wire by hand as <see cref="DatagramSessionSendFragmentationTests"/> does.</summary>
    private static int CountRecords(byte[] datagram)
    {
        int count = 0;
        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;
            int headerSize = isFragment ? FragmentRecordSize : UnfragmentedRecordSize;
            offset += headerSize + length;
            count++;
        }

        return count;
    }

    /// <summary>The most recent datagram travelling in <paramref name="direction"/> whose session id names a live Session rather than the reserved control session id.</summary>
    private static byte[] LastDataDatagram(WireTap tap, TapDirection direction) =>
        tap.PayloadsInDirection(direction).Last(payload => BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(0, 4)) != 0);

    [Fact]
    public void Send_TwoMessagesToTheSamePeerAtDifferentTimes_AreBothCutWithThatPeersOwnFragmentSize()
    {
        // Arrange
        const int fragmentSize = 600;
        const int maxMessageSize = 2000;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: fragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: fragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — one Message, a Pump to flush it, a jump in the clock standing in for "a later call"
        // (kept well under this Session's one-second Linger, so nothing here is a retransmission),
        // then a second, differently sized Message to the same Peer. Nothing between the two Sends
        // could plausibly resolve a fragment size afresh: not this Peer's own Slot, since nothing wrote
        // FragmentSize a second time; not options, since Send no longer reads them for this.
        byte[] firstMessage = Enumerable.Range(0, 1000).Select(i => (byte)i).ToArray();
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, firstMessage).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        byte[] firstDatagram = LastDataDatagram(tap, TapDirection.DiallerToAccepter);

        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));

        byte[] secondMessage = Enumerable.Range(0, 1900).Select(i => (byte)(i % 256)).ToArray();
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, secondMessage).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        byte[] secondDatagram = LastDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — both Messages are wider than one Fragment, so each cut into more than one piece, and
        // the first piece of each is exactly fragmentSize: the same value, both times.
        CountRecords(firstDatagram).Should().Be((1000 + fragmentSize - 1) / fragmentSize);
        CountRecords(secondDatagram).Should().Be((1900 + fragmentSize - 1) / fragmentSize);
        FirstRecordLength(firstDatagram).Should().Be((ushort)fragmentSize, "the first Send cut its Message to this Peer's fragment size");
        FirstRecordLength(secondDatagram).Should().Be(
            FirstRecordLength(firstDatagram), "a later Send to the same Peer cuts with the same fragment size as the first — it was captured once, not re-resolved");
    }

    [Fact]
    public void Connect_HandshakeDatagrams_EachCarryTheirOwnFragmentSizeAndReceiveWindowAtTheDocumentedOffsets()
    {
        // Arrange — each end gets a distinct pair, so a value read from the wrong datagram, or the two
        // fields written in the wrong order, reads back as a different number (ADR 0024).
        const int diallerFragmentSize = 733;
        const int diallerReceiveWindow = 17;
        const int accepterFragmentSize = 911;
        const int accepterReceiveWindow = 23;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: diallerFragmentSize, receiveWindowFragments: diallerReceiveWindow);
        using DatagramSessionReactorHarness accepter = new(
            fragmentSize: accepterFragmentSize, receiveWindowFragments: accepterReceiveWindow);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);

        // Act — connect only, so the only datagrams the tap relays are the handshake's.
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Assert — the terms start at a fixed offset in each datagram, with FragmentSize (u16) at 24 and
        // ReceiveWindowFragments (u32) at 26 inside them, big-endian.
        byte[] helloWithCookie = tap.Captures
            .Single(capture => capture.Direction == TapDirection.DiallerToAccepter && capture.Payload[4] == DatagramSessionWire.HelloWithCookieType)
            .Payload;
        byte[] accept = tap.Captures
            .Single(capture => capture.Direction == TapDirection.AccepterToDialler && capture.Payload[4] == DatagramSessionWire.AcceptType)
            .Payload;

        ReadAnnouncedFragmentSize(helloWithCookie, DatagramSessionWire.HelloWithCookieTermsOffset).Should().Be(diallerFragmentSize);
        ReadAnnouncedReceiveWindow(helloWithCookie, DatagramSessionWire.HelloWithCookieTermsOffset).Should().Be(diallerReceiveWindow);
        ReadAnnouncedFragmentSize(accept, DatagramSessionWire.AcceptTermsOffset).Should().Be(accepterFragmentSize);
        ReadAnnouncedReceiveWindow(accept, DatagramSessionWire.AcceptTermsOffset).Should().Be(accepterReceiveWindow);
    }

    /// <summary>The Fragment size a handshake datagram announces, read at `termsOffset` plus the documented offset inside the terms.</summary>
    private static ushort ReadAnnouncedFragmentSize(byte[] datagram, int termsOffset) =>
        BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(termsOffset + DatagramSessionWire.TermsFragmentSizeOffset, sizeof(ushort)));

    /// <summary>The receive window a handshake datagram announces, read at `termsOffset` plus the documented offset inside the terms.</summary>
    private static uint ReadAnnouncedReceiveWindow(byte[] datagram, int termsOffset) =>
        BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(termsOffset + DatagramSessionWire.TermsReceiveWindowOffset, sizeof(uint)));

    [Fact(Skip = "Replaced by #498")]
    public void Send_TwoSessionsWithDifferentFragmentSizes_InteroperateEachCuttingByItsOwnValue()
    {
        // Arrange — the dialler and the accepter each configure their own, different fragment size.
        // Fragment size is unilateral and per-direction (ADR 0004): the two need not, and here do not,
        // agree.
        const int diallerFragmentSize = 600;
        const int accepterFragmentSize = 900;
        const int maxMessageSize = 2400;
        // Capacity 1: the frame arena's per-Peer share is ArenaSize / Capacity, and the default
        // ArenaSize floor is one MaxMessageSize — with the harness's usual capacity of 4 that share
        // would hold only a quarter of one Message, well under what either Send below needs to reach
        // the consumer whole. One Peer is all this test ever admits.
        using DatagramSessionReactorHarness dialler = new(
            capacity: 1, bindLoopback: false, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: diallerFragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 1, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: accepterFragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(
            dialler, tap, accepter, diallerEvents, accepterEvents);

        // Act — the dialler Sends a Message wider than its own fragment size, cut by its own value.
        // Payloads are drained immediately once delivered, rather than after both exchanges: a Pump's
        // Frame does not outlive the next Pump (issue #219), so a later round would silently discard an
        // event drained too late.
        byte[] fromDialler = Enumerable.Range(0, 1500).Select(i => (byte)i).ToArray();
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, fromDialler).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        byte[] diallerToAccepterDatagram = LastDataDatagram(tap, TapDirection.DiallerToAccepter);
        List<byte[]> accepterPayloads = accepter.DrainPayloads();

        // Act — the accepter Sends a differently sized Message back, cut by its own, different value.
        byte[] fromAccepter = Enumerable.Range(0, 2300).Select(i => (byte)(i % 256)).ToArray();
        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, fromAccepter).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        byte[] accepterToDiallerDatagram = LastDataDatagram(tap, TapDirection.AccepterToDialler);

        // The accepter's reply left within the round above, after this round's own dialler.Pump()
        // already ran, so the dialler needs one further round to receive and parse it.
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        List<byte[]> diallerPayloads = dialler.DrainPayloads();

        // Assert — each direction was cut by its own sender's fragment size, not the other's or a
        // shared one.
        CountRecords(diallerToAccepterDatagram).Should().Be((1500 + diallerFragmentSize - 1) / diallerFragmentSize);
        FirstRecordLength(diallerToAccepterDatagram).Should().Be((ushort)diallerFragmentSize);

        CountRecords(accepterToDiallerDatagram).Should().Be((2300 + accepterFragmentSize - 1) / accepterFragmentSize);
        FirstRecordLength(accepterToDiallerDatagram).Should().Be((ushort)accepterFragmentSize);

        // Assert — every Fragment admits on the wire despite the two Sessions never agreeing on a
        // fragment size, whatever either side does with it once admitted. Nothing here required the
        // receiver to learn the sender's fragment size (ADR 0004) — interoperation held on the wire even
        // though the two values differ and neither side was ever told the other's. Since issue #259, a
        // completed set reassembles into exactly the Message its sender sent — but a receive-side
        // Fragment slot is sized to that receiver's own configured fragment size (#257), not the
        // sender's, so a Fragment wider than the receiver's own slot is refused outright and its group
        // can never complete: the dialler's fragments (600 bytes each) fit comfortably inside the
        // accepter's 900-byte slot, but the accepter's fragments (900 bytes each) do not fit the
        // dialler's 600-byte slot, so only the dialler-to-accepter direction ever assembles.
        accepterPayloads.Should().BeEquivalentTo(
            new[] { fromDialler }, "the dialler's 600-byte Fragments fit inside the accepter's 900-byte slot and reassemble whole");
        diallerPayloads.Should().BeEmpty(
            "the accepter's 900-byte Fragments do not fit inside the dialler's own 600-byte slot, so that group can never complete");
    }
}
