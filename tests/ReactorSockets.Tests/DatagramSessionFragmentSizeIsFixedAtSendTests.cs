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
        int firstCapturesBefore = tap.Captures.Count;
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, firstMessage).Should().Be(SendOutcome.Sent);
        List<byte[]> firstDatagrams = DatagramSessionReactorHarness.PumpOnceAndRelayDataDatagrams(
            dialler, tap, firstCapturesBefore, (1000 + fragmentSize - 1) / fragmentSize);

        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));

        byte[] secondMessage = Enumerable.Range(0, 1900).Select(i => (byte)(i % 256)).ToArray();
        int secondCapturesBefore = tap.Captures.Count;
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, secondMessage).Should().Be(SendOutcome.Sent);
        List<byte[]> secondDatagrams = DatagramSessionReactorHarness.PumpOnceAndRelayDataDatagrams(
            dialler, tap, secondCapturesBefore, (1900 + fragmentSize - 1) / fragmentSize);

        // Assert — both Messages are wider than one Fragment, so each cut into more than one piece, and
        // the first piece of each is exactly fragmentSize: the same value, both times.
        firstDatagrams.Sum(CountRecords).Should().Be((1000 + fragmentSize - 1) / fragmentSize);
        secondDatagrams.Sum(CountRecords).Should().Be((1900 + fragmentSize - 1) / fragmentSize);
        FirstRecordLength(firstDatagrams[0]).Should().Be((ushort)fragmentSize, "the first Send cut its Message to this Peer's fragment size");
        FirstRecordLength(secondDatagrams[0]).Should().Be(
            FirstRecordLength(firstDatagrams[0]), "a later Send to the same Peer cuts with the same fragment size as the first — it was captured once, not re-resolved");
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

    /// <summary>One drain's worth of Events from one end: every Event kind, and a copy of every received payload.</summary>
    private static void DrainInto(DatagramSessionReactorHarness harness, List<SessionEventKind> kinds, List<byte[]> payloads)
    {
        foreach (SessionEvent evt in harness.Reactor.Drain())
        {
            kinds.Add(evt.Kind);
            if (evt.Kind == SessionEventKind.MessageReceived)
            {
                payloads.Add(evt.Payload.ToArray());
            }
        }
    }

    [Theory]
    [InlineData(DeliveryMode.ReliableOrdered, 900, 600, true)]
    [InlineData(DeliveryMode.ReliableOrdered, 600, 900, true)]
    [InlineData(DeliveryMode.ReliableOrdered, 900, 600, false)]
    [InlineData(DeliveryMode.ReliableOrdered, 600, 900, false)]
    [InlineData(DeliveryMode.ReliableUnordered, 900, 600, true)]
    [InlineData(DeliveryMode.ReliableUnordered, 600, 900, true)]
    [InlineData(DeliveryMode.ReliableUnordered, 900, 600, false)]
    [InlineData(DeliveryMode.ReliableUnordered, 600, 900, false)]
    public void Send_TwoSessionsWithDifferentFragmentSizes_DeliverBothMessagesIntactAndRaiseNoPeerClosed(
        DeliveryMode mode, int senderFragmentSize, int receiverFragmentSize, bool senderIsDialler)
    {
        // Arrange — capacity 1 and retention for the Fragment counts (2300 at 600 is 4 Fragments, plus
        // the 50-byte Message), so neither the frame arena share nor retention drops a Message. The
        // sender is the dialler or the accepter, because each end fixes its effective size at a different
        // point of the handshake.
        const int maxMessageSize = 4000;
        int diallerFragmentSize = senderIsDialler ? senderFragmentSize : receiverFragmentSize;
        int accepterFragmentSize = senderIsDialler ? receiverFragmentSize : senderFragmentSize;
        using DatagramSessionReactorHarness dialler = new(
            capacity: 1, bindLoopback: false, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: diallerFragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 1, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: accepterFragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerHandshakeEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterHandshakeEvents = [];
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(
            dialler, tap, accepter, diallerHandshakeEvents, accepterHandshakeEvents);
        DatagramSessionReactorHarness sender = senderIsDialler ? dialler : accepter;
        DatagramSessionReactorHarness receiver = senderIsDialler ? accepter : dialler;
        PeerId senderPeer = senderIsDialler ? diallerPeer : accepterPeer;
        byte[] large = Enumerable.Range(0, 2300).Select(i => (byte)(i % 251)).ToArray();
        byte[] small = Enumerable.Range(0, 50).Select(i => (byte)(255 - i)).ToArray();
        List<SessionEventKind> senderKinds = [];
        List<SessionEventKind> receiverKinds = [];
        List<byte[]> received = [];
        List<byte[]> ignored = [];

        // Act — Pump with the clock advancing so acknowledgements flow, draining both ends after every
        // round and keeping every Event kind seen. A few rounds past the second delivery would expose a
        // late PeerClosed.
        sender.Reactor.Send(senderPeer, mode, large).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(senderPeer, mode, small).Should().Be(SendOutcome.Sent);
        int settleRounds = 0;
        for (int round = 0; round < 400 && settleRounds < 15; round++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
            DrainInto(sender, senderKinds, ignored);
            DrainInto(receiver, receiverKinds, received);
            dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
            Thread.Sleep(1);
            if (received.Count >= 2)
            {
                settleRounds++;
            }
        }

        // Assert
        received.Should().HaveCount(2);
        if (mode == DeliveryMode.ReliableOrdered)
        {
            received[0].Should().Equal(large, "the 2300-byte Message is delivered first");
            received[1].Should().Equal(small, "the 50-byte Message is delivered after it");
        }
        else
        {
            received.Should().ContainSingle(p => p.SequenceEqual(large));
            received.Should().ContainSingle(p => p.SequenceEqual(small));
        }

        senderKinds.Should().NotContain(SessionEventKind.PeerClosed);
        receiverKinds.Should().NotContain(SessionEventKind.PeerClosed);
        diallerHandshakeEvents.Select(e => e.Kind).Should().NotContain(SessionEventKind.PeerClosed);
        accepterHandshakeEvents.Select(e => e.Kind).Should().NotContain(SessionEventKind.PeerClosed);
    }
}
