using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #248: fragment size is decided once, at <see cref="DatagramSessionReactor.Send"/>, captured
/// per-Peer at admission rather than read afresh from <see cref="DatagramSessionOptions"/> on every
/// call. It is unilateral and per-direction (ADR 0004) and is never announced in the handshake, because
/// a receiver reassembles a Fragment set from <c>fragmentIndex</c> and <c>fragmentCount</c> alone and
/// never needs to know the sender's choice. Every test here drives a live Session through
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
    public void Connect_HandshakeDatagrams_NeverContainTheConfiguredFragmentSizesBytes()
    {
        // Arrange — a fragment size distinctive enough that finding its 4-byte big-endian encoding
        // anywhere in a handshake datagram could not be an innocent coincidence: not a small round
        // number, and not a value any other handshake field would plausibly carry (issue #248, ADR
        // 0004 — fragment size is unilateral, per-direction and never announced).
        const int diallerFragmentSize = 23_456;
        const int accepterFragmentSize = 61_234;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, retentionBytes: diallerFragmentSize * 2, fragmentSize: diallerFragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            retentionBytes: accepterFragmentSize * 2, fragmentSize: accepterFragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);

        byte[] diallerNeedle = new byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(diallerNeedle, (uint)diallerFragmentSize);
        byte[] accepterNeedle = new byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(accepterNeedle, (uint)accepterFragmentSize);

        // Act — connect only. No Send is ever called, so every datagram the tap relays is part of the
        // handshake (ADR 0009's control profile), not a data datagram that could carry a Fragment
        // header of its own.
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Assert
        tap.Captures.Should().NotBeEmpty();
        foreach (TapCapture capture in tap.Captures)
        {
            capture.Payload.AsSpan().IndexOf(diallerNeedle).Should().Be(
                -1, $"the dialler's configured fragment size ({diallerFragmentSize}) must never be announced on the wire (ADR 0004)");
            capture.Payload.AsSpan().IndexOf(accepterNeedle).Should().Be(
                -1, $"the accepter's configured fragment size ({accepterFragmentSize}) must never be announced on the wire (ADR 0004)");
        }
    }

    [Fact]
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

        // Assert — every byte crosses intact and in order despite the two Sessions never agreeing on a
        // fragment size: each admitted record surfaces as its own delivery, and for ReliableOrdered
        // those arrive strictly in sequence order, so concatenating them recovers the original Message
        // exactly. Nothing here required the receiver to learn the sender's fragment size (ADR 0004) —
        // interoperation held even though the two values differ and neither side was ever told the
        // other's.
        accepterPayloads.Should().HaveCount((1500 + diallerFragmentSize - 1) / diallerFragmentSize);
        accepterPayloads.SelectMany(payload => payload).Should().Equal(fromDialler);

        diallerPayloads.Should().HaveCount((2300 + accepterFragmentSize - 1) / accepterFragmentSize);
        diallerPayloads.SelectMany(payload => payload).Should().Equal(fromAccepter);
    }
}
