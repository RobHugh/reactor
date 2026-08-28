using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 7.4.1.4 (issue #288): the boundary <see cref="DatagramSessionReactor.Send"/> enforces on a
/// reliable Message is inclusive, and refusing at it is free. #284 changed that boundary from this
/// Reactor's own <see cref="DatagramSessionOptions.MaxMessageSize"/> to the peer's own announced limit,
/// so every test below gives the dialler and the accepter different limits: a Message that fits between
/// the two is refused only because the boundary belongs to the remote, which a shared limit could never
/// prove.
/// </summary>
/// <remarks>
/// The no-retention half is asserted through <see cref="PeerCounters.RetentionHighWaterBytes"/>, the
/// same reachable figure <see cref="DatagramSessionRefusedSendTraceTests"/> already documents: nothing
/// on any live path calls <c>Core.RetentionPool.Release</c> yet (issue #187), so this high-water mark
/// is monotonic for the life of the Peer and staying flat across a refusal is exactly what "acquired no
/// retention" means through the seam. No sharper figure is reachable without opening L0.
/// </remarks>
public class DatagramSessionRemoteMessageSizeTests
{
    private const int FragmentSize = 576;
    private const int AccepterMaxMessageSize = 600;
    private const int DiallerMaxMessageSize = 4096;
    private const int RetentionBytes = FragmentSize * 20;

    [Fact]
    public void Send_AMessageExactlyAtThePeersAnnouncedLimit_ReturnsSent()
    {
        // Arrange — the accepter announces 600, the dialler's own limit is 4096. A message this size
        // would be accepted under the dialler's own (unused) limit too, so the case that actually proves
        // the boundary is remote-owned is the one immediately below; this one pins that the boundary is
        // inclusive rather than exclusive.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: DiallerMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: AccepterMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        byte[] exactlyAtLimit = new byte[AccepterMaxMessageSize];

        // Act
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, exactlyAtLimit);

        // Assert
        outcome.Should().Be(SendOutcome.Sent, "a Message of exactly the peer's announced limit is one the design promises can complete");
    }

    [Fact]
    public void Send_OneByteOverThePeersAnnouncedLimitButUnderThisReactorsOwn_ReturnsRemoteMessageTooLarge()
    {
        // Arrange — the decisive case: 601 bytes is over the accepter's announced 600 but well under the
        // dialler's own 4096. A Reactor still checking its own MaxMessageSize would accept this; only
        // checking the peer's announced limit refuses it.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: DiallerMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: AccepterMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        byte[] oneByteOver = new byte[AccepterMaxMessageSize + 1];

        // Act
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, oneByteOver);

        // Assert
        outcome.Should().Be(
            SendOutcome.RemoteMessageTooLarge,
            "601 bytes exceeds the peer's own announced 600-byte limit even though it is far inside this Reactor's own 4096");
    }

    [Fact]
    public void Send_RefusedAsRemoteMessageTooLarge_AcquiresNoRetentionAcrossTheRefusal()
    {
        // Arrange — same mismatched limits as above. An accepted small Send establishes a non-zero
        // high-water mark; the refusal in between must leave it exactly where that Send left it, because
        // the figure is monotonic and a refusal that took even one Fragment's worth would move it.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: DiallerMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: AccepterMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        byte[] small = [1, 2, 3];
        byte[] oneByteOver = new byte[AccepterMaxMessageSize + 1];

        // Act
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, small).Should().Be(SendOutcome.Sent);
        ulong beforeRefusal = dialler.Reactor.CountersOf(diallerPeer).RetentionHighWaterBytes;

        SendOutcome refused = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, oneByteOver);
        ulong afterRefusal = dialler.Reactor.CountersOf(diallerPeer).RetentionHighWaterBytes;

        // Assert
        refused.Should().Be(SendOutcome.RemoteMessageTooLarge);
        afterRefusal.Should().Be(beforeRefusal, "the refusal is checked before any Fragment of it is retained, so the high-water mark cannot move");
    }

    [Fact]
    public void Send_RefusedAsRemoteMessageTooLarge_ConsumesNoSequenceAndLeavesThePeerAbleToSendASmallerMessageWhole()
    {
        // Arrange — same mismatched limits. A send, a refusal, and a second send: if the refusal had
        // burned a sequence number, the second accepted record would carry sequence 1 instead of the
        // sequence 0 immediately after the first accepted one — decoded straight off the wire, the same
        // way DatagramSessionRefusedSendTraceTests reads it, because nothing on the public seam surfaces
        // a sequence number directly. The refused Send also queues no record, so only two records — the
        // accepted ones — ever reach the wire, and the second one's payload is what proves the Peer
        // "untouched" claim: it must arrive whole at the accepter, not merely be accepted at the call.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: DiallerMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: AccepterMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] first = [1, 2, 3];
        byte[] oneByteOver = new byte[AccepterMaxMessageSize + 1];
        byte[] second = [4, 5, 6, 7];

        // Act
        SendOutcome accepted1 = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, first);
        SendOutcome refused = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, oneByteOver);
        SendOutcome accepted2 = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, second);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert
        accepted1.Should().Be(SendOutcome.Sent);
        refused.Should().Be(SendOutcome.RemoteMessageTooLarge);
        accepted2.Should().Be(SendOutcome.Sent, "the refusal did not touch the Peer, so a smaller Message sends immediately afterwards");

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap);
        datagram.Records.Should().HaveCount(2, "the refused Send queued no record to carry");
        datagram.Records[0].Sequence.Should().Be(0U, "the first accepted Send is this Space's first allocation");
        datagram.Records[0].Payload.Should().Equal(first);
        datagram.Records[1].Sequence.Should().Be(1U, "the refusal between them claimed no sequence, so the second accepted Send is allocation 1, not 2");
        datagram.Records[1].Payload.Should().Equal(second);

        delivered.Should().ContainSingle(payload => payload.SequenceEqual(second), "the second Message must arrive whole at the accepter, not merely be accepted at the call");
    }

    /// <summary>
    /// The first datagram the dialler sent that is not a control datagram, decoded. A control datagram
    /// carries session id zero; a Session datagram carries the receiver-assigned id, which is never
    /// zero, exactly as <see cref="DatagramSessionRefusedSendTraceTests"/> separates them.
    /// </summary>
    private static DecodedDatagram DecodeFirstDataDatagram(WireTap tap)
    {
        byte[] payload = tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .First(p => BinaryPrimitives.ReadUInt32BigEndian(p.AsSpan(0, 4)) != 0);

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);
        return decoded.Datagram!;
    }
}
