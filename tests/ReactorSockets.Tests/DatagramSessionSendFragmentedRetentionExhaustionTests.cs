using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A single Message cut into more Fragments than its Space has free slots for is refused
/// whole by <see cref="DatagramSessionReactor.Send"/>, rather than leaving whatever fragments already
/// fit behind in retention. A half-retained Message is unrepairable, the receiver's reassembly waits
/// forever for a set that was never cut in full, and, like any refused Send, the refusal
/// must not burn any of the Fragments' sequence numbers either, since a burned sequence opens a hole
/// the receiver's window would wait on for a record that is never sent.
/// </summary>
public class DatagramSessionSendFragmentedRetentionExhaustionTests
{
    private const int FragmentSize = 576;
    private const int SlotsPerSpace = 3;
    private const int RetentionBytes = FragmentSize * SlotsPerSpace;
    private const int MaxMessageSize = FragmentSize * 10;

    public static TheoryData<DeliveryMode> ReliableModes => new()
    {
        DeliveryMode.ReliableOrdered,
        DeliveryMode.ReliableUnordered,
    };

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MessageNeedingMoreFragmentsThanTheSpaceHasFreeSlots_ReturnsRetentionExhaustedFromOneSend(DeliveryMode mode)
    {
        // Arrange - the Space holds SlotsPerSpace slots; a Message needing one more Fragment than that
        // cannot possibly fit, in one Send, with nothing sent before it.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] message = new byte[FragmentSize * (SlotsPerSpace + 1)];

        // Act
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, mode, message);

        // Assert
        outcome.Should().Be(SendOutcome.RetentionExhausted, "the Message needs one more Fragment than the Space has free slots for");
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_RefusedAsRetentionExhausted_LeavesTheSpaceWithEveryOriginalSlotFree(DeliveryMode mode)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] tooWide = new byte[FragmentSize * (SlotsPerSpace + 1)];
        dialler.Reactor.Send(diallerPeer, mode, tooWide).Should().Be(SendOutcome.RetentionExhausted);

        // Act - if the refusal had left any of tooWide's Fragments retained, one of these SlotsPerSpace
        // single-Fragment Sends would itself be refused for want of a free slot.
        List<SendOutcome> outcomes = [];
        for (int i = 0; i < SlotsPerSpace; i++)
        {
            outcomes.Add(dialler.Reactor.Send(diallerPeer, mode, new byte[FragmentSize]));
        }

        // Assert
        outcomes.Should().AllSatisfy(o => o.Should().Be(SendOutcome.Sent, "the refusal retained none of the wide Message's Fragments"));
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_RefusedAsRetentionExhausted_BurnsNoSequenceSoTheNextSendCarriesSequenceZero(DeliveryMode mode)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] tooWide = new byte[FragmentSize * (SlotsPerSpace + 1)];
        SendOutcome refused = dialler.Reactor.Send(diallerPeer, mode, tooWide);

        // Act - a smaller Message on the same Peer, in the same Space, sent after the refusal.
        byte[] accepted = [9, 8, 7];
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, mode, accepted);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        refused.Should().Be(SendOutcome.RetentionExhausted);
        outcome.Should().Be(SendOutcome.Sent, "the Space was left usable after the refusal");

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap);
        DecodedPacketRecord record = datagram.Records.Should().ContainSingle(r => r.Mode == mode).Subject;
        record.Sequence.Should().Be(0U, "the refused Send's Fragments spent none of this Space's sequence numbers");
        record.Payload.Should().Equal(accepted);
    }

    /// <summary>The first datagram the dialler sent that is not a control datagram, decoded.</summary>
    private static DecodedDatagram DecodeFirstDataDatagram(WireTap tap)
    {
        byte[] payload = tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .First(p => BinaryPrimitives.ReadUInt32BigEndian(p.AsSpan(0, 4)) != 0);

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);
        return decoded.Datagram!;
    }
}
