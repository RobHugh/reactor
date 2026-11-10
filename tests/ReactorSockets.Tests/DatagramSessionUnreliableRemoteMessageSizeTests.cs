using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// An unreliable Message above the peer's announced <c>MaxMessageSize</c> is certain to be
/// discarded on arrival (issue #425), so <see cref="DatagramSessionReactor.Send"/> refuses it, spending
/// nothing. The one-Datagram cap is checked first, so a Message above the fragment size still returns
/// <see cref="SendOutcome.MessageTooLarge"/> whatever the peer announced.
/// </summary>
public class DatagramSessionUnreliableRemoteMessageSizeTests
{
    private const int FragmentSize = 576;
    private const int DiallerMaxMessageSize = 4096;
    private const int BelowFragmentSizeLimit = 100;
    private const int AboveFragmentSizeLimit = 2048;

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_OneByteOverThePeersLimitAndWithinTheFragmentSize_ReturnsRemoteMessageTooLarge(DeliveryMode mode)
    {
        // Arrange
        using Fixture fixture = Fixture.Build(BelowFragmentSizeLimit);

        // Act
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, new byte[BelowFragmentSizeLimit + 1]);

        // Assert
        outcome.Should().Be(SendOutcome.RemoteMessageTooLarge);
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_RefusedOverThePeersLimit_PutsNoRecordOnTheWire(DeliveryMode mode)
    {
        // Arrange
        using Fixture fixture = Fixture.Build(BelowFragmentSizeLimit);

        // Act
        fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, new byte[BelowFragmentSizeLimit + 1]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(fixture.Dialler, fixture.Tap, fixture.Accepter);

        // Assert
        DecodeDataDatagrams(fixture.Tap).SelectMany(datagram => datagram.Records).Should().BeEmpty();
        fixture.Accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.OversizedMessage);
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_RefusedOverThePeersLimit_LeavesTheNextSequenceUnchanged(DeliveryMode mode)
    {
        // Arrange
        using Fixture fixture = Fixture.Build(BelowFragmentSizeLimit);

        // Act
        fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, [1]).Should().Be(SendOutcome.Sent);
        fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, new byte[BelowFragmentSizeLimit + 1])
            .Should().Be(SendOutcome.RemoteMessageTooLarge);
        fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, [2]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(fixture.Dialler, fixture.Tap, fixture.Accepter);

        // Assert
        List<DecodedPacketRecord> records = DecodeDataDatagrams(fixture.Tap).SelectMany(datagram => datagram.Records).ToList();
        records.Should().HaveCount(2);
        records[0].Sequence.Should().Be(0U);
        records[1].Sequence.Should().Be(1U);
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_ExactlyAtThePeersLimit_IsSentAndDelivered(DeliveryMode mode)
    {
        // Arrange
        using Fixture fixture = Fixture.Build(BelowFragmentSizeLimit);
        byte[] atLimit = Enumerable.Range(0, BelowFragmentSizeLimit).Select(i => (byte)i).ToArray();

        // Act
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, atLimit);
        DatagramSessionReactorHarness.PumpRoundThroughTap(fixture.Dialler, fixture.Tap, fixture.Accepter);

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
        fixture.Accepter.DrainPayloads().Should().ContainSingle().Which.Should().Equal(atLimit);
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable, BelowFragmentSizeLimit)]
    [InlineData(DeliveryMode.UnreliableSequenced, BelowFragmentSizeLimit)]
    [InlineData(DeliveryMode.Unreliable, AboveFragmentSizeLimit)]
    [InlineData(DeliveryMode.UnreliableSequenced, AboveFragmentSizeLimit)]
    public void Send_OneByteOverTheFragmentSize_ReturnsMessageTooLargeWhateverThePeersLimit(DeliveryMode mode, int peerLimit)
    {
        // Arrange
        using Fixture fixture = Fixture.Build(peerLimit);

        // Act
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, mode, new byte[FragmentSize + 1]);

        // Assert
        outcome.Should().Be(SendOutcome.MessageTooLarge);
    }

    /// <summary>
    /// Every Session datagram the dialler sent, decoded. A control datagram carries session id zero; a
    /// Session datagram carries the receiver-assigned id, which is never zero.
    /// </summary>
    private static List<DecodedDatagram> DecodeDataDatagrams(WireTap tap)
    {
        List<DecodedDatagram> datagrams = [];
        foreach (byte[] payload in tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .Where(p => BinaryPrimitives.ReadUInt32BigEndian(p.AsSpan(0, 4)) != 0))
        {
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
            decoded.IsSuccess.Should().BeTrue(decoded.Error);
            datagrams.Add(decoded.Datagram!); // IsSuccess was just asserted, and Datagram is non-null whenever it holds.
        }

        return datagrams;
    }

    /// <summary>A dialler connected through a <see cref="WireTap"/> to an accepter announcing the limit under test.</summary>
    private sealed class Fixture : IDisposable
    {
        public required DatagramSessionReactorHarness Dialler { get; init; }

        public required DatagramSessionReactorHarness Accepter { get; init; }

        public required PeerId DiallerPeer { get; init; }

        public required WireTap Tap { get; init; }

        public static Fixture Build(int accepterMaxMessageSize)
        {
            DatagramSessionReactorHarness dialler = new(
                bindLoopback: false, maxMessageSize: DiallerMaxMessageSize, fragmentSize: FragmentSize);
            DatagramSessionReactorHarness accepter = new(
                maxMessageSize: accepterMaxMessageSize, fragmentSize: FragmentSize);

            WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
            PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
            DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
            return new Fixture { Dialler = dialler, Accepter = accepter, DiallerPeer = peer, Tap = tap };
        }

        public void Dispose()
        {
            Dialler.Dispose();
            Accepter.Dispose();
            Tap.Dispose();
        }
    }
}
