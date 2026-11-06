using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The flags byte, observed off a live <see cref="DatagramSessionReactor"/> dialogue. A record's flags byte
/// (ADR 0009) carries two mode bits identifying which of the four delivery modes Sent it, a fragment
/// bit set only on a Fragment, and five spare bits that carry no meaning, which holds only while
/// nothing sets one. <see cref="DatagramDecoder"/> exposes the raw <see cref="DecodedPacketRecord.Flags"/>
/// byte precisely so a test can check the fragment and spare bits directly, rather than only through
/// <see cref="DecodedPacketRecord.Mode"/>, which the decoder derives by masking those same bits away.
/// </summary>
public class DatagramSessionModeFlagTests
{
    // Mirrors DatagramSessionProtocol's private layout (ADR 0009), out of reach from this test
    // assembly (ADR 0015): bits 0-1 are the mode, bit 2 is the fragment bit, bits 3-7 are spare.
    private const byte FragmentBit = 0b0000_0100;
    private const byte SpareBitsMask = 0b1111_1000;

    [Fact]
    public void Send_OneRecordInEachOfTheFourModes_DecodesToADistinctPairOfModeBitsPerRecord()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — one Send per mode, coalesced by one Pump into one Datagram (issue #216 order,
        // #232's coalescing proof).
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.UnreliableSequenced, [2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [3]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [4]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — four records, each a distinct mode-bit pair, covering all four wire values.
        datagram.Records.Should().HaveCount(4, "one record per mode was Sent before the single Pump");
        datagram.Records.Select(r => r.Mode).Distinct().Should().HaveCount(
            4, "a record Sent in each of the four delivery modes must decode to a distinct pair of mode bits");
        datagram.Records.Select(r => r.Mode).Should().BeEquivalentTo(
            [DeliveryMode.Unreliable, DeliveryMode.UnreliableSequenced, DeliveryMode.ReliableUnordered, DeliveryMode.ReliableOrdered],
            "the four modes this library defines are exactly the four distinct mode-bit pairs observed");
    }

    [Fact]
    public void Send_OneRecordInEachOfTheFourModes_TheModeBitsARecordCarriesAreTheModeItWasSentIn()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Each payload names the mode it is Sent in, so the assertion below matches each decoded
        // record back to the Send that produced it without depending on wire order.
        (DeliveryMode Mode, byte[] Payload)[] sends =
        [
            (DeliveryMode.Unreliable, [10]),
            (DeliveryMode.UnreliableSequenced, [11]),
            (DeliveryMode.ReliableUnordered, [12]),
            (DeliveryMode.ReliableOrdered, [13]),
        ];

        // Act
        foreach ((DeliveryMode mode, byte[] payload) in sends)
        {
            dialler.Reactor.Send(diallerPeer, mode, payload).Should().Be(SendOutcome.Sent);
        }

        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — for every Send, the one record carrying that Send's payload decodes to that
        // Send's own mode, not any other.
        foreach ((DeliveryMode mode, byte[] payload) in sends)
        {
            DecodedPacketRecord record = datagram.Records.Single(r => r.Payload.SequenceEqual(payload));
            record.Mode.Should().Be(mode, $"the record carrying payload [{string.Join(",", payload)}] was Sent in {mode}");
        }
    }

    [Fact]
    public void Send_RecordsInEveryMode_HaveNoSpareBitSetOnTheRawFlagsByte()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.UnreliableSequenced, [2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [3]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [4]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — the decoder does not reject a set spare bit (it enforces write-side legality, not
        // read-side leniency), so this reads the raw flags byte directly, the only way to prove the
        // bit was never set rather than merely that a lenient decoder tolerated it.
        datagram.Records.Should().HaveCount(4);
        datagram.Records.Should().OnlyContain(
            r => (r.Flags & SpareBitsMask) == 0,
            "no record this library sends has any spare bit set, in any mode");
    }

    [Fact]
    public void Send_RecordsInEveryMode_HaveTheFragmentBitClearOnTheRawFlagsByte()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.UnreliableSequenced, [2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [3]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [4]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap, TapDirection.DiallerToAccepter);

        // Assert — same reasoning as the spare-bit proof above: read the raw flags byte, not the
        // decoder's derived Mode, since the fragment bit is not part of that derivation either.
        datagram.Records.Should().HaveCount(4);
        datagram.Records.Should().OnlyContain(
            r => (r.Flags & FragmentBit) == 0,
            "the fragment bit is clear on every record here, none being a fragment");
    }

    /// <summary>
    /// The first datagram captured travelling in <paramref name="direction"/> whose session id names a
    /// live Session rather than the reserved control session id — filters out the handshake's own
    /// control datagrams — decoded via <see cref="DatagramDecoder"/>.
    /// </summary>
    private static DecodedDatagram DecodeFirstDataDatagram(WireTap tap, TapDirection direction)
    {
        byte[] payload = tap.PayloadsInDirection(direction).First(
            candidate => BinaryPrimitives.ReadUInt32BigEndian(candidate.AsSpan(0, 4)) != 0);
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);
        return decoded.Datagram!;
    }
}
