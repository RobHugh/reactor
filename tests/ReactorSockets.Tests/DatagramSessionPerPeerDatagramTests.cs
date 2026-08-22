using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.7.6 (issue #233): coalescing is per Peer, not per Pump. Three established Peers, Sends
/// queued against each between two Pumps, leave exactly three data datagrams on the wire — one per
/// Peer, each carrying only the records Sent to that Peer.
/// </summary>
/// <remarks>
/// <see cref="WireTap"/> tracks exactly one dialler at a time (whichever address last sent it a
/// datagram — see its own remarks), which one-Reactor-with-three-Peers cannot fit through a single
/// instance: three simultaneous diallers relayed through one tap would have the accepter's replies to
/// the first two clobbered by <c>_lastDialler</c> tracking only the third. This suite instead uses
/// three <see cref="WireTap"/> instances, each a private point-to-point relay for one dialler, all
/// aimed at the same accepter endpoint — the accepter Reactor still sees three distinct source
/// addresses (one per tap's accepter-facing link) and so admits three distinct Peers on the one
/// Reactor under test, exactly as the ticket asks. No change to <see cref="WireTap"/> itself was
/// needed.
/// </remarks>
public class DatagramSessionPerPeerDatagramTests
{
    [Fact]
    public void Pump_SendsToThreeEstablishedPeers_LeavesExactlyOneDataDatagramPerPeerEachCarryingOnlyItsOwnRecords()
    {
        // Arrange — one accepter Reactor, three diallers each relayed through its own tap so all three
        // reach the same accepter as three distinct Peers.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler1 = new(bindLoopback: false);
        using DatagramSessionReactorHarness dialler2 = new(bindLoopback: false);
        using DatagramSessionReactorHarness dialler3 = new(bindLoopback: false);
        using WireTap tap1 = new(accepter.Endpoint, dialler1.TimeProvider);
        using WireTap tap2 = new(accepter.Endpoint, dialler2.TimeProvider);
        using WireTap tap3 = new(accepter.Endpoint, dialler3.TimeProvider);

        dialler1.Reactor.Connect(tap1.PublicEndPoint);
        dialler2.Reactor.Connect(tap2.PublicEndPoint);
        dialler3.Reactor.Connect(tap3.PublicEndPoint);

        PeerId accepterPeer1 = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler1, tap1, accepter);
        PeerId accepterPeer2 = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler2, tap2, accepter);
        PeerId accepterPeer3 = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler3, tap3, accepter);

        int baseline1 = tap1.CountInDirection(TapDirection.AccepterToDialler);
        int baseline2 = tap2.CountInDirection(TapDirection.AccepterToDialler);
        int baseline3 = tap3.CountInDirection(TapDirection.AccepterToDialler);

        byte[] toPeer1 = [1, 1, 1];
        byte[] toPeer2 = [2, 2, 2, 2];
        byte[] toPeer3 = [3, 3, 3, 3, 3];

        // Act — Sends queued against all three Peers, then a single Pump flushes every Peer's queue,
        // then every tap is pumped once so each relays whatever the accepter sent it.
        accepter.Reactor.Send(accepterPeer1, DeliveryMode.ReliableOrdered, toPeer1);
        accepter.Reactor.Send(accepterPeer2, DeliveryMode.ReliableOrdered, toPeer2);
        accepter.Reactor.Send(accepterPeer3, DeliveryMode.ReliableOrdered, toPeer3);
        accepter.Reactor.Pump();
        tap1.Pump();
        tap2.Pump();
        tap3.Pump();

        // Assert — exactly one new arrival per tap: three data datagrams total, one per Peer.
        List<byte[]> newCaptures1 = [.. tap1.PayloadsInDirection(TapDirection.AccepterToDialler).Skip(baseline1)];
        List<byte[]> newCaptures2 = [.. tap2.PayloadsInDirection(TapDirection.AccepterToDialler).Skip(baseline2)];
        List<byte[]> newCaptures3 = [.. tap3.PayloadsInDirection(TapDirection.AccepterToDialler).Skip(baseline3)];

        newCaptures1.Should().ContainSingle("Peer 1's own datagram is the only new arrival on its tap");
        newCaptures2.Should().ContainSingle("Peer 2's own datagram is the only new arrival on its tap");
        newCaptures3.Should().ContainSingle("Peer 3's own datagram is the only new arrival on its tap");

        DatagramDecodeResult decoded1 = DatagramDecoder.TryDecode(newCaptures1[0]);
        DatagramDecodeResult decoded2 = DatagramDecoder.TryDecode(newCaptures2[0]);
        DatagramDecodeResult decoded3 = DatagramDecoder.TryDecode(newCaptures3[0]);

        decoded1.IsSuccess.Should().BeTrue(decoded1.Error);
        decoded2.IsSuccess.Should().BeTrue(decoded2.Error);
        decoded3.IsSuccess.Should().BeTrue(decoded3.Error);

        // Each Peer's own datagram carries only the record Sent to that Peer — never another Peer's.
        decoded1.Datagram!.Records.Should().ContainSingle().Which.Payload.Should().Equal(toPeer1);
        decoded2.Datagram!.Records.Should().ContainSingle().Which.Payload.Should().Equal(toPeer2);
        decoded3.Datagram!.Records.Should().ContainSingle().Which.Payload.Should().Equal(toPeer3);
    }

    [Fact]
    public void Pump_SendsToTwoOfThreeEstablishedPeers_LeavesTwoArrivalsNotThree()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler1 = new(bindLoopback: false);
        using DatagramSessionReactorHarness dialler2 = new(bindLoopback: false);
        using DatagramSessionReactorHarness dialler3 = new(bindLoopback: false);
        using WireTap tap1 = new(accepter.Endpoint, dialler1.TimeProvider);
        using WireTap tap2 = new(accepter.Endpoint, dialler2.TimeProvider);
        using WireTap tap3 = new(accepter.Endpoint, dialler3.TimeProvider);

        dialler1.Reactor.Connect(tap1.PublicEndPoint);
        dialler2.Reactor.Connect(tap2.PublicEndPoint);
        dialler3.Reactor.Connect(tap3.PublicEndPoint);

        PeerId accepterPeer1 = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler1, tap1, accepter);
        PeerId accepterPeer2 = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler2, tap2, accepter);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler3, tap3, accepter);

        int baseline1 = tap1.CountInDirection(TapDirection.AccepterToDialler);
        int baseline2 = tap2.CountInDirection(TapDirection.AccepterToDialler);
        int baseline3 = tap3.CountInDirection(TapDirection.AccepterToDialler);

        // Act — only two of the three established Peers are Sent to.
        accepter.Reactor.Send(accepterPeer1, DeliveryMode.ReliableOrdered, [4, 4]);
        accepter.Reactor.Send(accepterPeer2, DeliveryMode.ReliableOrdered, [5, 5]);
        accepter.Reactor.Pump();
        tap1.Pump();
        tap2.Pump();
        tap3.Pump();

        // Assert — two arrivals, not three: the untouched Peer's tap sees nothing new.
        int newArrivals1 = tap1.CountInDirection(TapDirection.AccepterToDialler) - baseline1;
        int newArrivals2 = tap2.CountInDirection(TapDirection.AccepterToDialler) - baseline2;
        int newArrivals3 = tap3.CountInDirection(TapDirection.AccepterToDialler) - baseline3;

        newArrivals1.Should().Be(1, "Peer 1 was Sent to");
        newArrivals2.Should().Be(1, "Peer 2 was Sent to");
        newArrivals3.Should().Be(0, "Peer 3 was not Sent to, so it must receive nothing this Pump");
        (newArrivals1 + newArrivals2 + newArrivals3).Should().Be(2, "sending to two of three Peers leaves two arrivals, not three");
    }
}
