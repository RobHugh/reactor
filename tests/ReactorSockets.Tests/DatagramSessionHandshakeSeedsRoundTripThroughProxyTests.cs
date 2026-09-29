using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Review R1 on issue #57: a dialled Peer seeds its round-trip estimate from the handshake
/// (Hello-with-cookie to Accept), so its very first reliable record has a real retransmission timeout.
/// Unseeded, the timeout equals the Linger, so a Goodbye lost before any ack could never be resent inside
/// the Linger: the Linger expiry since Close fires in the same Pump the resend would.
/// </summary>
/// <remarks>
/// The handshake runs with the clock advancing ten milliseconds per round so the measured leg is not zero.
/// The Linger is ten seconds, far above the handshake round trip plus the resulting timeout.
/// </remarks>
public class DatagramSessionHandshakeSeedsRoundTripThroughProxyTests
{
    private const byte ControlBit = 0b0000_1000;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _handshakeRound = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    private static bool CarriesAGoodbye(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        return decoded.IsSuccess
            && decoded.Datagram!.Records.Any(r => r.Mode == DeliveryMode.ReliableOrdered && (r.Flags & ControlBit) != 0);
    }

    [Fact]
    public void Pump_DiallersFirstReliableRecordIsItsGoodbyeAndIsDropped_IsResentInsideTheLingerAndClosesBothEnds()
    {
        // Arrange — the handshake takes real (fake-clock) time, and nothing is sent before Close.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];
        for (int round = 0; round < 20 && !(diallerEvents.Concat(accepterEvents).Count(e => e.Kind == SessionEventKind.PeerConnected) == 2); round++)
        {
            dialler.TimeProvider.Advance(_handshakeRound);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            diallerEvents.AddRange(dialler.DrainToList());
            accepterEvents.AddRange(accepter.DrainToList());
        }

        diallerEvents.Concat(accepterEvents).Count(e => e.Kind == SessionEventKind.PeerConnected)
            .Should().Be(2, "both ends must be Established before the Goodbye");
        ulong retransmittedBefore = dialler.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — Close is the Peer's first reliable record, and its datagram is dropped.
        dialler.Reactor.Close(peer);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Pump();
        diallerEvents.AddRange(dialler.DrainToList());
        proxy.Pump();
        accepter.Reactor.Pump();
        accepterEvents.AddRange(accepter.DrainToList());

        proxy.Forwarded.Count(f => f.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(f.Payload))
            .Should().Be(0, "the only Goodbye transmission so far was dropped");

        ulong retransmittedAfterResend = retransmittedBefore;
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && retransmittedAfterResend == retransmittedBefore)
        {
            dialler.TimeProvider.Advance(_step);
            elapsed += _step;
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            retransmittedAfterResend = dialler.Reactor.CountersOf(peer).PacketsRetransmitted;
        }

        proxy.Pump();
        accepter.Reactor.Pump();
        accepterEvents.AddRange(accepter.DrainToList());
        proxy.Pump();
        dialler.Reactor.Pump();
        diallerEvents.AddRange(dialler.DrainToList());
        diallerEvents.AddRange(dialler.PumpAndDrainAll(2));
        accepterEvents.AddRange(accepter.PumpAndDrainAll(2));

        // Assert
        elapsed.Should().BeLessThan(_linger / 2, "the resend fired well inside the Linger");
        (retransmittedAfterResend - retransmittedBefore).Should().Be(1UL, "the Goodbye was resent exactly once");
        diallerEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
        accepterEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
    }
}
