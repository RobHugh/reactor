using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Review D3 on PR #409 (parent #57): when both ends <c>Close</c> at once, each end reports
/// <see cref="DisconnectReason.LocalClose"/>. A Peer whose own Close is in progress that receives the remote's
/// Goodbye acknowledges it and raises neither <see cref="SessionEventKind.PeerClosing"/> nor
/// <see cref="DisconnectReason.PeerGoodbye"/>. Every Pump is drained.
/// </summary>
public class DatagramSessionSimultaneousCloseThroughProxyTests
{
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    /// <summary>A connected pair through a proxy, both ends with a seeded round-trip estimate, and every event each end drained.</summary>
    private sealed class Rig : IDisposable
    {
        public DatagramSessionReactorHarness A { get; } = new(bindLoopback: false, linger: _linger);

        public DatagramSessionReactorHarness B { get; }

        public LossyProxy Proxy { get; }

        public PeerId PeerA { get; }

        public PeerId PeerB { get; }

        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> EventsA { get; } = [];

        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> EventsB { get; } = [];

        public Rig()
        {
            B = new(linger: _linger, timeProvider: A.TimeProvider);
            Proxy = new(B.Endpoint, A.TimeProvider);
            PeerA = A.Reactor.Connect(Proxy.PublicEndPoint);
            PeerB = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(A, Proxy, B);

            // Each end sends one reliable record the other acknowledges, so both have a sampled round trip.
            // Unseeded, an accepter's first reliable record resends only at the Linger.
            A.Reactor.Send(PeerA, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
            PumpA();
            Proxy.Pump();
            PumpB();
            A.TimeProvider.Advance(TimeSpan.FromMilliseconds(20));
            B.Reactor.Send(PeerB, DeliveryMode.ReliableOrdered, [2]).Should().Be(SendOutcome.Sent);
            PumpB();
            Proxy.Pump();
            PumpA();
            A.TimeProvider.Advance(TimeSpan.FromMilliseconds(20));
            A.Reactor.Send(PeerA, DeliveryMode.Unreliable, [3]).Should().Be(SendOutcome.Sent);
            PumpA();
            Proxy.Pump();
            PumpB();
            EventsA.Clear();
            EventsB.Clear();
        }

        public void PumpA()
        {
            A.Reactor.Pump();
            EventsA.AddRange(A.DrainToList());
        }

        public void PumpB()
        {
            B.Reactor.Pump();
            EventsB.AddRange(B.DrainToList());
        }

        /// <summary>One relayed round, each Pump drained: A, proxy, B, proxy, A.</summary>
        public void Round()
        {
            PumpA();
            Proxy.Pump();
            PumpB();
            Proxy.Pump();
            PumpA();
        }

        /// <summary>Rounds in steps until both ends have closed or half a Linger has passed. Returns the time it took.</summary>
        public TimeSpan RoundsUntilBothClosed()
        {
            TimeSpan elapsed = TimeSpan.Zero;
            while (elapsed < _linger / 2 && !(HasClosed(EventsA) && HasClosed(EventsB)))
            {
                A.TimeProvider.Advance(_step);
                elapsed += _step;
                Round();
            }

            Round();
            return elapsed;
        }

        private static bool HasClosed(List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events) =>
            events.Any(e => e.Kind == SessionEventKind.PeerClosed);

        public void Dispose()
        {
            Proxy.Dispose();
            B.Dispose();
            A.Dispose();
        }
    }

    private static void AssertBothEndsReportLocalClose(Rig rig, TimeSpan elapsed)
    {
        elapsed.Should().BeLessThan(_linger, "both ends closed inside the Linger");
        rig.EventsA.Should().ContainSingle().Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LocalClose,
            "A raised exactly one PeerClosed(LocalClose) and nothing else");
        rig.EventsB.Should().ContainSingle().Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LocalClose,
            "B raised exactly one PeerClosed(LocalClose) and nothing else");
    }

    [Fact]
    public void Pump_BothEndsCloseBeforeEitherPumps_EachRaisesOnlyPeerClosedLocalClose()
    {
        // Arrange
        using Rig rig = new();

        // Act
        rig.A.Reactor.Close(rig.PeerA);
        rig.B.Reactor.Close(rig.PeerB);
        TimeSpan elapsed = rig.RoundsUntilBothClosed();

        // Assert
        AssertBothEndsReportLocalClose(rig, elapsed);
    }

    [Fact]
    public void Pump_BStartsClosingWhileAsGoodbyeIsInFlight_BothEndsReportLocalClose()
    {
        // Arrange
        using Rig rig = new();
        rig.A.Reactor.Close(rig.PeerA);
        rig.Proxy.DelayNext(RelayDirection.ClientToUpstream, TimeSpan.FromMilliseconds(50));
        rig.PumpA();
        rig.Proxy.Pump();

        // Act — B closes while A's Goodbye is held. B's Goodbye reaches A, A acknowledges it, and B closes on
        // that ack before A's Goodbye is released to it.
        rig.B.Reactor.Close(rig.PeerB);
        rig.PumpB();
        rig.Proxy.Pump();
        rig.PumpA();
        rig.Proxy.Pump();
        rig.PumpB();
        rig.EventsB.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose, "B closed on its own Goodbye's ack before A's Goodbye reached it");
        rig.A.TimeProvider.Advance(TimeSpan.FromMilliseconds(60));
        TimeSpan elapsed = rig.RoundsUntilBothClosed();

        // Assert
        AssertBothEndsReportLocalClose(rig, elapsed);
    }

    [Fact]
    public void Pump_BClosedOnItsOwnAckAndTheReAckOfAsGoodbyeIsDropped_BothEndsStillReportLocalCloseInsideLinger()
    {
        // Arrange
        using Rig rig = new();
        rig.A.Reactor.Close(rig.PeerA);
        rig.Proxy.DelayNext(RelayDirection.ClientToUpstream, TimeSpan.FromMilliseconds(50));
        rig.PumpA();
        rig.Proxy.Pump();
        rig.B.Reactor.Close(rig.PeerB);
        rig.PumpB();
        rig.Proxy.Pump();
        rig.PumpA();
        rig.Proxy.Pump();
        rig.PumpB();

        // Act — A's held Goodbye now reaches a closed B, whose re-ack is dropped. A's RTO resends it.
        rig.A.TimeProvider.Advance(TimeSpan.FromMilliseconds(60));
        rig.Proxy.DropNext(RelayDirection.UpstreamToClient);
        rig.Proxy.Pump();
        rig.PumpB();
        rig.Proxy.Pump();
        rig.PumpA();
        rig.EventsA.Should().BeEmpty("the ack that would close A was dropped");
        TimeSpan elapsed = rig.RoundsUntilBothClosed();

        // Assert
        elapsed.Should().BeGreaterThan(TimeSpan.Zero, "A had to wait for its resend");
        rig.EventsB.Should().ContainSingle().Which.Reason.Should().Be(DisconnectReason.LocalClose);
        rig.EventsA.Should().ContainSingle().Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LocalClose);
        elapsed.Should().BeLessThan(_linger);
    }
}
