using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The deadlock the datagram profile's two independent liveness timers exist
/// to prevent. A single timer driven by receive idleness alone would deadlock two wholly quiet peers —
/// A's probe resets B's receive clock, so B stays silent, so A itself hears nothing and (wrongly)
/// declares a healthy peer dead. <see cref="DatagramSessionReactor.EnforceSendIdleTimer"/> (#340) and
/// <see cref="DatagramSessionReactor.EnforceReceiveIdleTimer"/> (#352) break that cycle by reading only
/// their own half of the state — a Ping goes out purely on send silence, never conditioned on what has
/// (or has not) been received — so each side keeps proving itself alive to the other independently of
/// whether the other has done the same yet.
/// </summary>
/// <remarks>
/// Both Reactors here share one <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/> and are
/// driven through a <see cref="LossyProxy"/> (unused as a fault injector here — see
/// <see cref="DatagramSessionReceiveIdleExpiryTests"/> for that use — just the two-real-Reactor rig with
/// captures in both directions). Neither Reactor ever calls <c>Send</c>: the only datagrams either one
/// ever emits are its own Pings, which is exactly the "two quiet peers" scenario the deadlock needs. Time
/// is advanced in steps far smaller than <see cref="_sendIdleInterval"/>, pumping the dialler, the proxy
/// and the accepter every step, for ten intervals' worth of quiet — comfortably past
/// <see cref="DatagramSessionOptions.ResolvedMissedHeartbeats"/> (3) receive-idle deadlines' worth of
/// elapsed time, which is exactly the deadline a merged-timer regression would trip well before this test
/// finishes.
/// </remarks>
public class DatagramSessionQuietPeersStayAliveTests
{
    /// <summary>Short enough that ten intervals (this file's whole run) is a small clock advance.</summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    /// <summary>Far longer than this file's whole run, so no unrelated Linger expiry is ever mistaken for the deadlock this file tests for.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(1);

    /// <summary>Well below <see cref="_sendIdleInterval"/> — ten steps per interval — so neither Reactor's timers are ever crossed in one advance.</summary>
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    /// <summary>The number of full <see cref="_sendIdleInterval"/>s of quiet this file observes.</summary>
    private const int IntervalsToObserve = 10;

    private const byte ControlBit = 0b0000_1000;

    // =========================================================================================
    // Neither Reactor raises PeerInterrupted or PeerClosed over ten intervals of quiet
    // =========================================================================================

    [Fact]
    public void Pump_TwoQuietPeersSharingASendIdleInterval_NeitherRaisesPeerInterruptedOrPeerClosedOverTenIntervals()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: _linger, sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(
            linger: _linger, sendIdleInterval: _sendIdleInterval, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];

        // Act — quiet on both sides, for ten send-idle intervals, in steps well below one interval.
        int steps = (int)(_sendIdleInterval.Ticks * IntervalsToObserve / _step.Ticks);
        for (int i = 0; i < steps; i++)
        {
            dialler.TimeProvider.Advance(_step);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            diallerEvents.AddRange(dialler.DrainToList());
            accepterEvents.AddRange(accepter.DrainToList());
        }

        // Assert — the independent send-idle Ping on each side kept the other's receive clock moving, so
        // neither end's receive-idle deadline (three missed heartbeats) was ever reached.
        diallerEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted || e.Kind == SessionEventKind.PeerClosed,
            "the dialler's own Pings kept the accepter's receive clock moving throughout");
        accepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted || e.Kind == SessionEventKind.PeerClosed,
            "the accepter's own Pings kept the dialler's receive clock moving throughout");
    }

    // =========================================================================================
    // Pings are observed flowing both ways, so the test is not passing only because the
    // clock never moved
    // =========================================================================================

    [Fact]
    public void Pump_TwoQuietPeersSharingASendIdleInterval_PingsAreObservedFlowingBothWaysAcrossTenIntervals()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: _linger, sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(
            linger: _linger, sendIdleInterval: _sendIdleInterval, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — quiet on both sides, for ten send-idle intervals, in steps well below one interval.
        int steps = (int)(_sendIdleInterval.Ticks * IntervalsToObserve / _step.Ticks);
        for (int i = 0; i < steps; i++)
        {
            dialler.TimeProvider.Advance(_step);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            dialler.DrainToList();
            accepter.DrainToList();
        }

        // Assert — a control-bearing datagram was captured in each direction: this is not passing merely
        // because the clock advanced with nothing on the wire.
        proxy.Captures.Should().Contain(
            capture => capture.Direction == RelayDirection.ClientToUpstream && CarriesAControlRecord(capture.Payload),
            "the dialler's own send-idle timer must have emitted at least one Ping toward the accepter");
        proxy.Captures.Should().Contain(
            capture => capture.Direction == RelayDirection.UpstreamToClient && CarriesAControlRecord(capture.Payload),
            "the accepter's own send-idle timer must have emitted at least one Ping toward the dialler");
    }

    /// <summary>Whether any record decoded from <paramref name="datagram"/> carries the control bit that marks a Ping (issue #340).</summary>
    private static bool CarriesAControlRecord(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        return decoded.IsSuccess
            && decoded.Datagram!.Records.Any(record => (record.Flags & ControlBit) == ControlBit);
    }
}
