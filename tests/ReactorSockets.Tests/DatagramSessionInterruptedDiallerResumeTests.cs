using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #382 (parent #56): while a dialled datagram Peer is Interrupted it sends a Resume control
/// datagram to the accepting end's address once per send-idle interval, carrying the accepting end's
/// session id and the secret the accepting end issued. Only the dialling end sends Resume, and only
/// while Interrupted.
/// </summary>
/// <remarks>
/// Drives a real <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on a shared
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>. The proxy records every arrival before
/// any drop rule runs, so cutting the path with <see cref="LossyProxy.StopRelayingBothDirections"/> still
/// leaves every datagram either end emits in <see cref="LossyProxy.Captures"/>. The wire layout is read
/// here by hand, never through the internal codec (ADR 0015), so a Resume the dialler writes is compared
/// with the Accept the accepter wrote, byte for byte, by parsing done independently of both writers. Every
/// Pump drains its Events, since a Pump clears any still-undrained ones when it starts.
/// </remarks>
public class DatagramSessionInterruptedDiallerResumeTests
{
    private const int ControlTypeOffset = 4;
    private const int ControlBodyOffset = 5;
    private const byte AcceptType = 4;
    private const byte ResumeType = 6;
    private const int AcceptSessionIdOffset = ControlBodyOffset + sizeof(ulong);
    private const int AcceptSecretOffset = ControlBodyOffset + sizeof(ulong) + sizeof(uint) + sizeof(uint) + sizeof(ulong) + sizeof(ulong);
    private const int ResumeSessionIdOffset = ControlBodyOffset;
    private const int ResumeSecretOffset = ControlBodyOffset + sizeof(uint);
    private const int SecretLength = 16;
    private const int MissedHeartbeats = 3;

    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    /// <summary>Far longer than every clock advance this file makes, so nothing here is a Linger expiry.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    [Fact]
    public void Pump_InterruptedDialledPeer_SendsOneResumePerSendIdleInterval()
    {
        // Arrange
        using Fixture fixture = new();
        PeerId peer = fixture.Connect();
        fixture.Proxy.StopRelayingBothDirections();
        fixture.StepUntilDiallerInterrupted(peer);
        int resumesAtInterruption = fixture.DiallerResumes().Count;

        // Act
        const int intervals = 5;
        for (int i = 0; i < intervals; i++)
        {
            fixture.Step(_sendIdleInterval, pumpAccepter: false);
        }

        // Assert
        resumesAtInterruption.Should().Be(1, "the first Resume leaves on the Pump that finds the Peer Interrupted");
        fixture.DiallerResumes().Should().HaveCount(
            1 + intervals, "one Resume goes out per send-idle interval while the Peer stays Interrupted");
    }

    [Fact]
    public void Pump_InterruptedDialledPeer_ResumeCarriesAccepterSessionIdAndIssuedSecret()
    {
        // Arrange
        using Fixture fixture = new();
        PeerId peer = fixture.Connect();
        byte[] accept = fixture.Proxy.Captures
            .Single(c => c.Direction == RelayDirection.UpstreamToClient && c.Payload[ControlTypeOffset] == AcceptType && IsControl(c.Payload))
            .Payload;
        uint issuedSessionId = BinaryPrimitives.ReadUInt32BigEndian(accept.AsSpan(AcceptSessionIdOffset));
        byte[] issuedSecret = accept.AsSpan(AcceptSecretOffset, SecretLength).ToArray();

        // Act
        fixture.Proxy.StopRelayingBothDirections();
        fixture.StepUntilDiallerInterrupted(peer);
        fixture.Step(_sendIdleInterval, pumpAccepter: false);

        // Assert
        List<byte[]> resumes = fixture.DiallerResumes();
        resumes.Should().HaveCountGreaterThan(1);
        issuedSecret.Should().NotBeEquivalentTo(new byte[SecretLength], "the Accept carries a real, nonzero secret");
        foreach (byte[] resume in resumes)
        {
            BinaryPrimitives.ReadUInt32BigEndian(resume.AsSpan(ResumeSessionIdOffset)).Should().Be(issuedSessionId);
            resume.AsSpan(ResumeSecretOffset, SecretLength).ToArray().Should().Equal(issuedSecret);
        }
    }

    [Fact]
    public void Pump_InterruptedAcceptingPeer_SendsNoResume()
    {
        // Arrange
        using Fixture fixture = new();
        fixture.Connect();
        fixture.Proxy.StopRelayingBothDirections();

        // Act
        bool accepterInterrupted = false;
        for (int i = 0; i < MissedHeartbeats + 4; i++)
        {
            accepterInterrupted |= fixture.Step(_sendIdleInterval, pumpAccepter: true)
                .Any(e => e.Kind == SessionEventKind.PeerInterrupted);
        }

        // Assert
        accepterInterrupted.Should().BeTrue("the accepting end's own receive-idle deadline was crossed");
        fixture.Proxy.Captures
            .Where(c => c.Direction == RelayDirection.UpstreamToClient && IsControl(c.Payload) && c.Payload[ControlTypeOffset] == ResumeType)
            .Should().BeEmpty("the accepting end never sends Resume");
    }

    [Fact]
    public void Pump_DialledPeerNotInterrupted_SendsNoResume()
    {
        // Arrange
        using Fixture fixture = new();
        fixture.Connect();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act — the path stays live, so both ends' Pings keep the receive-idle deadline from being crossed.
        for (int i = 0; i < 2 * MissedHeartbeats + 2; i++)
        {
            fixture.Step(_sendIdleInterval, pumpAccepter: true, relayRounds: 3);
            events.AddRange(fixture.DiallerEvents);
        }

        // Assert
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted, "the path is live");
        fixture.Proxy.Captures.Count(c => c.Direction == RelayDirection.ClientToUpstream).Should().BeGreaterThan(
            MissedHeartbeats, "the dialler kept sending, so an absent Resume is not an absent dialler");
        fixture.DiallerResumes().Should().BeEmpty("a dialled Peer that is not Interrupted sends no Resume");
    }

    private static bool IsControl(byte[] payload) =>
        payload.Length > ControlTypeOffset && BinaryPrimitives.ReadUInt32BigEndian(payload) == 0;

    /// <summary>A dialler, a proxy and an accepter sharing one clock.</summary>
    private sealed class Fixture : IDisposable
    {
        private readonly DatagramSessionReactorHarness _dialler;
        private readonly DatagramSessionReactorHarness _accepter;

        public Fixture()
        {
            _dialler = new(bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
            _accepter = new(
                sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats,
                timeProvider: _dialler.TimeProvider);
            Proxy = new LossyProxy(_accepter.Endpoint, _dialler.TimeProvider);
        }

        public LossyProxy Proxy { get; }

        /// <summary>The dialler's events drained by the latest <see cref="Step"/>.</summary>
        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DiallerEvents { get; private set; } = [];

        public PeerId Connect()
        {
            PeerId peer = _dialler.Reactor.Connect(Proxy.PublicEndPoint);
            DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(_dialler, Proxy, _accepter);
            return peer;
        }

        /// <summary>
        /// Advances the shared clock by <paramref name="interval"/>, Pumps the dialler (and optionally the
        /// accepter) draining after each Pump, and lets the proxy capture what they sent. Returns the
        /// accepter's drained events.
        /// </summary>
        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Step(
            TimeSpan interval, bool pumpAccepter, int relayRounds = 1)
        {
            _dialler.TimeProvider.Advance(interval);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];
            DiallerEvents = [];
            for (int round = 0; round < relayRounds; round++)
            {
                _dialler.Reactor.Pump();
                DiallerEvents.AddRange(_dialler.DrainToList());
                Proxy.Pump();
                if (pumpAccepter)
                {
                    _accepter.Reactor.Pump();
                    accepterEvents.AddRange(_accepter.DrainToList());
                    Proxy.Pump();
                }
            }

            // Loopback delivery is real UDP, so a datagram sent this Step may not have reached the
            // proxy's socket by its first Pump; give it a few more so the capture is complete.
            for (int settle = 0; settle < 10; settle++)
            {
                Thread.Sleep(2);
                Proxy.Pump();
            }

            return accepterEvents;
        }

        public void StepUntilDiallerInterrupted(PeerId peer)
        {
            for (int i = 0; i < MissedHeartbeats + 2; i++)
            {
                Step(_sendIdleInterval, pumpAccepter: false);
                if (DiallerEvents.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer)))
                {
                    return;
                }
            }

            throw new InvalidOperationException("The dialled Peer never became Interrupted.");
        }

        public List<byte[]> DiallerResumes() =>
            [.. Proxy.Captures
                .Where(c => c.Direction == RelayDirection.ClientToUpstream && IsControl(c.Payload) && c.Payload[ControlTypeOffset] == ResumeType)
                .Select(c => c.Payload)];

        public void Dispose()
        {
            Proxy.Dispose();
            _accepter.Dispose();
            _dialler.Dispose();
        }
    }
}
