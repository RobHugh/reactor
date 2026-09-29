using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #384 (parent #56): a third party that watches the wire learns the session field of a live
/// data datagram, but not the resume secret, and so cannot resume the Session (ADR 0009: the session
/// id routes, the secret authorises). The observer is a real <see cref="RawReactor"/> on its own
/// loopback port, so its datagrams come from an address that is not the Peer's.
/// </summary>
/// <remarks>
/// The observer's session id is read off a dialler-to-accepter data datagram the proxy captured, never
/// off the Accept, and the observer never reads the secret. Each test then acts on an Interrupted
/// accepting-end Peer. Were a Resume authorised by session id alone, the guessed-secret Resume would
/// match, and every test here would see PeerResumed, ResumeMatched or a moved interruption timestamp.
/// </remarks>
public class DatagramSessionObserverCannotResumeTests
{
    private const int SessionFieldLength = 4;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan _observerActsAfter = TimeSpan.FromMilliseconds(400);

    [Fact]
    public void Pump_ObservedSessionIdWithAGuessedSecretAgainstAnInterruptedPeer_LogsResumeUnmatchedNamingTheObserver()
    {
        // Arrange
        using Fixture fixture = Fixture.InterruptedAfterObserving();

        // Act
        fixture.Observer.Send(fixture.Scenario.Accepter.Endpoint, Scenario.BuildResume(fixture.ObservedSessionId, Fixture.GuessedSecret()));
        fixture.Scenario.PumpAccepterUntil(() => fixture.Scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        fixture.Scenario.PumpAccepter();

        // Assert
        DiagnosticRecord record = fixture.Scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.ToIPEndPoint().Should().Be(fixture.Observer.EndPoint);
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.Value0.Should().Be(1);
        record.Value1.Should().Be(6);
        fixture.Scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
        fixture.Scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
    }

    [Fact]
    public void Pump_ObservedSessionIdWithAGuessedSecretAgainstAnInterruptedPeer_LeavesResumesAtZero()
    {
        // Arrange
        using Fixture fixture = Fixture.InterruptedAfterObserving();

        // Act
        fixture.Observer.Send(fixture.Scenario.Accepter.Endpoint, Scenario.BuildResume(fixture.ObservedSessionId, Fixture.GuessedSecret()));
        fixture.Scenario.PumpAccepterUntil(() => fixture.Scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        fixture.Scenario.PumpAccepter();

        // Assert — Resumes reads 0 because the Resume was refused, so the Peer was not resumed either.
        fixture.Scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        fixture.Scenario.Accepter.Reactor.CountersOf(fixture.Scenario.AccepterPeer).Resumes.Should().Be(0);
    }

    [Fact]
    public void Pump_ObservedSessionIdOnAReplayedDataDatagramAgainstAnInterruptedPeer_DoesNotResumeIt()
    {
        // Arrange
        using Fixture fixture = Fixture.InterruptedAfterObserving();

        // Act
        fixture.Observer.Send(fixture.Scenario.Accepter.Endpoint, fixture.CapturedDataDatagram);
        fixture.PumpAccepterAFewTimes();

        // Assert
        fixture.Scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        fixture.Scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
        fixture.Scenario.Accepter.Reactor.CountersOf(fixture.Scenario.AccepterPeer).Resumes.Should().Be(0);
    }

    [Fact]
    public void Pump_ObservedSessionIdOnAHandBuiltDataDatagramAgainstAnInterruptedPeer_DoesNotResumeIt()
    {
        // Arrange — the observed session id followed by zeros, the same length as the captured datagram.
        using Fixture fixture = Fixture.InterruptedAfterObserving();
        byte[] handBuilt = new byte[fixture.CapturedDataDatagram.Length];
        BinaryPrimitives.WriteUInt32BigEndian(handBuilt, fixture.ObservedSessionId);

        // Act
        fixture.Observer.Send(fixture.Scenario.Accepter.Endpoint, handBuilt);
        fixture.PumpAccepterAFewTimes();

        // Assert
        fixture.Scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        fixture.Scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
        fixture.Scenario.Accepter.Reactor.CountersOf(fixture.Scenario.AccepterPeer).Resumes.Should().Be(0);
    }

    [Fact]
    public void Pump_ObserversResumeAndDataDatagramsAgainstAnInterruptedPeer_LeaveItInterruptedWithItsOriginalLingerExpiry()
    {
        // Arrange — Linger is 1s and the observer acts 400ms after the interruption, so a timestamp
        // moved by either datagram would push the close out to 1400ms.
        using Fixture fixture = Fixture.InterruptedAfterObserving(_linger);
        Scenario scenario = fixture.Scenario;
        scenario.Clock.Advance(_observerActsAfter);

        // Act
        fixture.Observer.Send(scenario.Accepter.Endpoint, Scenario.BuildResume(fixture.ObservedSessionId, Fixture.GuessedSecret()));
        fixture.Observer.Send(scenario.Accepter.Endpoint, fixture.CapturedDataDatagram);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        fixture.PumpAccepterAFewTimes();

        scenario.AdvanceAccepter(_linger - _observerActsAfter - TimeSpan.FromMilliseconds(1));
        scenario.PumpAccepter();
        bool closedBeforeTheOriginalExpiry = scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerClosed);

        scenario.Clock.Advance(TimeSpan.FromMilliseconds(1));
        scenario.PumpAccepter();

        // Assert
        closedBeforeTheOriginalExpiry.Should().BeFalse();
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
    }

    [Fact]
    public void Pump_ObserversResumeAndDataDatagramsAgainstAnInterruptedPeer_LeaveItsSendAddressUnchanged()
    {
        // Arrange — a long Linger so the Peer stays open across the send-idle intervals below.
        using Fixture fixture = Fixture.InterruptedAfterObserving(TimeSpan.FromSeconds(30));
        Scenario scenario = fixture.Scenario;

        // Act — both datagrams, then further send-idle intervals: the Interrupted Peer keeps pinging,
        // and where those Pings go is its send address.
        fixture.Observer.Send(scenario.Accepter.Endpoint, Scenario.BuildResume(fixture.ObservedSessionId, Fixture.GuessedSecret()));
        fixture.Observer.Send(scenario.Accepter.Endpoint, fixture.CapturedDataDatagram);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        fixture.PumpAccepterAFewTimes();

        int pingsToTheProxyBefore = scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);
        for (int i = 0; i < 3; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
            scenario.PumpAccepter();
            scenario.Proxy.Pump();
        }

        // Assert
        scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient)
            .Should().BeGreaterThan(pingsToTheProxyBefore, "the Peer's Pings still reach the address it was established from");
        fixture.Observer.ReceivedDatagramCount().Should().Be(0, "neither datagram made the observer's address the Peer's send address");
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
    }

    /// <summary>
    /// An established Scenario, an observer that has captured a live data datagram's session field, and
    /// the accepting end interrupted.
    /// </summary>
    private sealed class Fixture : IDisposable
    {
        private readonly RawReactorHarness _observerHarness = new();

        private Fixture(Scenario scenario, byte[] capturedDataDatagram)
        {
            Scenario = scenario;
            CapturedDataDatagram = capturedDataDatagram;
            ObservedSessionId = BinaryPrimitives.ReadUInt32BigEndian(capturedDataDatagram);
            (LinkId link, IPEndPoint endPoint) = _observerHarness.BindLoopbackLink();
            Observer = new ObserverSocket(_observerHarness, link, endPoint);
        }

        /// <summary>The connected, interrupted dialler and accepter.</summary>
        public Scenario Scenario { get; }

        /// <summary>The live dialler-to-accepter data datagram the observer captured before the interruption.</summary>
        public byte[] CapturedDataDatagram { get; }

        /// <summary>The session field of <see cref="CapturedDataDatagram"/>, which is all the observer knows of the Session.</summary>
        public uint ObservedSessionId { get; }

        /// <summary>The observer's sending socket.</summary>
        public ObserverSocket Observer { get; }

        /// <summary>Establishes a Scenario, lets the dialler's send-idle Pings cross the proxy, captures one, then interrupts the accepter.</summary>
        public static Fixture InterruptedAfterObserving(TimeSpan? linger = null)
        {
            Scenario scenario = Scenario.Establish(linger);
            try
            {
                for (int round = 0; round < 3; round++)
                {
                    scenario.Clock.Advance(Scenario.SendIdleInterval);
                    DatagramSessionReactorHarness.PumpRoundThroughProxy(scenario.Dialler, scenario.Proxy, scenario.Accepter);
                    scenario.Dialler.DrainToList();
                    scenario.AccepterEvents.AddRange(scenario.Accepter.DrainToList());
                }

                byte[] captured = scenario.Proxy.Captures
                    .Where(c => c.Direction == RelayDirection.ClientToUpstream
                        && c.Payload.Length > SessionFieldLength
                        && BinaryPrimitives.ReadUInt32BigEndian(c.Payload) != 0)
                    .Select(c => c.Payload)
                    .Last();
                scenario.AccepterEvents.Clear();
                scenario.InterruptAccepter();
                return new Fixture(scenario, captured);
            }
            catch
            {
                scenario.Dispose();
                throw;
            }
        }

        /// <summary>A secret the observer made up: not the issued one.</summary>
        public static byte[] GuessedSecret() => [.. Enumerable.Range(1, Scenario.ResumeSecretLength).Select(i => (byte)i)];

        /// <summary>Pumps the accepter a few times, draining after every Pump, so a datagram the observer sent has been handled.</summary>
        public void PumpAccepterAFewTimes()
        {
            for (int i = 0; i < 20; i++)
            {
                Scenario.PumpAccepter();
                Thread.Sleep(1);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _observerHarness.Dispose();
            Scenario.Dispose();
        }
    }

    /// <summary>A <see cref="RawReactor"/> bound to its own loopback port, standing in for a party that watches the wire.</summary>
    private sealed class ObserverSocket(RawReactorHarness harness, LinkId link, IPEndPoint endPoint)
    {
        /// <summary>The address the observer's datagrams come from.</summary>
        public IPEndPoint EndPoint { get; } = endPoint;

        /// <summary>Sends <paramref name="payload"/> to <paramref name="destination"/> from the observer's port.</summary>
        public void Send(IPEndPoint destination, byte[] payload) =>
            harness.Reactor.SendTo(link, Source.FromIPEndPoint(destination), payload).IsSuccess.Should().BeTrue();

        /// <summary>Pumps the observer and counts the datagrams that reached it.</summary>
        public int ReceivedDatagramCount()
        {
            harness.Reactor.Pump();
            int count = 0;
            foreach (RawEvent evt in harness.Reactor.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    count++;
                }
            }

            return count;
        }
    }
}
