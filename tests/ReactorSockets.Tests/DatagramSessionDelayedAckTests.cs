using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The delayed acknowledgement (issue #487, ADR 0026): a Peer that has received a reliable record it
/// has not acknowledged, and has no Datagram going out for that Peer, sends one header-only Datagram on
/// the next Pump. Every test drives two real <see cref="DatagramSessionReactor"/>s on loopback that share
/// one <see cref="FakeTimeProvider"/>, and observes only Send outcomes, drained Events and Counters.
/// </summary>
public class DatagramSessionDelayedAckTests
{
    /// <summary>The fake time between two steps of the one-way scenario, one Pump of each end per step.</summary>
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(10);

    [Fact]
    public void Send_OneWayReliableFlowForFiveSeconds_EverySendReturnsSent()
    {
        // Arrange
        using OneWayScenario scenario = OneWayScenario.Connect();

        // Act
        scenario.Run();

        // Assert
        scenario.Outcomes.Should().AllSatisfy(outcome => outcome.Should().Be(SendOutcome.Sent));
    }

    [Fact]
    public void Pump_OneWayReliableFlowForFiveSeconds_TheReceiverDeliversEveryMessage()
    {
        // Arrange
        using OneWayScenario scenario = OneWayScenario.Connect();

        // Act
        scenario.Run();

        // Assert
        scenario.Delivered.Should().Be(OneWayScenario.Messages);
    }

    [Fact]
    public void Pump_OneWayReliableFlowForFiveSeconds_TheSenderRetransmitsNothing()
    {
        // Arrange
        using OneWayScenario scenario = OneWayScenario.Connect();

        // Act
        scenario.Run();

        // Assert
        scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).PacketsRetransmitted.Should().Be(
            0, "nothing was lost, so every record was acknowledged before its retransmit deadline");
    }

    [Fact]
    public void Pump_ReceiverHasNothingQueued_WritesAtMostOneDatagramPerPeerPerPump()
    {
        // Arrange
        using OneWayScenario scenario = OneWayScenario.Connect();
        List<ulong> writesPerPump = [];

        // Act — a burst of reliable Messages in both reliable modes, then Pumps of the receiver only.
        for (int i = 0; i < 40; i++)
        {
            DeliveryMode mode = i % 2 == 0 ? DeliveryMode.ReliableOrdered : DeliveryMode.ReliableUnordered;
            scenario.Dialler.Reactor.Send(scenario.DiallerPeer, mode, new byte[100]).Should().Be(SendOutcome.Sent);
        }

        scenario.Dialler.Reactor.Pump();
        for (int pump = 0; pump < 10; pump++)
        {
            ulong before = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites;
            scenario.Accepter.Reactor.Pump();
            scenario.Accepter.DrainAndDiscard();
            writesPerPump.Add(scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites - before);
        }

        // Assert
        writesPerPump.Should().AllSatisfy(writes => writes.Should().BeLessThanOrEqualTo(1));
    }

    [Fact]
    public void Pump_ReceiverHasNothingQueued_AcknowledgesOnThePumpAfterTheRecordArrived()
    {
        // Arrange
        using OneWayScenario scenario = OneWayScenario.Connect();
        ulong writesBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites;

        // Act
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, new byte[100])
            .Should().Be(SendOutcome.Sent);
        scenario.Dialler.Reactor.Pump();
        scenario.Accepter.PumpUntil(() => scenario.Accepter.DrainPayloads().Count == 1);
        ulong writesOnArrival = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites;
        scenario.Accepter.Reactor.Pump();

        // Assert
        (writesOnArrival - writesBefore).Should().Be(0, "the Pump that admitted the record leaves room for a reply to carry the ack");
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites.Should().Be(
            writesOnArrival + 1, "the next Pump sends the ack on its own");
    }

    [Fact]
    public void Pump_ReceiverRepliesBeforeItsNextPump_TheReplyCarriesTheAckAndNoAckOnlyDatagramIsSent()
    {
        // Arrange
        using OneWayScenario scenario = OneWayScenario.Connect();
        ulong writesBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites;

        // Act
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, new byte[100])
            .Should().Be(SendOutcome.Sent);
        scenario.Dialler.Reactor.Pump();
        scenario.Accepter.PumpUntil(() => scenario.Accepter.DrainPayloads().Count == 1);
        scenario.Accepter.Reactor.Send(scenario.AccepterPeer, DeliveryMode.Unreliable, [0x01]).Should().Be(SendOutcome.Sent);
        for (int pump = 0; pump < 3; pump++)
        {
            scenario.Accepter.Reactor.Pump();
        }

        // Assert
        (scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites - writesBefore).Should().Be(
            1, "the reply's Datagram carried the ack, so nothing was owed afterwards");
    }

    [Fact]
    public void Pump_IdleSessionForTenSendIdleIntervals_EachEndWritesAtMostAPingAndAnAckPerInterval()
    {
        // Arrange
        TimeSpan sendIdleInterval = TimeSpan.FromSeconds(1);
        using OneWayScenario scenario = OneWayScenario.Connect(sendIdleInterval);
        ulong diallerBefore = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).SocketWrites;
        ulong accepterBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites;
        const int intervals = 10;

        // Act
        int steps = (int)(intervals * sendIdleInterval / _step);
        for (int i = 0; i < steps; i++)
        {
            scenario.Step();
        }

        // Assert
        ulong diallerWrites = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).SocketWrites - diallerBefore;
        ulong accepterWrites = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).SocketWrites - accepterBefore;
        Math.Max(diallerWrites, accepterWrites).Should().BeLessThanOrEqualTo(
            2 * intervals, "an idle end sends its own Ping and at most one ack of the other end's Ping per send-idle interval");
    }

    /// <summary>
    /// The issue #487 probe: a dialler Sends one 100-byte <c>ReliableOrdered</c> Message every 10 ms
    /// for 5 s, with a 1 s send-idle interval, 64 000 retention bytes and a 4000-byte message limit,
    /// and the accepter Sends nothing. Each step advances the shared clock one step and Pumps each end
    /// once, the dialler first. The handshake runs on the same cadence, so the dialler's round-trip
    /// seed (ADR 0018) is the one a real 10 ms loop would measure.
    /// </summary>
    private sealed class OneWayScenario : IDisposable
    {
        /// <summary>How many Messages <see cref="Run"/> sends.</summary>
        public const int Messages = 500;

        private OneWayScenario(DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter)
        {
            Dialler = dialler;
            Accepter = accepter;
        }

        /// <summary>The end that dials and sends.</summary>
        public DatagramSessionReactorHarness Dialler { get; }

        /// <summary>The end that accepts and sends nothing of its own.</summary>
        public DatagramSessionReactorHarness Accepter { get; }

        /// <summary>The accepter as the dialler addresses it.</summary>
        public PeerId DiallerPeer { get; private set; }

        /// <summary>The dialler as the accepter addresses it.</summary>
        public PeerId AccepterPeer { get; private set; }

        /// <summary>The outcome of every Send <see cref="Run"/> made, in order.</summary>
        public List<SendOutcome> Outcomes { get; } = [];

        /// <summary>How many Messages the accepter has delivered.</summary>
        public int Delivered { get; private set; }

        /// <summary>Builds both ends on one shared clock and completes the handshake one step per round.</summary>
        public static OneWayScenario Connect(TimeSpan? sendIdleInterval = null)
        {
            FakeTimeProvider clock = new();
            TimeSpan interval = sendIdleInterval ?? TimeSpan.FromSeconds(1);
            OneWayScenario scenario = new(
                new DatagramSessionReactorHarness(
                    bindLoopback: false, timeProvider: clock, retentionBytes: 64_000, maxMessageSize: 4000,
                    sendIdleInterval: interval),
                new DatagramSessionReactorHarness(
                    timeProvider: clock, retentionBytes: 64_000, maxMessageSize: 4000, sendIdleInterval: interval));

            scenario.DiallerPeer = scenario.Dialler.Reactor.Connect(scenario.Accepter.Endpoint);
            bool diallerConnected = false;
            PeerId? accepterPeer = null;
            for (int round = 0; round < 200 && !(diallerConnected && accepterPeer is not null); round++)
            {
                clock.Advance(_step);
                scenario.Dialler.Reactor.Pump();
                diallerConnected |= scenario.Dialler.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);
                scenario.Accepter.Reactor.Pump();
                foreach ((SessionEventKind kind, PeerId peer, DisconnectReason _) in scenario.Accepter.DrainToList())
                {
                    if (kind == SessionEventKind.PeerConnected)
                    {
                        accepterPeer = peer;
                    }
                }
            }

            scenario.AccepterPeer = accepterPeer ?? throw new InvalidOperationException("The handshake did not complete.");
            return scenario;
        }

        /// <summary>Sends <see cref="Messages"/> Messages one step apart, then steps until the receiver has had time to deliver them.</summary>
        public void Run()
        {
            for (int i = 0; i < Messages; i++)
            {
                Outcomes.Add(Dialler.Reactor.Send(DiallerPeer, DeliveryMode.ReliableOrdered, new byte[100]));
                Step();
            }

            for (int i = 0; i < 20; i++)
            {
                Step();
            }
        }

        /// <summary>Advances the shared clock one step and Pumps the dialler, then the accepter, once each.</summary>
        public void Step()
        {
            Dialler.TimeProvider.Advance(_step);
            Dialler.Reactor.Pump();
            Dialler.DrainAndDiscard();
            Accepter.Reactor.Pump();
            Delivered += Accepter.DrainPayloads().Count;
        }

        /// <summary>Disposes both ends.</summary>
        public void Dispose()
        {
            Dialler.Dispose();
            Accepter.Dispose();
        }
    }
}
