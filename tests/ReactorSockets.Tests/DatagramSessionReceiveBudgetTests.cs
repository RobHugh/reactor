using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #428: one Pump reads at most <see cref="DatagramSessionOptions.MaxDatagramsPerPump"/> datagrams
/// from each bound socket and leaves the rest for a later Pump, so a flood cannot keep a Pump from
/// returning. Each flood is one-byte datagrams, too short to name a Session, which are read, counted and
/// discarded without any other effect. The short sleep before a measured Pump gives loopback delivery time
/// to queue every send, as <see cref="DatagramSessionRefusalTests"/> does.
/// </summary>
public class DatagramSessionReceiveBudgetTests
{
    [Fact]
    public void Pump_MoreDatagramsQueuedThanTheBudget_ReadsOnlyTheBudget()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(maxDatagramsPerPump: 3);
        using Socket flooder = NewFlooder();
        Flood(flooder, accepter.Endpoint, datagrams: 10);
        Thread.Sleep(20);

        // Act
        accepter.Reactor.Pump();

        // Assert
        accepter.Reactor.Counters.DatagramsReceived.Should().Be(3);
    }

    [Fact]
    public void Pump_DatagramsLeftByAnEarlierPump_AreReadByLaterPumps()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(maxDatagramsPerPump: 3);
        using Socket flooder = NewFlooder();
        Flood(flooder, accepter.Endpoint, datagrams: 10);
        Thread.Sleep(20);

        // Act
        for (int pump = 0; pump < 4; pump++)
        {
            accepter.Reactor.Pump();
        }

        // Assert
        accepter.Reactor.Counters.DatagramsReceived.Should().Be(10);
    }

    [Fact]
    public void Pump_TwoBoundSockets_GivesEachItsOwnBudget()
    {
        // Arrange — a flood on one socket must not spend what the other socket may read.
        using DatagramSessionReactorHarness accepter = new(
            maxDatagramsPerPump: 3,
            endpoints: [new IPEndPoint(IPAddress.Loopback, 0), new IPEndPoint(IPAddress.Parse("127.0.0.2"), 0)]);
        using Socket flooder = NewFlooder();
        Flood(flooder, accepter.Reactor.BoundEndpoints[0], datagrams: 10);
        Flood(flooder, accepter.Reactor.BoundEndpoints[1], datagrams: 10);
        Thread.Sleep(20);

        // Act
        accepter.Reactor.Pump();

        // Assert
        accepter.Reactor.Counters.DatagramsReceived.Should().Be(6);
    }

    [Fact]
    public void Pump_FloodQueuedAheadOfADial_StillCompletesTheHandshake()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(maxDatagramsPerPump: 2);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using Socket flooder = NewFlooder();
        Flood(flooder, accepter.Endpoint, datagrams: 50);
        Thread.Sleep(20);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert
        diallerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
    }

    [Fact]
    public void Pump_ReadingTheWholeBudget_LogsDatagramBudgetReachedCarryingTheLimit()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(maxDatagramsPerPump: 3);
        using Socket flooder = NewFlooder();
        Flood(flooder, accepter.Endpoint, datagrams: 10);
        Thread.Sleep(20);

        // Act
        accepter.Reactor.Pump();

        // Assert
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DatagramBudgetReached).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.None);
        record.Value0.Should().Be(3);
    }

    [Fact]
    public void Pump_ReadingLessThanTheBudget_LogsNoDatagramBudgetReached()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(maxDatagramsPerPump: 3);
        using Socket flooder = NewFlooder();
        Flood(flooder, accepter.Endpoint, datagrams: 2);
        Thread.Sleep(20);

        // Act
        accepter.Reactor.Pump();

        // Assert
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.DatagramBudgetReached);
    }

    private static Socket NewFlooder() => new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

    private static void Flood(Socket flooder, IPEndPoint destination, int datagrams)
    {
        byte[] tooShortToNameASession = [0];
        for (int i = 0; i < datagrams; i++)
        {
            flooder.SendTo(tooShortToNameASession, destination);
        }
    }
}
