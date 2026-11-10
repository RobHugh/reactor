using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// On the stream profile, a Hello's announced send-idle interval and Linger are a <c>ulong</c> off
/// the wire, and an out-of-range one must not throw out of <see cref="StreamSessionReactor.Pump"/>. Driven from outside the L2 seam (tests/CLAUDE.md) by plain
/// sockets writing a hand-built record.
/// </summary>
public class StreamSessionHostileAnnouncedIntervalTests
{
    private const ulong LegalInterval = 60_000;

    /// <summary>The largest legal announcement: the largest count of milliseconds 32 bits can hold.</summary>
    private const ulong MaximumLegalInterval = uint.MaxValue;

    /// <summary>Inside the window that used to survive the handshake and then overflow every Pump.</summary>
    private const ulong SurvivesConversionOverflowsDeadline = 600_000_000_000_000;

    /// <summary>Each illegal value in each announced field, with the other field legal.</summary>
    public static TheoryData<ulong, ulong> IllegalAnnouncements => new()
    {
        { 0, LegalInterval },
        { LegalInterval, 0 },
        { MaximumLegalInterval + 1, LegalInterval },
        { LegalInterval, MaximumLegalInterval + 1 },
        { SurvivesConversionOverflowsDeadline, LegalInterval },
        { LegalInterval, SurvivesConversionOverflowsDeadline },
        { ulong.MaxValue, LegalInterval },
        { LegalInterval, ulong.MaxValue },
    };

    [Theory]
    [MemberData(nameof(IllegalAnnouncements))]
    public void Pump_HelloAnnouncingAnIllegalInterval_DiscardsItUnansweredAndKeepsPumping(
        ulong sendIdleIntervalMilliseconds, ulong lingerMilliseconds)
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using Socket hostile = ConnectTo(accepter.Endpoint);

        // Act
        hostile.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: sendIdleIntervalMilliseconds, lingerMs: lingerMilliseconds));
        (int bytesAnswered, List<SessionEventKind> events) = PumpUntilClosedBy(accepter, hostile);
        Action pump = accepter.Reactor.Pump;

        // Assert — no Hello came back, and no Peer was ever raised or closed.
        pump.Should().NotThrow();
        bytesAnswered.Should().Be(0);
        events.Should().BeEmpty();
    }

    [Fact]
    public void Pump_HelloAnnouncingAnIllegalIntervalToAFullReactor_IsNotAnsweredWithARefusal()
    {
        // Arrange — a capacity Refusal would tell the sender its Hello had been understood.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        accepter.FillToCapacity();
        using Socket hostile = ConnectTo(accepter.Endpoint);

        // Act
        hostile.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: ulong.MaxValue, lingerMs: LegalInterval));
        (int bytesAnswered, _) = PumpUntilClosedBy(accepter, hostile);

        // Assert
        bytesAnswered.Should().Be(0);
    }

    [Theory]
    [MemberData(nameof(IllegalAnnouncements))]
    public void Pump_HelloAnnouncingAnIllegalIntervalToADialler_ClosesThePeerWithoutEstablishingIt(
        ulong sendIdleIntervalMilliseconds, ulong lingerMilliseconds)
    {
        // Arrange
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        using Socket listener = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(1);
        IPEndPoint listenerEndpoint = listener.LocalEndPoint as IPEndPoint
            ?? throw new InvalidOperationException("A bound IPv4 listener has an IPEndPoint.");
        dialler.Reactor.Connect(listenerEndpoint);
        dialler.PumpUntil(() => listener.Poll(0, SelectMode.SelectRead));
        using Socket hostile = listener.Accept();

        // Act
        hostile.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: sendIdleIntervalMilliseconds, lingerMs: lingerMilliseconds));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        Action pump = () =>
        {
            events = dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));
            dialler.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerClosed && evt.Reason == DisconnectReason.TransportFailed);
    }

    [Fact]
    public void Pump_HelloOfAnotherVersionAnnouncingAnIllegalInterval_IsStillRefusedForItsVersion()
    {
        // Arrange — this end cannot judge the fields of a version it does not speak, and the sender can
        // act on a version Refusal.
        using StreamSessionReactorHarness accepter = new();
        using Socket counterparty = ConnectTo(accepter.Endpoint);

        // Act
        counterparty.Send(StreamSessionTestRecords.BuildHello(version: 2, sendIdleMs: ulong.MaxValue, lingerMs: ulong.MaxValue));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));

        // Assert
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerClosed && evt.Reason == DisconnectReason.VersionMismatch);
    }

    [Fact]
    public void Pump_AfterDiscardingAHelloWithAnIllegalInterval_StillAdmitsAGenuineDial()
    {
        // Arrange — one Slot, so a discard that wrongly kept the Slot would refuse the genuine dial.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using Socket hostile = ConnectTo(accepter.Endpoint);
        hostile.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: SurvivesConversionOverflowsDeadline, lingerMs: LegalInterval));
        PumpUntilClosedBy(accepter, hostile);
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_HelloAnnouncingTheMaximumAgainstTheLargestHeartbeatCount_EstablishesAndKeepsPumping()
    {
        // Arrange — the largest legal announcement meets the largest count a consumer can configure, so
        // the receive-idle deadline the two imply is far outside what a TimeSpan can represent.
        using StreamSessionReactorHarness accepter = new(missedHeartbeats: int.MaxValue);
        using Socket counterparty = ConnectTo(accepter.Endpoint);

        // Act
        counterparty.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: MaximumLegalInterval, lingerMs: MaximumLegalInterval));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        Action pump = () =>
        {
            events = accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));
            accepter.TimeProvider.Advance(TimeSpan.FromDays(1));
            accepter.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
        accepter.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerInterrupted);
    }

    private static Socket ConnectTo(IPEndPoint destination)
    {
        Socket socket = new(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        socket.Connect(destination);
        return socket;
    }

    /// <summary>
    /// Pumps <paramref name="reactor"/> until it closes <paramref name="counterparty"/>'s connection,
    /// returning how many bytes it wrote first and every Event it raised along the way.
    /// </summary>
    private static (int BytesAnswered, List<SessionEventKind> Events) PumpUntilClosedBy(
        StreamSessionReactorHarness reactor, Socket counterparty)
    {
        int bytesAnswered = 0;
        List<SessionEventKind> events = [];
        byte[] buffer = new byte[256];
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            reactor.Reactor.Pump();
            events.AddRange(reactor.DrainToList().Select(evt => evt.Kind));
            if (counterparty.Poll(0, SelectMode.SelectRead))
            {
                int read = counterparty.Receive(buffer);
                if (read == 0)
                {
                    return (bytesAnswered, events);
                }

                bytesAnswered += read;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The Reactor did not close the connection within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }
}
