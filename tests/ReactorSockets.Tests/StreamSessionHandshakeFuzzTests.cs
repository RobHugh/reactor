using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The stream handshake parser is total: whatever record a stranger writes, no exception escapes
/// <see cref="StreamSessionReactor.Pump"/>, either on receipt or on any later Pump that uses a value
/// the handshake accepted. Plain sockets play the hostile counterparty on each side of the handshake
/// and each writes one seeded record: a Hello whose announced fields are drawn by
/// <see cref="HandshakeFuzzValues"/>, a Refusal or Resume with fuzzed fields, noise, or a truncated
/// record followed by a close.
/// </summary>
public class StreamSessionHandshakeFuzzTests
{
    /// <summary>How many hostile connections each test makes.</summary>
    private const int Connections = 64;

    /// <summary>
    /// The fixed seed for every corpus: 20261107, the date this test was written, chosen only so a
    /// failure is reproducible. Never re-rolled to make a failing case pass.
    /// </summary>
    private const int Seed = 20261107;

    /// <summary>
    /// The clock steps taken after the corpus, so every timer driven by an accepted announcement is
    /// evaluated against elapsed time from a millisecond up to beyond the largest legal interval.
    /// </summary>
    private static readonly TimeSpan[] _clockSteps =
    [
        TimeSpan.FromMilliseconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromHours(1),
        TimeSpan.FromDays(1),
        TimeSpan.FromDays(30),
        TimeSpan.FromDays(50),
    ];

    [Fact]
    public void Pump_FuzzedHandshakeRecordsToAnAccepter_NeverThrowAndTheReactorStaysUsable()
    {
        // Arrange. The largest heartbeat count, so an accepted interval meets the largest multiplier.
        using StreamSessionReactorHarness accepter = new(capacity: Connections, missedHeartbeats: int.MaxValue);
        Random random = new(Seed);
        List<Socket> hostiles = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act
        Action fuzz = () =>
        {
            for (int i = 0; i < Connections; i++)
            {
                Socket hostile = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                hostiles.Add(hostile);
                hostile.Connect(accepter.Endpoint);
                SendFuzzedRecord(random, hostile);
            }

            events.AddRange(PumpUntilEveryCounterpartyIsAnswered(accepter.Reactor.Pump, accepter.DrainToList, hostiles));
            events.AddRange(AdvanceThroughClockSteps(accepter));
            DisposeAll(hostiles);
            events.AddRange(AdvanceThroughClockSteps(accepter));
        };

        // Assert. Nothing throws, and some record was legal enough to establish a Peer.
        fuzz.Should().NotThrow();
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);

        // Assert. The Reactor still admits a genuine dial.
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents, _) =
            StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        diallerEvents.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_FuzzedHandshakeRecordsToADialler_NeverThrowAndTheReactorStaysUsable()
    {
        // Arrange. One Slot beyond the dials, kept free for the genuine dial at the end.
        using StreamSessionReactorHarness dialler = new(capacity: Connections + 1, bindLoopback: false, missedHeartbeats: int.MaxValue);
        using Socket listener = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(Connections);
        IPEndPoint listenerEndpoint = listener.LocalEndPoint as IPEndPoint
            ?? throw new InvalidOperationException("A bound IPv4 listener has an IPEndPoint.");
        Random random = new(Seed);
        List<Socket> hostiles = [];
        for (int i = 0; i < Connections; i++)
        {
            dialler.Reactor.Connect(listenerEndpoint);
        }

        PumpDeadline accepting = PumpDeadline.Start();
        while (hostiles.Count < Connections && !accepting.Expired)
        {
            dialler.Reactor.Pump();
            while (listener.Poll(0, SelectMode.SelectRead))
            {
                hostiles.Add(listener.Accept());
            }

            PumpDeadline.Yield();
        }

        hostiles.Should().HaveCount(Connections, "every dial reaches the listener");
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act
        Action fuzz = () =>
        {
            foreach (Socket hostile in hostiles)
            {
                SendFuzzedRecord(random, hostile);
            }

            events.AddRange(PumpUntilEveryDialEnds(dialler, Connections));
            events.AddRange(AdvanceThroughClockSteps(dialler));
            DisposeAll(hostiles);
            events.AddRange(AdvanceThroughClockSteps(dialler));
        };

        // Assert. Nothing throws, and the corpus reached both the establish and the close path.
        fuzz.Should().NotThrow();
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerClosed);

        // Assert. The Reactor still completes a genuine dial.
        using StreamSessionReactorHarness accepter = new();
        dialler.Reactor.Connect(accepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents, _) =
            StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        diallerEvents.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    /// <summary>
    /// Writes one fuzzed handshake record to <paramref name="socket"/>: mostly a Hello, otherwise a
    /// Refusal, a Resume, noise, or a truncated record followed by a close of the sending direction.
    /// One record in five has a few bytes overwritten after it is built.
    /// </summary>
    private static void SendFuzzedRecord(Random random, Socket socket)
    {
        byte[] record = random.Next(10) switch
        {
            < 6 => BuildFuzzedHello(random),
            6 => StreamSessionTestRecords.BuildRefusal(
                HandshakeFuzzValues.NextRefusalReason(random),
                HandshakeFuzzValues.NextVersion(random),
                HandshakeFuzzValues.NextVersion(random)),
            7 => StreamSessionTestRecords.BuildResume(RandomSecret(random), (ulong)random.NextInt64() | ((ulong)random.Next(2) << 63)),
            _ => RandomBytes(random, StreamSessionTestRecords.HandshakeRecordSize),
        };

        if (random.Next(5) == 0)
        {
            record = HandshakeFuzzValues.Mutate(random, record);
        }

        if (random.Next(10) == 0)
        {
            socket.Send(record.AsSpan(0, random.Next(record.Length)));
            socket.Shutdown(SocketShutdown.Send);
            return;
        }

        socket.Send(record);
    }

    private static byte[] BuildFuzzedHello(Random random) =>
        StreamSessionTestRecords.BuildHello(
            version: HandshakeFuzzValues.NextVersion(random),
            maxMessageSize: HandshakeFuzzValues.NextMaxMessageSize(random),
            sendIdleMs: HandshakeFuzzValues.NextInterval(random),
            lingerMs: HandshakeFuzzValues.NextInterval(random),
            resumeSecret: RandomSecret(random));

    private static byte[] RandomSecret(Random random) => RandomBytes(random, StreamSessionTestRecords.ResumeSecretLength);

    private static byte[] RandomBytes(Random random, int length)
    {
        byte[] bytes = new byte[length];
        random.NextBytes(bytes);
        return bytes;
    }

    /// <summary>
    /// Pumps until every counterparty has something to read or has been closed, which is how the
    /// accepting end answers every record: with its own Hello, a Refusal, or a close. Returns at the
    /// deadline without failing, because the test's own assertions say what had to happen.
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpUntilEveryCounterpartyIsAnswered(
        Action pump,
        Func<List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>> drain,
        List<Socket> counterparties)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        PumpDeadline deadline = PumpDeadline.Start(TimeSpan.FromSeconds(5));
        while (!deadline.Expired)
        {
            pump();
            events.AddRange(drain());
            if (counterparties.All(socket => socket.Poll(0, SelectMode.SelectRead)))
            {
                break;
            }

            PumpDeadline.Yield();
        }

        events.AddRange(drain());
        return events;
    }

    /// <summary>
    /// Pumps until <paramref name="dials"/> dials have each been established or closed. Returns at the
    /// deadline without failing, because the test's own assertions say what had to happen.
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpUntilEveryDialEnds(
        StreamSessionReactorHarness dialler, int dials)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        PumpDeadline deadline = PumpDeadline.Start(TimeSpan.FromSeconds(5));
        while (!deadline.Expired)
        {
            dialler.Reactor.Pump();
            events.AddRange(dialler.DrainToList());
            if (events.Count(evt => evt.Kind is SessionEventKind.PeerConnected or SessionEventKind.PeerClosed) >= dials)
            {
                break;
            }

            PumpDeadline.Yield();
        }

        return events;
    }

    /// <summary>Advances the clock through <see cref="_clockSteps"/>, Pumping after each step.</summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AdvanceThroughClockSteps(
        StreamSessionReactorHarness harness)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        foreach (TimeSpan step in _clockSteps)
        {
            harness.TimeProvider.Advance(step);
            events.AddRange(harness.PumpAndDrainAll(2));
        }

        return events;
    }

    private static void DisposeAll(List<Socket> sockets)
    {
        foreach (Socket socket in sockets)
        {
            socket.Dispose();
        }
    }
}
