using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Logging.Tests;

/// <summary>
/// One Reactor of each profile, each installed with its own <see cref="LoggerSink"/> over a logger from
/// one factory — the setup the package README recommends. The adapter is one type across all three
/// profiles, the factory makes one stream, and the category names each record's emitter, including a
/// record whose subject names no Reactor. Every record is provoked through the public surface, as in
/// ReactorSockets.Tests.
/// </summary>
public sealed class LoggerSinkStreamTests : IDisposable
{
    private const string RawCategory = "Reactor.Raw";
    private const string DatagramCategory = "Reactor.DatagramSession";
    private const string StreamCategory = "Reactor.StreamSession";

    // Matches ReactorSockets.Tests' PumpDeadline: a loopback handshake occasionally takes about a
    // millisecond on Windows, and the loop returns the moment every record has arrived.
    private static readonly TimeSpan _budget = TimeSpan.FromSeconds(2);

    private readonly RecordingLoggerFactory _factory = new();
    private readonly FakeTimeProvider _time = new();
    private readonly Socket _counterparty = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
    private readonly RawReactor _raw;
    private readonly DatagramSessionReactor _datagramDialler;
    private readonly DatagramSessionReactor _datagramAccepter;
    private readonly StreamSessionReactor _streamDialler;
    private readonly StreamSessionReactor _streamAccepter;

    /// <summary>
    /// Builds the three Reactors under test, each with a Sink over its own category, and two accepters
    /// with no Sink, so every entry in the stream comes from one of the three.
    /// </summary>
    public LoggerSinkStreamTests()
    {
        _raw = new RawReactor(ReactorOptions(Sink(RawCategory)), scratchBufferSize: 16);
        _datagramDialler = new DatagramSessionReactor(DatagramOptions(Sink(DatagramCategory), bindLoopback: false));
        _datagramAccepter = new DatagramSessionReactor(DatagramOptions(sink: null, bindLoopback: true));
        _streamDialler = new StreamSessionReactor(StreamOptions(Sink(StreamCategory), bindLoopback: false));
        _streamAccepter = new StreamSessionReactor(StreamOptions(sink: null, bindLoopback: true));
        _counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
    }

    [Fact]
    public void Log_OneReactorOfEachProfileWithASinkPerReactorFromOneFactory_ProducesOneStreamWhoseEveryRecordNamesItsEmitter()
    {
        // Arrange — a datagram larger than Raw's 16-byte scratch buffer logs DatagramTruncated, whose
        // Source subject names no Reactor; each Session dialler logs LingerNegotiated when it establishes.
        LinkId rawLink = _raw.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _counterparty.SendTo(new byte[100], _raw.LocalOf(rawLink));
        _datagramDialler.Connect(_datagramAccepter.BoundEndpoints[0]);
        _streamDialler.Connect(_streamAccepter.BoundEndpoints[0]);
        Dictionary<string, DiagnosticId> provoked = new()
        {
            [RawCategory] = DiagnosticId.DatagramTruncated,
            [DatagramCategory] = DiagnosticId.LingerNegotiated,
            [StreamCategory] = DiagnosticId.LingerNegotiated,
        };

        // Act
        PumpAllUntil(() => provoked.Keys.All(category => _factory.Entries.Exists(entry => entry.Category == category)));

        // Assert — every entry in the one stream carries the category of a Reactor provoked into
        // exactly that condition, and all three Reactors appear.
        _factory.Entries.Should().AllSatisfy(entry =>
            entry.Fields["DiagnosticId"].Should().Be(provoked.GetValueOrDefault(entry.Category)));
        _factory.Entries.Select(entry => entry.Category).Distinct().Should().BeEquivalentTo(provoked.Keys);
    }

    /// <summary>Disposes every Reactor and the counterparty socket.</summary>
    public void Dispose()
    {
        _streamAccepter.Dispose();
        _streamDialler.Dispose();
        _datagramAccepter.Dispose();
        _datagramDialler.Dispose();
        _raw.Dispose();
        _counterparty.Dispose();
    }

    /// <summary>A Sink over a logger of <paramref name="category"/> from the one shared factory.</summary>
    private LoggerSink Sink(string category) => new(_factory.CreateLogger(category));

    /// <summary>
    /// Pumps every Reactor in turn until <paramref name="condition"/> holds or the real-time budget is
    /// spent: a datagram's arrival and a loopback handshake both resolve inside the platform's stack,
    /// which a Pump count cannot wait on.
    /// </summary>
    private void PumpAllUntil(Func<bool> condition)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!condition() && elapsed.Elapsed < _budget)
        {
            _raw.Pump();
            foreach (RawEvent evt in _raw.Drain())
            {
                _ = evt.Kind;
            }

            _datagramDialler.Pump();
            Discard(_datagramDialler.Drain());
            _datagramAccepter.Pump();
            Discard(_datagramAccepter.Drain());
            _streamDialler.Pump();
            Discard(_streamDialler.Drain());
            _streamAccepter.Pump();
            Discard(_streamAccepter.Drain());
            Thread.Sleep(1);
        }
    }

    /// <summary>Drains one Session Reactor's Frame, keeping nothing.</summary>
    private static void Discard(SessionEventEnumerator events)
    {
        foreach (SessionEvent evt in events)
        {
            _ = evt.Kind;
        }
    }

    /// <summary>Reactor options shared by all five Reactors, with diagnostics on only when a Sink is given.</summary>
    private ReactorOptions ReactorOptions(IReactorDiagnostics? sink) => new()
    {
        Capacity = 4,
        TimeProvider = _time,
        Sink = sink,
        MinimumLevel = sink is null ? null : DiagnosticLevel.Info,
    };

    /// <summary>Datagram Session options, bound to loopback for the accepter and unbound for the dialler.</summary>
    private DatagramSessionOptions DatagramOptions(IReactorDiagnostics? sink, bool bindLoopback) => new()
    {
        Reactor = ReactorOptions(sink),
        MaxMessageSize = 4096,
        RetentionBytes = 4096,
        MaxPeersPerAddress = 1,
        SendIdleInterval = TimeSpan.FromHours(1),
        Linger = TimeSpan.FromSeconds(1),
        Endpoints = bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : [],
    };

    /// <summary>
    /// Stream Session options, bound as <see cref="DatagramOptions"/> is. Retention must hold one
    /// Message plus its length prefix, so it is twice the Message size here.
    /// </summary>
    private StreamSessionOptions StreamOptions(IReactorDiagnostics? sink, bool bindLoopback) => new()
    {
        Reactor = ReactorOptions(sink),
        MaxMessageSize = 4096,
        RetentionBytes = 8192,
        SendIdleInterval = TimeSpan.FromHours(1),
        Linger = TimeSpan.FromSeconds(1),
        Endpoints = bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : [],
    };
}
