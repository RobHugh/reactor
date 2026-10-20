using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Logging.Tests;

/// <summary>
/// One Reactor of each profile installed with the same <see cref="LoggerSink"/>: the reason the adapter
/// stays single is that a consumer running all three gets one log stream. Each record is provoked
/// through the public surface, as in ReactorSockets.Tests, and each carries a Link or Peer subject so
/// the Reactor that emitted it is named in the record itself.
/// </summary>
public sealed class LoggerSinkStreamTests : IDisposable
{
    // Windows retries a refused connect for about two seconds before reporting it, so the Raw record
    // arrives last there. Overshooting costs nothing: the loop returns once all three have logged.
    private static readonly TimeSpan _budget = TimeSpan.FromSeconds(10);

    private readonly RecordingLogger _logger = new();
    private readonly FakeTimeProvider _time = new();
    private readonly Socket _closedPort = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    private readonly RawReactor _raw;
    private readonly DatagramSessionReactor _datagramDialler;
    private readonly DatagramSessionReactor _datagramAccepter;
    private readonly StreamSessionReactor _streamDialler;
    private readonly StreamSessionReactor _streamAccepter;

    public LoggerSinkStreamTests()
    {
        LoggerSink sink = new(_logger);

        // Bound and never listening: a dial is refused, and the port stays held so nothing else in the
        // machine can start listening on it mid-test.
        _closedPort.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        _raw = new RawReactor(ReactorOptions(sink), scratchBufferSize: null);
        _datagramDialler = new DatagramSessionReactor(DatagramOptions(sink, bindLoopback: false));
        _datagramAccepter = new DatagramSessionReactor(DatagramOptions(sink: null, bindLoopback: true));
        _streamDialler = new StreamSessionReactor(StreamOptions(sink, bindLoopback: false));
        _streamAccepter = new StreamSessionReactor(StreamOptions(sink: null, bindLoopback: true));
    }

    [Fact]
    public void Log_OneReactorOfEachProfileSharingOneSink_ProducesOneStreamWhoseEveryRecordNamesItsEmitter()
    {
        // Arrange — a refused Raw dial logs LinkFailed; each Session dialler logs LingerNegotiated when
        // it establishes. The accepters carry no Sink, so every entry comes from one of three Reactors.
        // `!` works around a framework limitation: Socket.LocalEndPoint is declared nullable for an
        // unbound socket, and this one was bound in the constructor.
        LinkId rawLink = _raw.Connect((IPEndPoint)_closedPort.LocalEndPoint!);
        PeerId datagramPeer = _datagramDialler.Connect(_datagramAccepter.BoundEndpoints[0]);
        PeerId streamPeer = _streamDialler.Connect(_streamAccepter.BoundEndpoints[0]);
        Dictionary<uint, DiagnosticId> emitters = new()
        {
            [rawLink.Origin] = DiagnosticId.LinkFailed,
            [datagramPeer.Origin] = DiagnosticId.LingerNegotiated,
            [streamPeer.Origin] = DiagnosticId.LingerNegotiated,
        };

        // Act
        PumpAllUntil(() => emitters.Keys.All(origin => _logger.Entries.Exists(entry => Equals(entry.Fields.GetValueOrDefault("Reactor"), origin))));

        // Assert — every entry names one of the three Reactors and the condition that Reactor was
        // provoked into, and all three appear in the one stream.
        _logger.Entries.Should().AllSatisfy(entry =>
        {
            uint origin = entry.Fields.GetValueOrDefault("Reactor").Should().BeOfType<uint>().Subject;
            emitters.Should().ContainKey(origin);
            entry.Fields["DiagnosticId"].Should().Be(emitters[origin]);
            entry.Message.Should().Contain($"of reactor {origin}");
        });
        _logger.Entries.Select(entry => entry.Fields["Reactor"]).Distinct().Should().HaveCount(3);
    }

    public void Dispose()
    {
        _streamAccepter.Dispose();
        _streamDialler.Dispose();
        _datagramAccepter.Dispose();
        _datagramDialler.Dispose();
        _raw.Dispose();
        _closedPort.Dispose();
    }

    /// <summary>
    /// Pumps every Reactor in turn until <paramref name="condition"/> holds or the real-time budget is
    /// spent: a refused dial and a loopback handshake both resolve inside the platform's stack, which a
    /// Pump count cannot wait on.
    /// </summary>
    private void PumpAllUntil(Func<bool> condition)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!condition() && elapsed.Elapsed < _budget)
        {
            _raw.Pump();
            DrainRaw();
            _datagramDialler.Pump();
            DrainSession(_datagramDialler.Drain());
            _datagramAccepter.Pump();
            DrainSession(_datagramAccepter.Drain());
            _streamDialler.Pump();
            DrainSession(_streamDialler.Drain());
            _streamAccepter.Pump();
            DrainSession(_streamAccepter.Drain());
            Thread.Sleep(1);
        }
    }

    private void DrainRaw()
    {
        foreach (RawEvent evt in _raw.Drain())
        {
            _ = evt.Kind;
        }
    }

    private static void DrainSession(SessionEventEnumerator events)
    {
        foreach (SessionEvent evt in events)
        {
            _ = evt.Kind;
        }
    }

    private ReactorOptions ReactorOptions(IReactorDiagnostics? sink) => new()
    {
        Capacity = 4,
        TimeProvider = _time,
        Sink = sink,
        MinimumLevel = sink is null ? null : DiagnosticLevel.Info,
    };

    private DatagramSessionOptions DatagramOptions(IReactorDiagnostics? sink, bool bindLoopback) => new()
    {
        Reactor = ReactorOptions(sink),
        MaxMessageSize = 4096,
        RetentionBytes = 4096,
        SendIdleInterval = TimeSpan.FromHours(1),
        Linger = TimeSpan.FromSeconds(1),
        Endpoints = bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : [],
    };

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
