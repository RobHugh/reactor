using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// The fixture every test in this project is written against (see tests/CLAUDE.md's testing
/// decisions): a real Reactor on loopback, a <see cref="FakeTimeProvider"/>, a recording Sink, and a
/// place to register BCL counterparty sockets so they get disposed with the harness. L0 and L1 are
/// internal, so this — the public surface a consumer has — is the only seam the tests use.
/// </summary>
internal sealed class RawReactorHarness : IDisposable
{
    private readonly List<Socket> _counterpartySockets = [];

    /// <param name="sinkOverride">
    /// Installed instead of <see cref="Sink"/> when a test needs a Sink that throws or disables
    /// itself. Such a test holds its own reference to it; <see cref="Sink"/> stays uninstalled.
    /// </param>
    /// <param name="withoutDiagnostics">Installs no Sink at all, and therefore no minimum level.</param>
    public RawReactorHarness(
        int capacity = 4,
        int? scratchBufferSize = null,
        DiagnosticLevel minimumLevel = DiagnosticLevel.Trace,
        int? arenaSize = null,
        IReactorDiagnostics? sinkOverride = null,
        bool withoutDiagnostics = false)
    {
        IReactorDiagnostics? installedSink = withoutDiagnostics ? null : sinkOverride ?? Sink;
        ReactorOptions options = new()
        {
            Capacity = capacity,
            ArenaSize = arenaSize,
            TimeProvider = TimeProvider,
            Sink = installedSink,
            MinimumLevel = installedSink is null ? null : minimumLevel,
        };
        Reactor = new RawReactor(options, scratchBufferSize);
    }

    public FakeTimeProvider TimeProvider { get; } = new();

    public RecordingDiagnosticsSink Sink { get; } = new();

    public RawReactor Reactor { get; }

    /// <summary>Creates a BCL UDP socket the harness disposes on teardown, standing in for a counterparty.</summary>
    public Socket CreateCounterpartySocket(AddressFamily family = AddressFamily.InterNetwork)
    {
        Socket socket = new(family, SocketType.Dgram, ProtocolType.Udp);
        _counterpartySockets.Add(socket);
        return socket;
    }

    /// <summary>Binds a loopback Link and returns it together with the address a counterparty sends to.</summary>
    public (LinkId Link, IPEndPoint LocalEndPoint) BindLoopbackLink()
    {
        LinkId link = Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return (link, Reactor.LocalOf(link));
    }

    /// <summary>Sends <paramref name="payload"/> to <paramref name="destination"/> from a fresh counterparty socket.</summary>
    public void SendFromCounterparty(IPEndPoint destination, byte[] payload)
    {
        Socket counterparty = CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        counterparty.SendTo(payload, destination);
    }

    /// <summary>
    /// Binds a Link, sends it a 100-byte datagram and Pumps once. With a scratch buffer smaller than
    /// that — every caller passes <c>scratchBufferSize: 16</c> — this is the cheapest way to make the
    /// Reactor emit a real <c>Warn</c> record, which is what the diagnostics tests need.
    /// </summary>
    public void ProvokeTruncation()
    {
        (_, IPEndPoint localEndPoint) = BindLoopbackLink();
        SendFromCounterparty(localEndPoint, new byte[100]);
        Reactor.Pump();
    }

    /// <summary>Drains the current Frame into a list, copying each payload out before the next Pump invalidates it.</summary>
    public List<(RawEventKind Kind, LinkId Link, byte[] Payload)> DrainToList()
    {
        List<(RawEventKind, LinkId, byte[])> drained = [];
        foreach (RawEvent evt in Reactor.Drain())
        {
            drained.Add((evt.Kind, evt.Link, evt.Payload.ToArray()));
        }

        return drained;
    }

    /// <summary>
    /// Pumps up to <paramref name="maxPumps"/> times, stopping as soon as <paramref name="condition"/>
    /// is satisfied. Used instead of a sleep for the one place UDP's own asynchrony (an ICMP-driven
    /// error surfacing on a later receive) cannot be pinned to an exact Pump count.
    /// </summary>
    public void PumpUntil(Func<bool> condition, int maxPumps = 20)
    {
        for (int i = 0; i < maxPumps && !condition(); i++)
        {
            Reactor.Pump();
        }
    }

    public void Dispose()
    {
        Reactor.Dispose();
        foreach (Socket socket in _counterpartySockets)
        {
            socket.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}
