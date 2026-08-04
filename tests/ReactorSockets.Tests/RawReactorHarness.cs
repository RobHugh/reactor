using System.Net.Sockets;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// The fixture every <see cref="RawReactor"/> test is written against (see tests/CLAUDE.md's testing
/// decisions): a real Reactor on loopback, a <see cref="FakeTimeProvider"/>, a recording Sink, and a
/// place to register BCL counterparty sockets so they get disposed with the harness.
/// </summary>
internal sealed class RawReactorHarness : IDisposable
{
    private readonly List<Socket> _counterpartySockets = [];

    public RawReactorHarness(int capacity = 4, int? scratchBufferSize = null, DiagnosticLevel minimumLevel = DiagnosticLevel.Trace)
    {
        ReactorOptions options = new()
        {
            Capacity = capacity,
            TimeProvider = TimeProvider,
            Sink = Sink,
            MinimumLevel = minimumLevel,
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
        foreach (Socket socket in _counterpartySockets)
        {
            socket.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}
