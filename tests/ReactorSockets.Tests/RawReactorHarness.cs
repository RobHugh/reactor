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

    /// <summary>
    /// Constructs a harness around a Reactor built from the given options, every one of which
    /// defaults to the value the majority of tests want so that a test names only what it varies.
    /// </summary>
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
        int? admissionCap = null,
        IReactorDiagnostics? sinkOverride = null,
        bool withoutDiagnostics = false)
    {
        IReactorDiagnostics? installedSink = withoutDiagnostics ? null : sinkOverride ?? Sink;
        ReactorOptions options = new()
        {
            Capacity = capacity,
            ArenaSize = arenaSize,
            AdmissionCap = admissionCap,
            TimeProvider = TimeProvider,
            Sink = installedSink,
            MinimumLevel = installedSink is null ? null : minimumLevel,
        };
        Reactor = new RawReactor(options, scratchBufferSize);
    }

    /// <summary>The clock the Reactor was built with — advance it to exercise anything time-dependent.</summary>
    public FakeTimeProvider TimeProvider { get; } = new();

    /// <summary>The recording Sink installed on the Reactor, unless the harness was told to override or omit it.</summary>
    public RecordingDiagnosticsSink Sink { get; } = new();

    /// <summary>The Reactor under test.</summary>
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
    /// Binds a loopback listening Link and returns it together with the address a counterparty
    /// connects to.
    /// </summary>
    public (LinkId Listener, IPEndPoint LocalEndPoint) ListenOnLoopback()
    {
        LinkId listener = Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));
        return (listener, Reactor.LocalOf(listener));
    }

    /// <summary>
    /// Binds and listens on a fresh BCL TCP socket, standing in for a counterparty that plays the
    /// server role while this harness's Reactor dials out to it — the mirror image of
    /// <see cref="ListenOnLoopback"/>, needed only for tests that exercise the dial side of a stream
    /// Link's reads rather than the accept side.
    /// </summary>
    public (Socket Listener, IPEndPoint LocalEndPoint) CreateCounterpartyListener()
    {
        Socket listener = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _counterpartySockets.Add(listener);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen();
        return (listener, (IPEndPoint)listener.LocalEndPoint!);
    }

    /// <summary>
    /// Begins connecting a fresh BCL TCP counterparty socket to <paramref name="destination"/> and
    /// returns it immediately — the connect itself runs asynchronously against the OS, exactly like a
    /// consumer's own dial would, so the caller Pumps the Reactor afterwards for the accept to land.
    /// </summary>
    public Socket ConnectCounterpartyTo(IPEndPoint destination)
    {
        Socket counterparty = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        _counterpartySockets.Add(counterparty);
        counterparty.Connect(destination);
        return counterparty;
    }

    /// <summary>
    /// Forces <paramref name="socket"/>'s close to send an immediate RST instead of a graceful FIN —
    /// the reliable way to provoke <see cref="CloseReason.ResetByPeer"/> from a counterparty, including
    /// one that already shut down its send half and would otherwise close without putting anything
    /// further on the wire.
    /// </summary>
    public static void CloseCounterpartyAbruptly(Socket socket)
    {
        socket.LingerState = new LingerOption(true, 0);
        socket.Close();
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

    /// <summary>Drains the current Frame into a list that also carries each event's <see cref="CloseReason"/>.</summary>
    public List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> DrainToReasonList()
    {
        List<(RawEventKind, LinkId, CloseReason)> drained = [];
        foreach (RawEvent evt in Reactor.Drain())
        {
            drained.Add((evt.Kind, evt.Link, evt.Reason));
        }

        return drained;
    }

    /// <summary>
    /// Drains the current Frame and returns the Link named by its first
    /// <see cref="RawEventKind.Accepted"/> event, throwing if there was none. The Link rather than the
    /// event itself, because a <see cref="RawEvent"/> aliases the frame arena and must not outlive the
    /// drain — a <see cref="LinkId"/> is a value and survives it.
    /// </summary>
    public LinkId FirstAcceptedLink()
    {
        foreach (RawEvent evt in Reactor.Drain())
        {
            if (evt.Kind == RawEventKind.Accepted)
            {
                return evt.Link;
            }
        }

        throw new InvalidOperationException("No Accepted event was drained.");
    }

    /// <summary>
    /// Pumps exactly <paramref name="pumps"/> times, draining and accumulating events after each one —
    /// used instead of looping on a condition when an outcome (dial completion, connection
    /// establishment) is known to need more than one Pump on loopback but the exact count is fixed and
    /// deterministic rather than open-ended.
    /// </summary>
    public List<(RawEventKind Kind, LinkId Link, byte[] Payload)> PumpAndDrainAll(int pumps)
    {
        List<(RawEventKind, LinkId, byte[])> all = [];
        for (int i = 0; i < pumps; i++)
        {
            Reactor.Pump();
            all.AddRange(DrainToList());
        }

        return all;
    }

    /// <summary>The <see cref="CloseReason"/>-carrying counterpart to <see cref="PumpAndDrainAll"/>.</summary>
    public List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> PumpAndDrainReasons(int pumps)
    {
        List<(RawEventKind, LinkId, CloseReason)> all = [];
        for (int i = 0; i < pumps; i++)
        {
            Reactor.Pump();
            all.AddRange(DrainToReasonList());
        }

        return all;
    }

    /// <summary>
    /// Pumps until <paramref name="link"/> produces a <see cref="RawEventKind.Closed"/> or
    /// <paramref name="maxAttempts"/> is spent, sleeping <paramref name="delayMilliseconds"/> between
    /// attempts. A refused TCP dial is not a loopback-local decision the way a UDP ICMP unreachable or
    /// a successful loopback accept/connect is — the OS resolves the RST on its own real-time schedule,
    /// observed here to take over a second even on loopback in this environment — so a bound on
    /// wall-clock attempts, not on Pump count alone, is what determinism means for this one outcome.
    /// </summary>
    public List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> PumpAndDrainReasonsUntilClosed(
        LinkId link, int maxAttempts = 100, int delayMilliseconds = 50)
    {
        List<(RawEventKind, LinkId, CloseReason)> all = [];
        for (int i = 0; i < maxAttempts; i++)
        {
            Reactor.Pump();
            List<(RawEventKind, LinkId, CloseReason)> drained = DrainToReasonList();
            all.AddRange(drained);
            if (drained.Exists(evt => evt.Item1 == RawEventKind.Closed && evt.Item2 == link))
            {
                break;
            }

            Thread.Sleep(delayMilliseconds);
        }

        return all;
    }

    /// <summary>
    /// Binds a loopback TCP socket, reads back the port the OS gave it, then closes it — a reliable way
    /// to provoke a refused dial, since nothing is listening on that port once this returns but the
    /// port itself was genuinely live a moment ago.
    /// </summary>
    public IPEndPoint ReserveAndReleaseLoopbackPort()
    {
        using Socket probe = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return (IPEndPoint)probe.LocalEndPoint!;
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
