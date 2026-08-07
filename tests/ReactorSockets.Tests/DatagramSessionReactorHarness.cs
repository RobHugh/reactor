using System.Net;
using System.Security.Cryptography;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// The fixture the datagram Session tests are written against (see tests/CLAUDE.md): a real
/// <see cref="DatagramSessionReactor"/> on loopback, a <see cref="FakeTimeProvider"/>, and a recording
/// Sink. L0 and L1 are internal, so this — the public surface a consumer has — is the only seam.
/// </summary>
internal sealed class DatagramSessionReactorHarness : IDisposable
{
    /// <summary>
    /// Constructs a harness around a Reactor built from the given options, every one of which defaults
    /// to the value the majority of tests want so that a test names only what it varies.
    /// </summary>
    /// <param name="bindLoopback">
    /// Binds one loopback port-zero endpoint. Passed <see langword="false"/> by tests exercising the
    /// dial-only case, where the first <c>Connect</c> is what binds anything at all.
    /// </param>
    public DatagramSessionReactorHarness(
        int capacity = 4,
        int? admissionCap = null,
        int maxMessageSize = 4096,
        int retentionBytes = 4096,
        int? fragmentSize = null,
        int? receiveWindowFragments = null,
        int? missedHeartbeats = null,
        int? arenaSize = null,
        DiagnosticLevel minimumLevel = DiagnosticLevel.Trace,
        IReactorDiagnostics? sinkOverride = null,
        bool withoutDiagnostics = false,
        bool bindLoopback = true,
        IReadOnlyList<IPEndPoint>? endpoints = null,
        RandomNumberGenerator? entropy = null)
    {
        IReactorDiagnostics? installedSink = withoutDiagnostics ? null : sinkOverride ?? Sink;
        Entropy = entropy ?? RandomNumberGenerator.Create();
        Reactor = new DatagramSessionReactor(new DatagramSessionOptions
        {
            Reactor = new ReactorOptions
            {
                Capacity = capacity,
                AdmissionCap = admissionCap,
                ArenaSize = arenaSize,
                TimeProvider = TimeProvider,
                Sink = installedSink,
                MinimumLevel = installedSink is null ? null : minimumLevel,
            },
            MaxMessageSize = maxMessageSize,
            RetentionBytes = retentionBytes,
            FragmentSize = fragmentSize,
            ReceiveWindowFragments = receiveWindowFragments,
            MissedHeartbeats = missedHeartbeats,
            SendIdleInterval = TimeSpan.FromSeconds(1),
            Linger = TimeSpan.FromSeconds(1),
            Endpoints = endpoints ?? (bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : []),
            Entropy = Entropy,
        });
    }

    /// <summary>The clock the Reactor was built with — advance it to exercise anything time-dependent.</summary>
    public FakeTimeProvider TimeProvider { get; } = new();

    /// <summary>The recording Sink installed on the Reactor, unless the harness was told to override or omit it.</summary>
    public RecordingDiagnosticsSink Sink { get; } = new();

    /// <summary>The entropy the Reactor draws session ids, nonces and secrets from.</summary>
    public RandomNumberGenerator Entropy { get; }

    /// <summary>The Reactor under test.</summary>
    public DatagramSessionReactor Reactor { get; }

    /// <summary>The first endpoint this Reactor is bound to — what a counterparty sends to.</summary>
    public IPEndPoint Endpoint => Reactor.BoundEndpoints[0];

    /// <summary>Drains the current Frame into a list of plain values that outlive the drain.</summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DrainToList()
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> drained = [];
        foreach (SessionEvent evt in Reactor.Drain())
        {
            drained.Add((evt.Kind, evt.Peer, evt.Reason));
        }

        return drained;
    }

    /// <summary>Pumps exactly <paramref name="pumps"/> times, accumulating everything drained.</summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpAndDrainAll(int pumps)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> all = [];
        for (int i = 0; i < pumps; i++)
        {
            Reactor.Pump();
            all.AddRange(DrainToList());
        }

        return all;
    }

    /// <summary>
    /// Pumps both ends alternately until <paramref name="condition"/> holds or
    /// <paramref name="maxRounds"/> is spent, accumulating what each end drains. A handshake is four
    /// flights across two Reactors, and loopback delivery is not synchronous with the send, so the
    /// number of rounds is bounded rather than exact.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntil(
            DatagramSessionReactorHarness dialler,
            DatagramSessionReactorHarness accepter,
            Func<bool> condition,
            int maxRounds = 20)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> diallerEvents = [];
        List<(SessionEventKind, PeerId, DisconnectReason)> accepterEvents = [];

        for (int round = 0; round < maxRounds && !condition(); round++)
        {
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
        }

        return (diallerEvents, accepterEvents);
    }

    /// <summary>Pumps up to <paramref name="maxPumps"/> times, stopping as soon as <paramref name="condition"/> holds.</summary>
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
        Entropy.Dispose();
        GC.SuppressFinalize(this);
    }
}
