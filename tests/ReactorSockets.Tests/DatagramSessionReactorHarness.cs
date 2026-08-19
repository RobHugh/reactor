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

    /// <summary>
    /// Pumps exactly <paramref name="pumps"/> times, accumulating everything drained. For asserting
    /// that something does <em>not</em> happen: a fixed count of Pumps bounds syscalls, not time, so
    /// it is not a way to wait for something to arrive — see <see cref="PumpDeadline"/> and use
    /// <see cref="PumpUntilDrained"/> for that.
    /// </summary>
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
    /// Pumps until <paramref name="condition"/> holds over everything drained so far, or until
    /// <paramref name="budget"/> of real time is spent. This is how a test waits for an event to
    /// arrive; <see cref="PumpAndDrainAll"/> counts Pumps and so waits for essentially no time at all
    /// (<see cref="PumpDeadline"/>, issue #153).
    /// </summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpUntilDrained(
        Func<IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>, bool> condition,
        TimeSpan? budget = null)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> all = [];
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            Reactor.Pump();
            all.AddRange(DrainToList());
            if (condition(all) || deadline.Expired)
            {
                return all;
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps both ends alternately until <paramref name="condition"/> holds or <paramref name="budget"/>
    /// of real time is spent, accumulating what each end drains. A handshake is four flights across two
    /// Reactors, and loopback delivery is not synchronous with the send, so the wait is bounded by a
    /// clock rather than a round count (<see cref="PumpDeadline"/>).
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntil(
            DatagramSessionReactorHarness dialler,
            DatagramSessionReactorHarness accepter,
            Func<bool> condition,
            TimeSpan? budget = null)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> diallerEvents = [];
        List<(SessionEventKind, PeerId, DisconnectReason)> accepterEvents = [];

        // The condition is asked once per round, after both ends have pumped, never before: several
        // callers phrase it as a DrainToList call, which consumes what it counts, so asking twice in a
        // round would swallow the very events the round produced.
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            if (condition() || deadline.Expired)
            {
                return (diallerEvents, accepterEvents);
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps both ends alternately until <paramref name="condition"/> holds over what each has drained
    /// so far, or until <paramref name="budget"/> of real time is spent. The two-Reactor counterpart to
    /// <see cref="PumpUntilDrained"/>, and what a test asserting on a completed handshake wants:
    /// <see cref="PumpBothRounds"/> counts rounds and so waits for essentially no time at all
    /// (<see cref="PumpDeadline"/>, issue #153).
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntilDrained(
            DatagramSessionReactorHarness dialler,
            DatagramSessionReactorHarness accepter,
            Func<
                IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>,
                IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>,
                bool> condition,
            TimeSpan? budget = null)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];

        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            if (condition(diallerEvents, accepterEvents) || deadline.Expired)
            {
                return (diallerEvents, accepterEvents);
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps both ends alternately until each has raised <see cref="SessionEventKind.PeerConnected"/>,
    /// which is what almost every two-Reactor test needs before it can assert anything: the handshake
    /// has completed on both sides and both ends' events are in hand.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntilConnected(
            DatagramSessionReactorHarness dialler,
            DatagramSessionReactorHarness accepter,
            TimeSpan? budget = null) =>
        PumpBothUntilDrained(
            dialler,
            accepter,
            (diallerEvents, accepterEvents) =>
                diallerEvents.Any(e => e.Kind == SessionEventKind.PeerConnected)
                && accepterEvents.Any(e => e.Kind == SessionEventKind.PeerConnected),
            budget);

    /// <summary>
    /// Pumps both ends alternately exactly <paramref name="rounds"/> times, accumulating what each
    /// drains. For asserting that something does <em>not</em> happen; to wait for something that
    /// should, use <see cref="PumpBothUntilDrained"/>.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothRounds(
            DatagramSessionReactorHarness dialler,
            DatagramSessionReactorHarness accepter,
            int rounds)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];

        for (int round = 0; round < rounds; round++)
        {
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
        }

        return (diallerEvents, accepterEvents);
    }

    /// <summary>
    /// Pumps until <paramref name="condition"/> holds or <paramref name="budget"/> of real time is
    /// spent. For a condition read off something other than the drained events — a Sink, a counter —
    /// where <see cref="PumpUntilDrained"/> has nothing to hand the caller (<see cref="PumpDeadline"/>).
    /// </summary>
    public void PumpUntil(Func<bool> condition, TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            Reactor.Pump();
            if (condition() || deadline.Expired)
            {
                return;
            }

            PumpDeadline.Yield();
        }
    }

    public void Dispose()
    {
        Reactor.Dispose();
        Entropy.Dispose();
        GC.SuppressFinalize(this);
    }
}
