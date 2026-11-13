using System.Buffers.Binary;
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
    /// <param name="timeProvider">
    /// The clock this harness's Reactor is built with. Defaults to a fresh <see cref="FakeTimeProvider"/>
    /// so most tests need not think about it, but a fixture driving two Reactors and a
    /// <see cref="LossyProxy"/> as one deterministic scenario passes the same instance to every harness
    /// and to the proxy — a shared clock is the only way advancing it once moves every retransmission
    /// timer in the scenario together, rather than moving one Reactor's timers while the other's stand
    /// still (<see cref="FragmentSizeDiagnosisFixture"/>).
    /// </param>
    public DatagramSessionReactorHarness(
        int capacity = 4,
        int? admissionCap = null,
        int? maxPeersPerAddress = null,
        bool defaultMaxPeersPerAddress = false,
        int? peerLimitIPv6PrefixLength = null,
        int? maxDatagramsPerPump = null,
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
        RandomNumberGenerator? entropy = null,
        FakeTimeProvider? timeProvider = null,
        TimeSpan? linger = null,
        TimeSpan? sendIdleInterval = null)
    {
        IReactorDiagnostics? installedSink = withoutDiagnostics ? null : sinkOverride ?? Sink;
        Entropy = entropy ?? RandomNumberGenerator.Create();
        TimeProvider = timeProvider ?? new();
        Options = new DatagramSessionOptions
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
            // No limit unless asked: every party in a test shares the loopback address.
            MaxPeersPerAddress = defaultMaxPeersPerAddress ? null : maxPeersPerAddress ?? capacity,
            PeerLimitIPv6PrefixLength = peerLimitIPv6PrefixLength,
            FragmentSize = fragmentSize,
            ReceiveWindowFragments = receiveWindowFragments,
            MissedHeartbeats = missedHeartbeats,
            MaxDatagramsPerPump = maxDatagramsPerPump,
            // Defaults far out (issue #340): a test that never mentions the send-idle timer should
            // never see a Ping arrive uninvited mid-scenario and upset a wire-count assertion it wrote
            // long before the Ping existed. Tests exercising the timer itself pass their own short value.
            SendIdleInterval = sendIdleInterval ?? TimeSpan.FromHours(1),
            Linger = linger ?? TimeSpan.FromSeconds(1),
            Endpoints = endpoints ?? (bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : []),
            Entropy = Entropy,
        };
        Reactor = new DatagramSessionReactor(Options);
    }

    /// <summary>The clock the Reactor was built with — advance it to exercise anything time-dependent.</summary>
    public FakeTimeProvider TimeProvider { get; }

    /// <summary>
    /// The options the Reactor was built from — the same public surface a consumer reads back through
    /// (e.g. <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>), kept here so a test can confirm
    /// a configured value after a run rather than only trusting what it passed in at construction.
    /// </summary>
    public DatagramSessionOptions Options { get; }

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
    /// Drains the current Frame and copies out every <see cref="SessionEventKind.MessageReceived"/>
    /// payload. The copy is what makes the bytes outlive the Frame they aliased, which is the only way
    /// a test can assert on them after the next Pump (#219).
    /// </summary>
    public List<byte[]> DrainPayloads()
    {
        List<byte[]> payloads = [];
        foreach (SessionEvent evt in Reactor.Drain())
        {
            if (evt.Kind == SessionEventKind.MessageReceived)
            {
                payloads.Add(evt.Payload.ToArray());
            }
        }

        return payloads;
    }

    /// <summary>
    /// Drains the current Frame and keeps nothing. What an allocation measurement uses in place of
    /// <see cref="DrainToList"/>: that helper's own list grows with the number of Events drained, so a
    /// phase delivering more Messages than another would appear to allocate more inside the Reactor
    /// when the difference is the test's list (#217).
    /// </summary>
    public void DrainAndDiscard()
    {
        foreach (SessionEvent evt in Reactor.Drain())
        {
            _ = evt.Kind;
        }
    }

    /// <summary>
    /// Ends <paramref name="peer"/>'s Session at once with <see cref="DisconnectReason.LocalClose"/>; the
    /// Frame then holds the <see cref="SessionEventKind.PeerInterrupted"/> the wait caused as well as the
    /// <see cref="SessionEventKind.PeerClosed"/>. <c>Close</c> on an
    /// Established Peer now starts a Goodbye instead of closing (issue #397), so a test that only needs the
    /// Peer gone (a freed Slot, a stale <see cref="PeerId"/>) lets the receive-idle deadline pass first: an
    /// Interrupted Peer keeps the immediate <c>Close</c>.
    /// </summary>
    public void CloseImmediately(PeerId peer)
    {
        TimeProvider.Advance(Options.SendIdleInterval * Options.ResolvedMissedHeartbeats);
        Reactor.Pump();
        Reactor.Close(peer);
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
    /// Pumps a dialler, a <see cref="LossyProxy"/> and an accepter in a round until both ends have
    /// raised <see cref="SessionEventKind.PeerConnected"/> — the three-party equivalent of
    /// <see cref="PumpBothUntilConnected"/>, for the delivery-mode tests that need faults scripted
    /// between two live Sessions (#218 and the promise tests that follow it). The proxy relays toward
    /// the accepter, so the dialler must have been pointed at <see cref="LossyProxy.PublicEndPoint"/>.
    /// </summary>
    public static PeerId PumpThroughProxyUntilConnected(
        DatagramSessionReactorHarness dialler,
        LossyProxy proxy,
        DatagramSessionReactorHarness accepter,
        TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        PeerId? accepterPeer = null;
        bool diallerConnected = false;

        while (true)
        {
            PumpRoundThroughProxy(dialler, proxy, accepter);

            foreach ((SessionEventKind kind, PeerId peer, DisconnectReason _) in accepter.DrainToList())
            {
                if (kind == SessionEventKind.PeerConnected)
                {
                    accepterPeer = peer;
                }
            }

            diallerConnected |= dialler.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);

            if (diallerConnected && accepterPeer is PeerId established)
            {
                return established;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The proxied handshake did not complete within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// One round of the proxied loop: the dialler, then the proxy, then the accepter, then the proxy
    /// again so the accepter's reply is relayed back in the same round. Drains nothing — a caller
    /// asserting on Events drains for itself.
    /// </summary>
    public static void PumpRoundThroughProxy(
        DatagramSessionReactorHarness dialler,
        LossyProxy proxy,
        DatagramSessionReactorHarness accepter)
    {
        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        proxy.Pump();
    }

    /// <summary>
    /// Pumps a dialler, a <see cref="WireTap"/> and an accepter in a round until both ends have raised
    /// <see cref="SessionEventKind.PeerConnected"/> — the tap's counterpart to
    /// <see cref="PumpThroughProxyUntilConnected"/>, which is typed to
    /// <see cref="LossyProxy"/> and so cannot drive a <see cref="WireTap"/> instead.
    /// </summary>
    public static PeerId PumpThroughTapUntilConnected(
        DatagramSessionReactorHarness dialler,
        WireTap tap,
        DatagramSessionReactorHarness accepter,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>? diallerEvents = null,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>? accepterEvents = null,
        TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        PeerId? accepterPeer = null;
        bool diallerConnected = false;

        while (true)
        {
            PumpRoundThroughTap(dialler, tap, accepter);

            foreach ((SessionEventKind kind, PeerId peer, DisconnectReason reason) in accepter.DrainToList())
            {
                accepterEvents?.Add((kind, peer, reason));
                if (kind == SessionEventKind.PeerConnected)
                {
                    accepterPeer = peer;
                }
            }

            foreach ((SessionEventKind kind, PeerId peer, DisconnectReason reason) in dialler.DrainToList())
            {
                diallerEvents?.Add((kind, peer, reason));
                diallerConnected |= kind == SessionEventKind.PeerConnected;
            }

            if (diallerConnected && accepterPeer is PeerId established)
            {
                return established;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The tapped handshake did not complete within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>One round of the tapped loop: the dialler, then the tap, then the accepter, then the tap again so the accepter's reply is relayed back in the same round.</summary>
    public static void PumpRoundThroughTap(DatagramSessionReactorHarness dialler, WireTap tap, DatagramSessionReactorHarness accepter)
    {
        dialler.Reactor.Pump();
        tap.Pump();
        accepter.Reactor.Pump();
        tap.Pump();
    }

    /// <summary>
    /// Pumps <paramref name="dialler"/> once, then pumps <paramref name="tap"/> until it has relayed
    /// <paramref name="dataDatagrams"/> data Datagrams from the dialler after its first
    /// <paramref name="capturesBefore"/> captures, and returns them in the order they were relayed. A
    /// Raw Link delivers one datagram per Pump, so a dialler Pump that writes several Datagrams needs as
    /// many tap Pumps. The accepter is not pumped. A data Datagram is one whose session id is not the
    /// reserved 0. Returns whatever arrived by the deadline, so assert the count on the result.
    /// </summary>
    public static List<byte[]> PumpOnceAndRelayDataDatagrams(
        DatagramSessionReactorHarness dialler, WireTap tap, int capturesBefore, int dataDatagrams)
    {
        dialler.Reactor.Pump();
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            tap.Pump();
            List<byte[]> relayed = tap.Captures
                .Skip(capturesBefore)
                .Where(c => c.Direction == TapDirection.DiallerToAccepter
                    && BinaryPrimitives.ReadUInt32BigEndian(c.Payload.AsSpan(0, 4)) != 0)
                .Select(c => c.Payload)
                .ToList();
            if (relayed.Count >= dataDatagrams || deadline.Expired)
            {
                return relayed;
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

    /// <summary>
    /// Splits a large wait into "advance to one millisecond short, Pump once, advance the final
    /// millisecond" so the Pump immediately following this call observes a one-millisecond cadence
    /// rather than the whole wait (<see cref="DatagramSessionPingRetransmissionTests"/>'s own remarks
    /// give the full reasoning: <c>RetransmissionTimer.EffectiveTimeout</c> floors the RTO at the
    /// observed Pump interval, so a large advance immediately before an assertion Pump would corrupt
    /// that floor for whatever timer that Pump is meant to exercise). Promoted here from that file
    /// (issue #352) because every liveness test that advances the clock by a whole idle interval needs
    /// the same technique, not only the Ping-retransmission ones.
    /// </summary>
    public static void SettleThenAdvance(DatagramSessionReactorHarness harness, TimeSpan interval)
    {
        harness.TimeProvider.Advance(interval - TimeSpan.FromMilliseconds(1));
        harness.Reactor.Pump();
        harness.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
    }

    public void Dispose()
    {
        Reactor.Dispose();
        Entropy.Dispose();
        GC.SuppressFinalize(this);
    }
}
