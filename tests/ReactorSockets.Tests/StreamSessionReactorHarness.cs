using System.Net;
using System.Security.Cryptography;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// The fixture the stream Session tests are written against (see tests/CLAUDE.md): a real
/// <see cref="StreamSessionReactor"/> on loopback, a <see cref="FakeTimeProvider"/>, and a recording
/// Sink. L0 and L1 are internal, so this — the public surface a consumer has — is the only seam.
/// Mirrors <see cref="DatagramSessionReactorHarness"/>, the house pattern for this profile.
/// </summary>
internal sealed class StreamSessionReactorHarness : IDisposable
{
    public StreamSessionReactorHarness(
        int capacity = 4,
        int? admissionCap = null,
        int maxMessageSize = 4096,
        int retentionBytes = 4100,
        int? receiveBufferSize = null,
        int? sendBufferSize = null,
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
        Reactor = new StreamSessionReactor(new StreamSessionOptions
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
            ReceiveBufferSize = receiveBufferSize,
            SendBufferSize = sendBufferSize,
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

    /// <summary>The entropy the Reactor draws the resume secret from.</summary>
    public RandomNumberGenerator Entropy { get; }

    /// <summary>The Reactor under test.</summary>
    public StreamSessionReactor Reactor { get; }

    /// <summary>The first endpoint this Reactor is bound to — what a counterparty connects to.</summary>
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
    /// Drains the current Frame into a list of plain values that outlive the drain, copying each
    /// event's payload out as a <see cref="byte"/> array — empty for anything but
    /// <see cref="SessionEventKind.MessageReceived"/> — since <see cref="SessionEvent.Payload"/>
    /// aliases the frame arena and is invalid past the next Pump (ADR 0008).
    /// </summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> DrainToListWithPayload()
    {
        List<(SessionEventKind, PeerId, DisconnectReason, byte[])> drained = [];
        foreach (SessionEvent evt in Reactor.Drain())
        {
            drained.Add((evt.Kind, evt.Peer, evt.Reason, evt.Payload.ToArray()));
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
    /// <paramref name="maxRounds"/> is spent, accumulating what each end drains. A TCP handshake needs
    /// several round trips of the connect and the record exchange to settle, and delivery is not
    /// synchronous with a send, so the number of rounds is bounded rather than exact.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntil(
            StreamSessionReactorHarness dialler,
            StreamSessionReactorHarness accepter,
            Func<bool> condition,
            int maxRounds = 20)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> diallerEvents = [];
        List<(SessionEventKind, PeerId, DisconnectReason)> accepterEvents = [];

        for (int round = 0; round < maxRounds && !condition(); round++)
        {
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
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
