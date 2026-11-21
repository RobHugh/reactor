using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// A connected dialler and accepter through a <see cref="LossyProxy"/> on one shared clock, plus
/// the session id and resume secret the accepter issued, read off the Accept the proxy captured
/// (issue #56). Shared by the Resume tests so each can act as a resuming party on the accepting end.
/// </summary>
/// <remarks>
/// The accepting end's session id and secret travel in its Accept, at the offsets ADR 0009 documents.
/// A resuming party is a plain <see cref="UdpClient"/> on its own loopback port
/// (<see cref="NewResumer"/>), so a Resume's source differs from the Peer's address.
/// </remarks>
internal sealed class DatagramSessionResumeScenario : IDisposable
{
    /// <summary>The fixed size of every control datagram, zero padded.</summary>
    public const int ControlDatagramSize = 256;

    /// <summary>The offset of a control datagram's body, after the session field and type byte.</summary>
    public const int ControlBodyOffset = 5;

    /// <summary>A Resume's body: target session id (4) then the resume secret (16).</summary>
    public const int ResumeBodyLength = 20;

    /// <summary>The length of the resume secret.</summary>
    public const int ResumeSecretLength = 16;

    /// <summary>The send-idle interval both ends announce.</summary>
    public static readonly TimeSpan SendIdleInterval = TimeSpan.FromSeconds(1);

    /// <summary>The missed-heartbeat tolerance both ends use.</summary>
    public const int MissedHeartbeats = 3;

    /// <summary>How long a quiet Peer goes before this scenario's accepter interrupts it.</summary>
    public static readonly TimeSpan ReceiveIdleDeadline = SendIdleInterval * MissedHeartbeats;

    /// <summary>The offset of a control datagram's type byte, after the zero session field.</summary>
    public const int ControlTypeOffset = 4;

    /// <summary>The control type byte of a Resume.</summary>
    public const byte ResumeType = 6;

    private const byte AcceptType = 4;
    private const int AcceptSessionIdOffset = 13;
    private const int AcceptSecretOffset = DatagramSessionWire.AcceptSecretOffset;

    private DatagramSessionResumeScenario(
        DatagramSessionReactorHarness dialler,
        DatagramSessionReactorHarness accepter,
        LossyProxy proxy,
        FakeTimeProvider clock,
        PeerId accepterPeer)
    {
        Dialler = dialler;
        Accepter = accepter;
        Proxy = proxy;
        Clock = clock;
        AccepterPeer = accepterPeer;

        byte[] accept = proxy.Captures
            .Single(c => c.Direction == RelayDirection.UpstreamToClient && c.Payload[4] == AcceptType)
            .Payload;
        AccepterSessionId = BinaryPrimitives.ReadUInt32BigEndian(accept.AsSpan(AcceptSessionIdOffset));
        ResumeSecret = accept.AsSpan(AcceptSecretOffset, ResumeSecretLength).ToArray();
        UpstreamFacingEndPoint = proxy.Captures
            .Last(c => c.Direction == RelayDirection.UpstreamToClient).ReceivedOn!;
    }

    /// <summary>The dialling end, which reaches the accepter through <see cref="Proxy"/>.</summary>
    public DatagramSessionReactorHarness Dialler { get; }

    /// <summary>The accepting end.</summary>
    public DatagramSessionReactorHarness Accepter { get; }

    /// <summary>The proxy between the two ends, which also captures every relayed datagram.</summary>
    public LossyProxy Proxy { get; }

    /// <summary>The clock both ends and the proxy share.</summary>
    public FakeTimeProvider Clock { get; }

    /// <summary>The accepter's <see cref="PeerId"/> for the dialler.</summary>
    public PeerId AccepterPeer { get; }

    /// <summary>The session id the accepter asked to have stamped on datagrams sent to it.</summary>
    public uint AccepterSessionId { get; }

    /// <summary>The secret the accepter issued, which authorises a Resume against its Peer.</summary>
    public byte[] ResumeSecret { get; }

    /// <summary>Every event the accepter has drained so far, in order. Filled by this scenario's own pumping helpers, which drain after every Pump.</summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AccepterEvents { get; } = [];

    /// <summary>A copy of every <see cref="SessionEventKind.MessageReceived"/> payload the accepter has drained so far, in order: the payloads of <see cref="AccepterReceived"/>.</summary>
    public IReadOnlyList<byte[]> AccepterPayloads => [.. AccepterReceived.Select(r => r.Payload)];

    /// <summary>Every event the dialler has drained through <see cref="Step"/> or <see cref="PumpDiallerUntil"/> so far, in order. The Connect-time events are not in it.</summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DiallerEvents { get; } = [];

    /// <summary>The proxy's upstream-facing address the accepter currently sees as the dialler's source. Updated by <see cref="ResumeThroughNewPort"/>.</summary>
    public IPEndPoint UpstreamFacingEndPoint { get; private set; }

    /// <summary>The step <see cref="InterruptBothEnds"/> and <see cref="ResumeThroughNewPort"/> advance the clock by: a tenth of a send-idle interval, so the observed Pump interval (which scales the retransmission timeout) stays small.</summary>
    public static readonly TimeSpan StepInterval = SendIdleInterval / 10;

    /// <summary>A Linger long enough that <see cref="InterruptBothEnds"/> and <see cref="ResumeThroughNewPort"/>, repeated, never close the Session. Pass it to <see cref="Establish"/>.</summary>
    public static readonly TimeSpan ResumableLinger = TimeSpan.FromMinutes(5);

    /// <summary>The most steps either helper takes before it fails, so a broken path errors rather than hangs.</summary>
    private const int MaxSteps = 300;

    private bool _roundTripSeeded;

    /// <summary>
    /// One round of the whole path: advances the shared clock by <paramref name="interval"/>, Pumps the dialler,
    /// the proxy, the accepter and the proxy again, and drains each end into <see cref="DiallerEvents"/> and
    /// <see cref="AccepterEvents"/> straight after its own Pump, so no Pump clears an undrained event.
    /// Each proxy Pump repeats until the proxy has captured what the end before it sent this round
    /// (<see cref="PumpEndThenProxy"/>), rather than sleeping a fixed time.
    /// </summary>
    public void Step(TimeSpan interval)
    {
        Clock.Advance(interval);
        PumpEndThenProxy(Proxy, RelayDirection.ClientToUpstream, Dialler, () =>
        {
            Dialler.Reactor.Pump();
            DrainCapturing(Dialler, DiallerEvents, DiallerReceived);
        });
        PumpEndThenProxy(Proxy, RelayDirection.UpstreamToClient, Accepter, PumpAccepter);
    }

    /// <summary>
    /// Runs <paramref name="pumpEnd"/>, which Pumps and drains <paramref name="end"/>, then Pumps
    /// <paramref name="proxy"/> until it has captured in <paramref name="direction"/> every datagram that
    /// Pump sent (<see cref="PumpProxyUntilCaptured"/>).
    /// </summary>
    public static void PumpEndThenProxy(LossyProxy proxy, RelayDirection direction, DatagramSessionReactorHarness end, Action pumpEnd)
    {
        int capturedBefore = CapturedCount(proxy, direction);
        ulong sentBefore = end.Reactor.Counters.DatagramsSent;
        pumpEnd();
        PumpProxyUntilCaptured(proxy, direction, capturedBefore, end.Reactor.Counters.DatagramsSent - sentBefore);
    }

    /// <summary>
    /// How long <see cref="PumpProxyUntilCaptured"/> waits. Loopback delivery normally lands before the
    /// first proxy Pump. An end can also send somewhere the proxy never hears, such as the accepter's
    /// sends to the proxy's closed old port before a Resume, so the wait is short rather than
    /// <see cref="PumpDeadline.Default"/>.
    /// </summary>
    private static readonly TimeSpan _captureBudget = TimeSpan.FromMilliseconds(20);

    /// <summary>
    /// Pumps <paramref name="proxy"/> at least once, then again until it has captured <paramref name="sent"/>
    /// datagrams in <paramref name="direction"/> since it held <paramref name="capturedBefore"/>, or a short
    /// real-time budget is spent. Spins between Pumps rather than sleeping, because a one-millisecond sleep
    /// costs about fifteen on Windows (issue #393).
    /// </summary>
    public static void PumpProxyUntilCaptured(LossyProxy proxy, RelayDirection direction, int capturedBefore, ulong sent)
    {
        PumpDeadline deadline = PumpDeadline.Start(_captureBudget);
        while (true)
        {
            proxy.Pump();
            if ((ulong)(CapturedCount(proxy, direction) - capturedBefore) >= sent || deadline.Expired)
            {
                return;
            }

            Thread.Yield();
        }
    }

    /// <summary>How many datagrams <paramref name="proxy"/> has captured in <paramref name="direction"/>.</summary>
    public static int CapturedCount(LossyProxy proxy, RelayDirection direction) =>
        proxy.Captures.Count(c => c.Direction == direction);

    /// <summary>
    /// Cuts the path in both directions and steps the whole path until each end has raised one more
    /// <see cref="SessionEventKind.PeerInterrupted"/> than it had when this was called.
    /// Reusable: call it again after a Resume to interrupt the same Session a second time.
    /// </summary>
    /// <remarks>
    /// The first call first runs the live path for a few send-idle intervals, so each end has an acknowledged
    /// Ping and therefore a round-trip estimate. Without one, an unacknowledged Ping is retransmitted only after a
    /// whole Linger, and an end that filled its retention with Pings during the cut has nothing to say after the Resume.
    /// </remarks>
    public void InterruptBothEnds()
    {
        if (!_roundTripSeeded)
        {
            for (int i = 0; i < 4 * (int)(SendIdleInterval / StepInterval); i++)
            {
                Step(StepInterval);
            }

            _roundTripSeeded = true;
        }

        Proxy.StopRelayingBothDirections();
        StepUntilBothRaise(SessionEventKind.PeerInterrupted);
    }

    /// <summary>
    /// Moves the proxy's upstream-facing socket to a new port, resumes relaying, and steps the whole path
    /// until each end has raised one more <see cref="SessionEventKind.PeerResumed"/> than it had when this
    /// was called. Messages a test sent while Interrupted are relayed here too.
    /// </summary>
    /// <param name="address">The loopback address the new socket binds, so the source address changes too; <see cref="IPAddress.Loopback"/> when <see langword="null"/>.</param>
    /// <returns>The new upstream-facing address, which <see cref="UpstreamFacingEndPoint"/> now holds.</returns>
    public IPEndPoint ResumeThroughNewPort(IPAddress? address = null)
    {
        UpstreamFacingEndPoint = Proxy.MoveUpstreamFacingSocket(address);
        Proxy.ResumeRelayingBothDirections();
        StepUntilBothRaise(SessionEventKind.PeerResumed);
        return UpstreamFacingEndPoint;
    }

    /// <summary>The number of <paramref name="kind"/> events the dialler has raised so far.</summary>
    public int DiallerCount(SessionEventKind kind) => DiallerEvents.Count(e => e.Kind == kind);

    /// <summary>The number of <paramref name="kind"/> events the accepter has raised so far.</summary>
    public int AccepterCount(SessionEventKind kind) => AccepterEvents.Count(e => e.Kind == kind);

    private void StepUntilBothRaise(SessionEventKind kind)
    {
        int diallerTarget = DiallerCount(kind) + 1;
        int accepterTarget = AccepterCount(kind) + 1;
        for (int i = 0; i < MaxSteps; i++)
        {
            Step(StepInterval);
            if (DiallerCount(kind) >= diallerTarget && AccepterCount(kind) >= accepterTarget)
            {
                return;
            }
        }

        throw new InvalidOperationException($"Both ends did not raise {kind} within {MaxSteps} steps.");
    }

    /// <summary>
    /// Connects a dialler to an accepter through a fresh proxy, both with <paramref name="linger"/> when given.
    /// The size options apply to both ends and default to the harness defaults, except <paramref name="arenaSize"/>
    /// and <paramref name="receiveWindowFragments"/>, which size the accepter's receive side only.
    /// <paramref name="bindDialler"/> binds the dialler to a loopback port, so a test has an address to
    /// send it datagrams at directly (<c>Dialler.Endpoint</c>).
    /// </summary>
    public static DatagramSessionResumeScenario Establish(
        TimeSpan? linger = null,
        int maxMessageSize = 4096,
        int retentionBytes = 4096,
        int? fragmentSize = null,
        int? arenaSize = null,
        int? receiveWindowFragments = null,
        bool bindDialler = false,
        int? accepterMaxPeersPerAddress = null)
    {
        FakeTimeProvider clock = new();
        DatagramSessionReactorHarness dialler = new(
            maxMessageSize: maxMessageSize,
            retentionBytes: retentionBytes,
            fragmentSize: fragmentSize,
            bindLoopback: bindDialler,
            sendIdleInterval: SendIdleInterval,
            linger: linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        DatagramSessionReactorHarness accepter = new(
            maxPeersPerAddress: accepterMaxPeersPerAddress,
            maxMessageSize: maxMessageSize,
            retentionBytes: retentionBytes,
            fragmentSize: fragmentSize,
            arenaSize: arenaSize,
            receiveWindowFragments: receiveWindowFragments,
            sendIdleInterval: SendIdleInterval,
            linger: linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        LossyProxy proxy = new(accepter.Endpoint, clock);

        PeerId diallerPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
        return new DatagramSessionResumeScenario(dialler, accepter, proxy, clock, accepterPeer) { DiallerPeer = diallerPeer };
    }

    /// <summary>The dialler's <see cref="PeerId"/> for the accepter.</summary>
    public PeerId DiallerPeer { get; private init; }

    /// <summary>The session id the dialler asked to have stamped on datagrams sent to it, read off its captured HelloWithCookie.</summary>
    public uint DiallerSessionId => BinaryPrimitives.ReadUInt32BigEndian(CapturedHelloWithCookie().AsSpan(HelloWithCookieSessionIdOffset));

    /// <summary>The resume secret the dialler issued in its HelloWithCookie: the value a Resume against the dialler would have to present.</summary>
    public byte[] DiallerResumeSecret => CapturedHelloWithCookie().AsSpan(HelloWithCookieSecretOffset, ResumeSecretLength).ToArray();

    // HelloWithCookie body: version(4), nonce(8), epoch(8), cookie(32), sessionIdForMe(4),
    // maxMessageSize(4), sendIdleInterval(8), linger(8), fragmentSize(2), receiveWindowFragments(4),
    // resumeSecret(16).
    private const byte HelloWithCookieType = 3;
    private const int HelloWithCookieSessionIdOffset = ControlBodyOffset + 4 + 8 + 8 + 32;
    private const int HelloWithCookieSecretOffset = DatagramSessionWire.HelloWithCookieSecretOffset;

    private byte[] CapturedHelloWithCookie() => Proxy.Captures
        .Single(c => c.Direction == RelayDirection.ClientToUpstream && c.Payload[4] == HelloWithCookieType)
        .Payload;

    /// <summary>A UDP socket on its own loopback port: the party sending a Resume, from an address that is not the Peer's.</summary>
    public static UdpClient NewResumer() => new(new IPEndPoint(IPAddress.Loopback, 0));

    /// <summary>Builds a 256-byte, zero-padded Resume by hand: header, target session id, then the secret.</summary>
    public static byte[] BuildResume(uint targetSessionId, ReadOnlySpan<byte> secret)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, 0);
        datagram[4] = ResumeType;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ControlBodyOffset), targetSessionId);
        secret.CopyTo(datagram.AsSpan(ControlBodyOffset + sizeof(uint)));
        return datagram;
    }

    /// <summary>The control type byte of a PathChallenge.</summary>
    public const byte PathChallengeType = 7;

    /// <summary>The control type byte of a PathResponse.</summary>
    public const byte PathResponseType = 8;

    /// <summary>The length of a path challenge's nonce.</summary>
    public const int PathNonceLength = 16;

    /// <summary>A UDP socket on its own port at <paramref name="address"/>: a resuming party whose source address, not just its port, differs from the Peer's.</summary>
    public static UdpClient NewResumer(IPAddress address) => new(new IPEndPoint(address, 0));

    /// <summary>Builds a 256-byte, zero-padded PathResponse by hand: header, target session id, then the echoed nonce.</summary>
    public static byte[] BuildPathResponse(uint targetSessionId, ReadOnlySpan<byte> nonce) =>
        BuildSessionAndNonce(PathResponseType, targetSessionId, nonce);

    /// <summary>Builds a 256-byte, zero-padded PathChallenge by hand: header, target session id, then the nonce.</summary>
    public static byte[] BuildPathChallenge(uint targetSessionId, ReadOnlySpan<byte> nonce) =>
        BuildSessionAndNonce(PathChallengeType, targetSessionId, nonce);

    /// <summary>Whether <paramref name="payload"/> is a control datagram of <paramref name="type"/>.</summary>
    public static bool IsControlOfType(byte[] payload, byte type) => IsControl(payload) && payload[ControlTypeOffset] == type;

    /// <summary>The target session id a PathChallenge or PathResponse carries.</summary>
    public static uint PathTargetSessionId(byte[] payload) => BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(ControlBodyOffset));

    /// <summary>The nonce a PathChallenge or PathResponse carries.</summary>
    public static byte[] PathNonce(byte[] payload) => payload.AsSpan(ControlBodyOffset + sizeof(uint), PathNonceLength).ToArray();

    private static byte[] BuildSessionAndNonce(byte type, uint targetSessionId, ReadOnlySpan<byte> nonce)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        datagram[ControlTypeOffset] = type;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ControlBodyOffset), targetSessionId);
        nonce.CopyTo(datagram.AsSpan(ControlBodyOffset + sizeof(uint)));
        return datagram;
    }

    /// <summary>Whether <paramref name="payload"/> is a control datagram: zero in its session field and exactly <see cref="ControlDatagramSize"/> bytes.</summary>
    public static bool IsControl(byte[] payload) =>
        payload.Length == ControlDatagramSize && !payload.AsSpan(0, sizeof(uint)).ContainsAnyExcept((byte)0);

    /// <summary>Whether <paramref name="payload"/> is a Resume control datagram.</summary>
    public static bool IsResume(byte[] payload) => IsControl(payload) && payload[ControlTypeOffset] == ResumeType;

    /// <summary>Every Resume <paramref name="proxy"/> captured from the dialling side, in arrival order.</summary>
    public static List<byte[]> DiallerResumes(LossyProxy proxy) =>
        [.. proxy.Captures
            .Where(c => c.Direction == RelayDirection.ClientToUpstream && IsResume(c.Payload))
            .Select(c => c.Payload)];

    /// <summary>Every datagram the proxy captured in <paramref name="direction"/>, in arrival order.</summary>
    public List<RelayCapture> Captured(RelayDirection direction) =>
        [.. Proxy.Captures.Where(c => c.Direction == direction)];

    /// <summary>
    /// Waits until <paramref name="client"/> has a datagram to read, or the real-time budget is spent.
    /// Returns whether one arrived.
    /// </summary>
    public static bool WaitForDatagram(UdpClient client, TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (client.Available == 0)
        {
            if (deadline.Expired)
            {
                return false;
            }

            PumpDeadline.Yield();
        }

        return true;
    }

    /// <summary>
    /// Pumps the dialler, draining after every Pump into <see cref="DiallerEvents"/>, until
    /// <paramref name="condition"/> holds, <paramref name="pumps"/> Pumps have run, or the real-time
    /// budget is spent.
    /// </summary>
    public void PumpDiallerUntil(Func<bool> condition, int pumps = int.MaxValue)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        for (int i = 0; i < pumps; i++)
        {
            Dialler.Reactor.Pump();
            DrainCapturing(Dialler, DiallerEvents, DiallerReceived);
            if (condition() || deadline.Expired)
            {
                return;
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>Pumps the proxy until several Pumps in a row read nothing new, so every datagram already sent has been captured.</summary>
    public void SettleProxy()
    {
        PumpDeadline deadline = PumpDeadline.Start(TimeSpan.FromMilliseconds(500));
        int quiet = 0;
        while (quiet < 20 && !deadline.Expired)
        {
            int count = Proxy.Captures.Count;
            Proxy.Pump();
            quiet = Proxy.Captures.Count == count ? quiet + 1 : 0;
            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps the accepter, draining after every Pump, until it has read more than
    /// <paramref name="receivedBefore"/> datagrams in total, so a datagram sent to it directly has been handled.
    /// </summary>
    public void PumpAccepterUntilReceivedMoreThan(ulong receivedBefore) =>
        PumpAccepterUntil(() => Accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

    /// <summary>The Peer's real secret with its last byte flipped: as close as a wrong secret gets.</summary>
    public byte[] WrongSecret()
    {
        byte[] secret = [.. ResumeSecret];
        secret[^1] ^= 0xFF;
        return secret;
    }

    /// <summary>
    /// Advances the clock by <paramref name="interval"/> in the settle-then-advance shape
    /// (<see cref="DatagramSessionReactorHarness.SettleThenAdvance"/>) but drains the settle Pump into
    /// <see cref="AccepterEvents"/>, so an event raised on it is not cleared by the next Pump.
    /// </summary>
    public void AdvanceAccepter(TimeSpan interval)
    {
        Clock.Advance(interval - TimeSpan.FromMilliseconds(1));
        PumpAccepter();
        Clock.Advance(TimeSpan.FromMilliseconds(1));
    }

    /// <summary>Pumps the accepter once and drains that Pump into <see cref="AccepterEvents"/>.</summary>
    public void PumpAccepter()
    {
        Accepter.Reactor.Pump();
        DrainCapturing(Accepter, AccepterEvents, AccepterReceived);
    }

    /// <summary>Pumps the accepter, draining after every Pump, until <paramref name="condition"/> holds or the real-time budget is spent.</summary>
    public void PumpAccepterUntil(Func<bool> condition)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            PumpAccepter();
            if (condition() || deadline.Expired)
            {
                return;
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Cuts the path and pumps the accepter past its receive-idle deadline until it has raised
    /// exactly one <see cref="SessionEventKind.PeerInterrupted"/>, draining after every Pump.
    /// </summary>
    public void InterruptAccepter()
    {
        Proxy.StopRelayingBothDirections();
        AdvanceAccepter(ReceiveIdleDeadline);
        PumpAccepter();
        AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Dialler.Dispose();
        Accepter.Dispose();
        Proxy.Dispose();
    }

    /// <summary>Every Message the dialler has received through <see cref="Step"/>, in delivery order, each with the number of <see cref="SessionEventKind.PeerResumed"/> events the dialler had raised before it.</summary>
    public List<(byte[] Payload, int ResumedBefore)> DiallerReceived { get; } = [];

    /// <summary>Every Message the accepter has received through this scenario's pumping helpers, in delivery order, each with the number of <see cref="SessionEventKind.PeerResumed"/> events the accepter had raised before it.</summary>
    public List<(byte[] Payload, int ResumedBefore)> AccepterReceived { get; } = [];

    private static void DrainCapturing(
        DatagramSessionReactorHarness harness,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events,
        List<(byte[] Payload, int ResumedBefore)> received)
    {
        foreach (SessionEvent evt in harness.Reactor.Drain())
        {
            if (evt.Kind == SessionEventKind.MessageReceived)
            {
                received.Add((evt.Payload.ToArray(), events.Count(e => e.Kind == SessionEventKind.PeerResumed)));
            }

            events.Add((evt.Kind, evt.Peer, evt.Reason));
        }
    }
}
