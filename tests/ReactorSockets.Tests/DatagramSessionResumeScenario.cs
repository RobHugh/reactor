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

    private const byte ResumeType = 6;
    private const byte AcceptType = 4;
    private const int AcceptSessionIdOffset = 13;
    private const int AcceptSecretOffset = 37;

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

    /// <summary>Connects a dialler to an accepter through a fresh proxy, both with <paramref name="linger"/> when given.</summary>
    public static DatagramSessionResumeScenario Establish(TimeSpan? linger = null)
    {
        FakeTimeProvider clock = new();
        DatagramSessionReactorHarness dialler = new(
            bindLoopback: false,
            sendIdleInterval: SendIdleInterval,
            linger: linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: SendIdleInterval,
            linger: linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        LossyProxy proxy = new(accepter.Endpoint, clock);

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
        return new DatagramSessionResumeScenario(dialler, accepter, proxy, clock, accepterPeer);
    }

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
        AccepterEvents.AddRange(Accepter.DrainToList());
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
}
