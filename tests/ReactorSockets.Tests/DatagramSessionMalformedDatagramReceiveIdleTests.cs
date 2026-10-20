using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #354 (parent #54): the malformed side of #351's rule. Only a well-formed data datagram
/// counts as hearing from the peer — a malformed one still reaches the Reactor and is counted (proof
/// via <see cref="ReactorCounters.MalformedSessionDatagrams"/>), but must never touch
/// <c>PeerSlot.LastReceiveTimestamp</c>, so #352's receive-idle deadline
/// (<c>peerSendIdle × ResolvedMissedHeartbeats</c>) keeps counting from the last well-formed
/// datagram no matter how much malformed noise arrives in between.
/// </summary>
/// <remarks>
/// Connects two real <see cref="DatagramSessionReactor"/>s through a <see cref="LossyProxy"/> exactly
/// as <see cref="DatagramSessionReceiveIdleExpiryTests"/> does, then — once
/// <see cref="LossyProxy.StopRelayingBothDirections"/> has cut real traffic — uses a
/// <see cref="HostilePeer"/> to send malformed datagrams straight to the accepter's own bound
/// endpoint, bypassing the proxy entirely, carrying the accepter's own wire session id read off the
/// Accept control datagram the proxy already relayed during the handshake
/// (<see cref="DatagramSessionWireFormatTests"/>'s own technique, offset-for-offset).
/// </remarks>
public class DatagramSessionMalformedDatagramReceiveIdleTests
{
    /// <summary>Short enough that the deadline this file exercises (× <see cref="MissedHeartbeats"/>) is a small clock advance.</summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than <see cref="_sendIdleInterval"/> × <see cref="MissedHeartbeats"/>: out of scope here (#55), but must not itself expire mid-test.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    /// <summary>The margin "just before"/"just after" the deadline is measured by.</summary>
    private static readonly TimeSpan _margin = TimeSpan.FromMilliseconds(1);

    private const int ControlDatagramSize = 256;
    private const int ControlTypeOffset = 4;
    private const byte ControlTypeAccept = 4;

    /// <summary>Offset of the session id the accepter asks to be stamped on datagrams sent to it — ControlBodyOffset (5) plus the 8-byte echoed nonce — per <see cref="DatagramSessionWireFormatTests"/>.</summary>
    private const int AcceptSessionIdForMeOffset = 5 + 8;

    private const int DatagramHeaderSize = 20;

    /// <summary>ReliableOrdered's wire mode bits (ADR 0009) — arbitrary, since any legal flags byte with no complete record behind it is malformed the same way.</summary>
    private const byte ReliableOrderedFlags = 0b0000_0011;

    [Fact]
    public void Pump_MalformedDatagramsThroughoutTheWindow_DoNotPostponeThePeerInterruptedDeadline()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        using HostilePeer intruder = new();

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        uint accepterSessionId = ReadAccepterSessionId(proxy);
        byte[] malformedDatagram = BuildMalformedDatagram(accepterSessionId);

        // Act — every datagram either way is silently lost from here on, so only the malformed
        // datagrams this test injects directly below can ever reach the accepter again.
        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;

        // Inject malformed datagrams spread across the whole window, including one shortly before
        // the deadline: a regression that restored the receive-idle stamp on malformed input would
        // push the deadline visibly later than every assertion below expects.
        TimeSpan[] injectionPoints =
        [
            deadline / 5,
            deadline * 2 / 5,
            deadline * 3 / 5,
            deadline * 4 / 5,
            deadline - _margin,
        ];

        TimeSpan elapsed = TimeSpan.Zero;
        ulong expectedMalformed = 0;
        foreach (TimeSpan point in injectionPoints)
        {
            accepter.TimeProvider.Advance(point - elapsed);
            elapsed = point;

            intruder.SendDatagram(accepter.Endpoint, malformedDatagram);
            expectedMalformed++;
            ulong malformedToReach = expectedMalformed;
            accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams >= malformedToReach);
        }

        // Assert — AC2: every injected datagram is shown to have reached the Reactor via its
        // malformed-discard counter, and none of it reached the event stream.
        accepter.Reactor.Counters.MalformedSessionDatagrams.Should().Be((ulong)injectionPoints.Length);
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted,
            "the last injection landed one millisecond short of the deadline — nothing has happened yet");

        // Act — the final millisecond: the deadline, measured from the handshake's last well-formed
        // datagram, has now passed.
        accepter.TimeProvider.Advance(deadline - elapsed);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(1);

        // Assert — AC1: PeerInterrupted arrives just after peerSendIdle × 3 from the last well-formed
        // datagram, exactly as if none of the malformed traffic above had ever arrived.
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(accepterPeer));
    }

    /// <summary>
    /// Reads the accepter's own wire session id — the one it asked to be stamped on datagrams sent to
    /// it — off the Accept control datagram <paramref name="proxy"/> already relayed during the
    /// handshake, at the same documented offset <see cref="DatagramSessionWireFormatTests"/> reads it
    /// from a <see cref="WireTap"/> capture.
    /// </summary>
    private static uint ReadAccepterSessionId(LossyProxy proxy)
    {
        byte[] accept = proxy.Captures.Single(
            c => c.Direction == RelayDirection.UpstreamToClient
                && c.Payload.Length == ControlDatagramSize
                && c.Payload[ControlTypeOffset] == ControlTypeAccept).Payload;

        return BinaryPrimitives.ReadUInt32BigEndian(accept.AsSpan(AcceptSessionIdForMeOffset, 4));
    }

    /// <summary>
    /// Builds a datagram that clears every check up to and including <c>FragmentSetsAreWellFormed</c>'s
    /// prerequisite — a valid 20-byte header naming <paramref name="sessionId"/> — but fails in the
    /// parse loop itself: one flags byte claiming a legal, unfragmented ReliableOrdered record with
    /// nothing behind it, one byte short of the seven <see cref="PacketRecordCodec"/> requires to read
    /// even its fixed fields. This is issue #354's own example: "a valid 20-byte header followed by a
    /// truncated record."
    /// </summary>
    private static byte[] BuildMalformedDatagram(uint sessionId)
    {
        byte[] datagram = new byte[DatagramHeaderSize + 1];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), sessionId);
        // Bytes [4, 20) are the two all-zero ack blocks — an ordinary, well-formed empty ack.
        datagram[DatagramHeaderSize] = ReliableOrderedFlags;
        return datagram;
    }
}
