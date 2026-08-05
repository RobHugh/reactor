using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Self-tests for <see cref="HostilePeer"/> — sub-issue 2.5's requirement that the Raw test instrument
/// arrives at slice 3 already known to work, rather than debugged alongside the code it tests. Proven
/// against a <see cref="WireObserver"/>, itself only proven by <c>WireObserverTests</c>, so between the
/// two files every claim rests on the public Raw surface and nothing else.
/// </summary>
public class HostilePeerTests
{
    [Fact]
    public void SendDatagram_ArbitraryBytesIncludingOnesNoRealProtocolWouldSend_ArriveUnmodified()
    {
        // Arrange — bytes no length-prefixed or versioned protocol would ever produce on its own: a
        // leading 0xFF, an internally inconsistent length claim, no valid content at all.
        using HostilePeer peer = new();
        using WireObserver observer = new();
        IPEndPoint target = observer.BindUdp(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] malformed = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01];

        // Act
        peer.SendDatagram(target, malformed);
        observer.PumpUntil(() => observer.ReceivedDatagrams.Count > 0);

        // Assert
        observer.ReceivedDatagrams.Should().ContainSingle().Which.Should().Equal(malformed);
    }

    [Fact]
    public void ConnectAndSend_OverTcp_BytesArriveOnTheAcceptedLinkUnmodified()
    {
        // Arrange
        using HostilePeer peer = new();
        using WireObserver observer = new();
        IPEndPoint target = observer.ListenTcp(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] malformed = [0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF]; // e.g. a zero version, oversized length prefix

        // Act
        peer.ConnectAndSend(target, malformed);
        observer.PumpUntil(() => observer.ReceivedSegments.Count > 0);

        // Assert
        observer.ReceivedSegments.SelectMany(s => s).Should().Equal(malformed);
    }

    [Fact]
    public void SendMore_OnAConnectionAlreadyEstablished_ConcatenatesInOrderOnTheWire()
    {
        // Arrange — a hostile peer sending a malformed continuation after an initial write, to
        // exercise a Session Reactor's handling of a stream split across reads.
        using HostilePeer peer = new();
        using WireObserver observer = new();
        IPEndPoint target = observer.ListenTcp(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] first = [1, 2, 3];
        byte[] second = [4, 5, 6];

        // Act
        LinkId link = peer.ConnectAndSend(target, first);
        observer.PumpUntil(() => observer.ReceivedSegments.Count > 0);
        peer.SendMore(link, second);
        observer.PumpUntil(() => observer.ReceivedSegments.SelectMany(s => s).Count() >= first.Length + second.Length);

        // Assert
        observer.ReceivedSegments.SelectMany(s => s).Should().Equal([.. first, .. second]);
    }
}
