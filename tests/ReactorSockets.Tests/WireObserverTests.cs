using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Self-tests for <see cref="WireObserver"/> — proven independently against plain BCL sockets rather
/// than <see cref="HostilePeer"/>, so this file and <c>HostilePeerTests</c> do not validate each other
/// circularly.
/// </summary>
public class WireObserverTests
{
    [Fact]
    public void BindUdp_CounterpartySendsADatagram_IsRecordedExactly()
    {
        // Arrange
        using WireObserver observer = new();
        IPEndPoint target = observer.BindUdp(new IPEndPoint(IPAddress.Loopback, 0));
        using Socket counterparty = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        byte[] payload = [9, 8, 7, 6, 5];

        // Act
        counterparty.SendTo(payload, target);
        observer.PumpUntil(() => observer.ReceivedDatagrams.Count > 0);

        // Assert
        observer.ReceivedDatagrams.Should().ContainSingle().Which.Should().Equal(payload);
    }

    [Fact]
    public void BindUdp_TwoDatagramsFromTheSameCounterparty_AreRecordedInArrivalOrder()
    {
        // Arrange
        using WireObserver observer = new();
        IPEndPoint target = observer.BindUdp(new IPEndPoint(IPAddress.Loopback, 0));
        using Socket counterparty = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

        // Act
        counterparty.SendTo([1], target);
        counterparty.SendTo([2], target);
        observer.PumpUntil(() => observer.ReceivedDatagrams.Count >= 2);

        // Assert — Should().Equal on a List<byte[]> compares each element by reference, not content, so
        // this asserts per-slot content directly instead.
        observer.ReceivedDatagrams.Should().HaveCount(2);
        observer.ReceivedDatagrams[0].Should().Equal(1);
        observer.ReceivedDatagrams[1].Should().Equal(2);
    }

    [Fact]
    public void ListenTcp_CounterpartyConnectsAndWrites_LinkIsAcceptedAndBytesAreRecordedExactly()
    {
        // Arrange
        using WireObserver observer = new();
        IPEndPoint target = observer.ListenTcp(new IPEndPoint(IPAddress.Loopback, 0));
        using Socket counterparty = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        byte[] payload = [1, 2, 3, 4];

        // Act
        counterparty.Connect(target);
        observer.PumpUntil(() => observer.AcceptedLinks.Count > 0);
        counterparty.Send(payload);
        observer.PumpUntil(() => observer.ReceivedSegments.Count > 0);

        // Assert
        observer.AcceptedLinks.Should().ContainSingle();
        observer.ReceivedSegments.SelectMany(s => s).Should().Equal(payload);
    }
}
