using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Self-tests for <see cref="LossyProxy"/>, driven from two plain BCL UDP sockets standing in for the
/// client and the upstream endpoint — the proxy itself is the only <see cref="RawReactor"/> in this
/// picture, exactly the role it plays for a real Session-under-test in slices 3 through 8.
/// </summary>
public class LossyProxyTests
{
    [Fact]
    public void Pump_NoFaultQueued_RelaysAClientDatagramToUpstreamUnmodified()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        byte[] payload = [1, 2, 3, 4, 5];

        // Act
        client.SendTo(payload, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert
        Receive(upstream).Should().Equal(payload);
    }

    [Fact]
    public void Pump_NoFaultQueued_RelaysAnUpstreamReplyBackToTheMostRecentClient()
    {
        // Arrange — the reply direction needs a client datagram first, so the proxy knows who to relay
        // back to. Upstream replies to whichever address the proxy's own relay appeared to come from —
        // the proxy's upstream-facing Link — captured off the wire exactly as a real upstream would.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[]? _, IPEndPoint proxyUpstreamFacingEndPoint) = ReceiveFrom(upstream);

        byte[] reply = [9, 8, 7];

        // Act
        upstream.SendTo(reply, proxyUpstreamFacingEndPoint);
        proxy.Pump();

        // Assert
        Receive(client).Should().Equal(reply);
    }

    [Fact]
    public void DropNext_ClientToUpstream_TheNextRelayNeverReachesUpstream()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());

        // Act
        proxy.DropNext(RelayDirection.ClientToUpstream);
        client.SendTo([1, 2, 3], proxy.PublicEndPoint);
        proxy.Pump();

        // Assert
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("the datagram was dropped, not delayed");
    }

    [Fact]
    public void DuplicateNext_ClientToUpstream_UpstreamReceivesTheDatagramTwice()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        byte[] payload = [4, 5, 6];

        // Act
        proxy.DuplicateNext(RelayDirection.ClientToUpstream);
        client.SendTo(payload, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert
        Receive(upstream).Should().Equal(payload);
        Receive(upstream).Should().Equal(payload);
    }

    [Fact]
    public void DelayNext_ClientToUpstream_HeldUntilTheTimeProviderReachesTheDeadline()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, timeProvider);
        byte[] payload = [7, 7, 7];

        proxy.DelayNext(RelayDirection.ClientToUpstream, TimeSpan.FromSeconds(5));
        client.SendTo(payload, proxy.PublicEndPoint);
        proxy.Pump();

        // Act / Assert — not yet due.
        Receive(upstream, timeoutMilliseconds: 100).Should().BeNull("the delay has not elapsed on the time provider yet");

        // Act — advance the same clock the proxy reads, then Pump again to release it.
        timeProvider.Advance(TimeSpan.FromSeconds(5));
        proxy.Pump();

        // Assert
        Receive(upstream).Should().Equal(payload);
    }

    [Fact]
    public void ReorderNextTwo_ClientToUpstream_UpstreamReceivesTheSecondDatagramFirst()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        byte[] first = [1];
        byte[] second = [2];

        // Act
        proxy.ReorderNextTwo(RelayDirection.ClientToUpstream);
        client.SendTo(first, proxy.PublicEndPoint);
        proxy.Pump();
        client.SendTo(second, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert
        Receive(upstream).Should().Equal(second, "the held datagram is released only after the one that followed it");
        Receive(upstream).Should().Equal(first);
    }

    private static Socket CreateLoopbackUdpSocket()
    {
        Socket socket = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return socket;
    }

    /// <summary>
    /// Reads one datagram if one arrives within <paramref name="timeoutMilliseconds"/>, or returns
    /// <see langword="null"/> if none does — used both to fetch an expected relay and to prove a
    /// dropped or not-yet-due one genuinely never (yet) arrived.
    /// </summary>
    private static byte[]? Receive(Socket socket, int timeoutMilliseconds = 2000)
    {
        if (!socket.Poll(timeoutMilliseconds * 1000, SelectMode.SelectRead))
        {
            return null;
        }

        byte[] buffer = new byte[1024];
        int received = socket.Receive(buffer);
        return buffer[..received];
    }

    /// <summary>The <see cref="Receive"/> counterpart that also reports the sender's address, for a test that needs to reply to it.</summary>
    private static (byte[] Payload, IPEndPoint Sender) ReceiveFrom(Socket socket, int timeoutMilliseconds = 2000)
    {
        socket.Poll(timeoutMilliseconds * 1000, SelectMode.SelectRead).Should().BeTrue("a datagram was expected within the timeout");
        byte[] buffer = new byte[1024];
        EndPoint sender = new IPEndPoint(IPAddress.Any, 0);
        int received = socket.ReceiveFrom(buffer, ref sender);
        return (buffer[..received], (IPEndPoint)sender);
    }
}
