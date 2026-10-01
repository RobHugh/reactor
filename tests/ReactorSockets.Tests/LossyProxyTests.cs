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

    [Fact]
    public void SetSizeThreshold_PayloadExactlyAtThreshold_IsForwardedRepeatedlyAsTheRuleStands()
    {
        // Arrange — pin the boundary exactly: a threshold of 5 must forward every payload of length 5,
        // not just the first one, because the rule is a standing property of the path rather than a
        // one-shot fault consumed after a single use.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        byte[] atThreshold = [1, 2, 3, 4, 5];

        // Act
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 5);
        client.SendTo(atThreshold, proxy.PublicEndPoint);
        proxy.Pump();
        client.SendTo(atThreshold, proxy.PublicEndPoint);
        proxy.Pump();
        client.SendTo(atThreshold, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert — three separate relays, all three at exactly the threshold, all three forwarded.
        Receive(upstream).Should().Equal(atThreshold);
        Receive(upstream).Should().Equal(atThreshold);
        Receive(upstream).Should().Equal(atThreshold);
    }

    [Fact]
    public void SetSizeThreshold_PayloadOneByteOverThreshold_IsNeverForwarded()
    {
        // Arrange — pin the boundary exactly rather than near it: a payload one byte over a threshold
        // of 5 must never be forwarded, so an off-by-one in the comparison doesn't silently let
        // oversized traffic through.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        byte[] oneByteOverThreshold = [1, 2, 3, 4, 5, 6];

        // Act
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 5);
        client.SendTo(oneByteOverThreshold, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("the payload exceeded the threshold by exactly one byte");
    }

    [Fact]
    public void SetLossEveryNth_SixRelaysWithEveryThirdRule_DropsExactlyTheThirdAndSixthRelay()
    {
        // Arrange — pin exact positions, not a rate: with every-3rd set, relays 1, 2, 4 and 5 must be
        // forwarded and relays 3 and 6 must be dropped, counted from when the rule was set. A
        // probability that happened to average the same rate would not guarantee this.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, 3);

        // Act / Assert — send and check one relay at a time so a dropped relay is confirmed dropped,
        // not merely not-yet-arrived.
        for (int position = 1; position <= 6; position++)
        {
            byte[] payload = [(byte)position];
            client.SendTo(payload, proxy.PublicEndPoint);
            proxy.Pump();

            if (position is 3 or 6)
            {
                Receive(upstream, timeoutMilliseconds: 200).Should().BeNull($"relay {position} is the Nth relay and must be dropped");
            }
            else
            {
                Receive(upstream).Should().Equal(payload, $"relay {position} is not the Nth relay and must be forwarded");
            }
        }
    }

    [Fact]
    public void SetSizeThreshold_ClearedWithNoSizeThreshold_RestoresNormalRelayingForPreviouslyOversizedPayloads()
    {
        // Arrange — a payload the threshold would have dropped must relay normally once the threshold
        // is cleared back to NoSizeThreshold, proving the clear value actually turns the rule off
        // rather than just raising it.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        byte[] payload = [1, 2, 3, 4, 5, 6];
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 5);

        // Act
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, LossyProxy.NoSizeThreshold);
        client.SendTo(payload, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert
        Receive(upstream).Should().Equal(payload);
    }

    [Fact]
    public void SetLossEveryNth_ClearedWithNoLossEveryNth_RestoresNormalRelayingPastThePreviousNthPosition()
    {
        // Arrange — set every-2nd and let the count reach 1, one relay short of dropping, then clear
        // it; the relays that follow must all go through, including the one that would have landed on
        // the old rule's Nth position.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, 2);
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([1], "relay 1 is not the Nth relay under the old rule");

        // Act
        proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, LossyProxy.NoLossEveryNth);
        client.SendTo([2], proxy.PublicEndPoint);
        proxy.Pump();
        client.SendTo([3], proxy.PublicEndPoint);
        proxy.Pump();

        // Assert — both relays after clearing go through, including relay 2, which the old rule would have dropped.
        Receive(upstream).Should().Equal([2]);
        Receive(upstream).Should().Equal([3]);
    }

    [Fact]
    public void DropNext_QueuedBehindAStandingSizeThresholdDrop_AppliesToTheNextRelayThatReachesTheQueue()
    {
        // Arrange — the size threshold drops an oversized relay before the one-shot queue is ever
        // consulted for it, so a fault queued in the meantime must still land on the next relay that
        // actually reaches the queue, not be silently consumed by the relay the threshold killed.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 3);
        proxy.DropNext(RelayDirection.ClientToUpstream);

        // Act / Assert — relay 1 is oversized: caught by the threshold, never reaches the one-shot queue.
        client.SendTo([1, 2, 3, 4], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("the oversized relay was dropped by the threshold, before the queue was consulted");

        // Act / Assert — relay 2 is within the threshold: it is the first to reach the queue, and the queued drop applies to it.
        client.SendTo([9], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("the queued drop applied to the first relay that actually reached the queue");

        // Assert — relay 3 is within the threshold and the queue is now empty: it relays normally.
        client.SendTo([8], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([8]);
    }

    [Fact]
    public void DropNext_QueuedBehindAStandingEveryNthDrop_AppliesToTheNextRelayThatReachesTheQueue()
    {
        // Arrange — every-3rd drops the third relay before the one-shot queue is ever consulted for
        // it, so a fault queued in the meantime must still land on the next relay that actually
        // reaches the queue, not be silently consumed by the relay the standing rule killed.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, 3);

        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([1], "relay 1 is not the Nth relay");

        client.SendTo([2], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([2], "relay 2 is not the Nth relay");

        // Act — queue a one-shot drop now, immediately before the standing rule drops relay 3.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        client.SendTo([3], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("relay 3 is the Nth relay and is dropped by the standing rule, never reaching the queue");

        client.SendTo([4], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("relay 4 is the first to reach the queue, and the queued drop applies to it");

        // Assert — the queue is empty again and the every-Nth count is only at 2 of 3, so relay 5 relays normally.
        client.SendTo([5], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([5]);
    }

    [Fact]
    public void SetSizeThreshold_OnClientToUpstreamOnly_LeavesUpstreamToClientRelayingNormally()
    {
        // Arrange — the size threshold is set on one direction only; the reply direction must be
        // untouched by it, even for a payload that would have exceeded the threshold had it applied
        // there too.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[]? _, IPEndPoint proxyUpstreamFacingEndPoint) = ReceiveFrom(upstream);
        byte[] largeReply = new byte[10];

        // Act
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 3);
        upstream.SendTo(largeReply, proxyUpstreamFacingEndPoint);
        proxy.Pump();

        // Assert
        Receive(client).Should().Equal(largeReply, "the threshold was set only on the client-to-upstream direction");
    }

    [Fact]
    public void SetLossEveryNth_OnClientToUpstreamOnly_LeavesUpstreamToClientRelayingNormally()
    {
        // Arrange — every client-to-upstream relay is set to drop; the reply direction must keep
        // relaying every datagram normally regardless.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[]? _, IPEndPoint proxyUpstreamFacingEndPoint) = ReceiveFrom(upstream);
        proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, 1);

        // Act — three replies upstream-to-client, none of which should be affected by the client-to-upstream rule.
        upstream.SendTo([1], proxyUpstreamFacingEndPoint);
        proxy.Pump();
        upstream.SendTo([2], proxyUpstreamFacingEndPoint);
        proxy.Pump();
        upstream.SendTo([3], proxyUpstreamFacingEndPoint);
        proxy.Pump();

        // Assert
        Receive(client).Should().Equal([1]);
        Receive(client).Should().Equal([2]);
        Receive(client).Should().Equal([3]);
    }

    [Fact]
    public void SetLossEveryNth_SizeThresholdStandingOnTheSameDirection_OversizedRelayDoesNotAdvanceTheEveryNthCount()
    {
        // Arrange — issue #296's acceptance criterion: the every-Nth rule composes with a size
        // threshold standing at the same time. The threshold is consulted first and unconditionally
        // (per Relay's doc), so a relay it kills must never reach the every-Nth rule and therefore
        // must not advance its count. Every-3rd is set alongside a threshold of 3; an oversized
        // payload is interleaved between two in-size ones. If the oversized relay wrongly advanced
        // the count, the third in-size relay below would be forwarded instead of dropped, and the
        // drop would land one in-size relay later than pinned here.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 3);
        proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, 3);
        byte[] oversized = [9, 9, 9, 9];

        // Act / Assert — in-size relay 1: not the Nth in-size relay, forwarded.
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([1], "the first in-size relay is not the Nth in-size relay");

        // Act / Assert — in-size relay 2: not the Nth in-size relay, forwarded.
        client.SendTo([2], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([2], "the second in-size relay is not the Nth in-size relay");

        // Act / Assert — oversized relay: killed by the threshold before the every-Nth rule ever sees it.
        client.SendTo(oversized, proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("the oversized relay was dropped by the threshold, never reaching the every-Nth rule");

        // Act / Assert — in-size relay 3: the third in-size relay to reach the every-Nth rule, dropped by it.
        client.SendTo([3], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("this is the third in-size relay, and the oversized relay in between did not count toward it");

        // Assert — in-size relay 4: the count reset after the drop, so this one is forwarded again.
        client.SendTo([4], proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream).Should().Equal([4], "the every-Nth count reset after dropping the third in-size relay");
    }

    [Fact]
    public void SetLossEveryNth_NegativeEveryNth_ThrowsArgumentOutOfRangeException()
    {
        // Arrange — a negative count does not correspond to any dropping schedule, so it must be
        // rejected rather than silently coerced into one.
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());

        // Act
        Action act = () => proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, -1);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Relay_StandingRuleDropsARelayWhileAReorderIsHeld_TheHeldDatagramIsReleasedOnlyByTheNextForwardedRelay()
    {
        // Arrange — a standing rule dropping a relay is not a relay for reorder purposes: it must not
        // release a held-back datagram, and it must not itself become the held datagram. A reorder is
        // queued for relay 1, holding it back; relay 2 is then killed outright by a standing size
        // threshold before the one-shot queue is ever consulted for it, so it cannot touch the held
        // datagram either way. Relay 3 is the first relay to actually reach the queue afterward, so it
        // is the one that both forwards itself and releases the datagram relay 1 left held.
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, 3);
        proxy.ReorderNextTwo(RelayDirection.ClientToUpstream);
        byte[] held = [1];
        byte[] oversized = [9, 9, 9, 9];
        byte[] releasing = [3];

        // Act / Assert — relay 1 is held for reorder, nothing arrives yet.
        client.SendTo(held, proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("relay 1 was held back for reorder, not forwarded");

        // Act / Assert — relay 2 is oversized: dropped by the standing threshold, never reaching the
        // reorder machinery, and it never appears on the wire.
        client.SendTo(oversized, proxy.PublicEndPoint);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull("relay 2 was dropped by the standing threshold and did not release the held datagram");

        // Act — relay 3 is in-size and unfaulted: it is forwarded, and only now is the held datagram released.
        client.SendTo(releasing, proxy.PublicEndPoint);
        proxy.Pump();

        // Assert — relay 3 goes out first, followed by the datagram held since relay 1; relay 2 never appears at all.
        Receive(upstream).Should().Equal(releasing, "relay 3 forwards itself before the held datagram is released");
        Receive(upstream).Should().Equal(held, "the held datagram is released only once a relay actually reaches and passes the queue");
    }

    [Fact]
    public void MoveUpstreamFacingSocket_AfterwardsRelaysReachUpstreamFromANewSourcePort()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[] _, IPEndPoint before) = ReceiveFrom(upstream);

        // Act
        IPEndPoint moved = proxy.MoveUpstreamFacingSocket();
        client.SendTo([2], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[] payload, IPEndPoint after) = ReceiveFrom(upstream);

        // Assert
        payload.Should().Equal([2]);
        after.Port.Should().NotBe(before.Port);
        after.Should().Be(moved);
    }

    [Fact]
    public void MoveUpstreamFacingSocket_ReplyToNewAddressReachesClient_ReplyToOldAddressDoesNot()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[] _, IPEndPoint oldAddress) = ReceiveFrom(upstream);
        IPEndPoint newAddress = proxy.MoveUpstreamFacingSocket();

        // Act
        upstream.SendTo([7], oldAddress);
        proxy.Pump();
        byte[]? viaOld = Receive(client, timeoutMilliseconds: 200);
        upstream.SendTo([8], newAddress);
        proxy.Pump();

        // Assert
        viaOld.Should().BeNull("the old socket is closed and no longer relays");
        Receive(client).Should().Equal([8]);
    }

    [Fact]
    public void Captures_RecordTheSocketEachDatagramArrivedOn_AcrossAMove()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[] _, IPEndPoint oldAddress) = ReceiveFrom(upstream);
        upstream.SendTo([2], oldAddress);
        proxy.Pump();
        Receive(client);
        IPEndPoint newAddress = proxy.MoveUpstreamFacingSocket();

        // Act
        upstream.SendTo([3], newAddress);
        proxy.Pump();
        Receive(client);

        // Assert
        proxy.Captures.Select(c => c.ReceivedOn).Should().Equal(proxy.PublicEndPoint, oldAddress, newAddress);
    }

    [Fact]
    public void MoveUpstreamFacingSocket_WhileStopped_StaysStoppedUntilResumed()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        ReceiveFrom(upstream);
        proxy.StopRelayingBothDirections();

        // Act
        IPEndPoint newAddress = proxy.MoveUpstreamFacingSocket();
        client.SendTo([2], proxy.PublicEndPoint);
        proxy.Pump();
        byte[]? whileStopped = Receive(upstream, timeoutMilliseconds: 200);
        proxy.ResumeRelayingBothDirections();
        client.SendTo([3], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[] payload, IPEndPoint sender) = ReceiveFrom(upstream);
        upstream.SendTo([4], newAddress);
        proxy.Pump();

        // Assert
        whileStopped.Should().BeNull();
        payload.Should().Equal([3]);
        sender.Should().Be(newAddress);
        Receive(client).Should().Equal([4]);
    }

    [Fact]
    public void ResumeRelayingBothDirections_AfterStop_RelaysBothDirectionsAgain()
    {
        // Arrange
        using Socket client = CreateLoopbackUdpSocket();
        using Socket upstream = CreateLoopbackUdpSocket();
        using LossyProxy proxy = new((IPEndPoint)upstream.LocalEndPoint!, new FakeTimeProvider());
        client.SendTo([1], proxy.PublicEndPoint);
        proxy.Pump();
        (byte[] _, IPEndPoint proxyAddress) = ReceiveFrom(upstream);
        proxy.StopRelayingBothDirections();
        client.SendTo([2], proxy.PublicEndPoint);
        upstream.SendTo([3], proxyAddress);
        proxy.Pump();
        Receive(upstream, timeoutMilliseconds: 200).Should().BeNull();
        Receive(client, timeoutMilliseconds: 200).Should().BeNull();

        // Act
        proxy.ResumeRelayingBothDirections();
        client.SendTo([4], proxy.PublicEndPoint);
        upstream.SendTo([5], proxyAddress);
        proxy.Pump();

        // Assert
        Receive(upstream).Should().Equal([4]);
        Receive(client).Should().Equal([5]);
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
