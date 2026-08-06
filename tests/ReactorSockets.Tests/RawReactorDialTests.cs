using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Dialling outbound TCP connections and the two dial-failure reasons — sub-issue 2.2. Every assertion
/// is against the drained <see cref="RawEvent"/>s, a <see cref="Result"/> or a
/// <see cref="DiagnosticRecord"/>; nothing here reaches into how or when a connect was detected.
/// </summary>
/// <remarks>
/// <para>
/// The rejected-destination tests below now run on Linux as well as Windows, which corrected two things
/// this file used to assume. A dial is begun on a non-blocking socket, and Windows reports essentially
/// every outcome later through the pending socket error <c>PollConnect</c> reads; Linux answers some of
/// them — the unreachable-network case among them — synchronously from <c>connect</c> itself, and
/// destroys others before <c>PollConnect</c> can look. <c>TcpLink.Dial</c> captures both, and its
/// remarks carry the ordering rule that makes the second possible.
/// </para>
/// <para>
/// The other correction is to the addresses. Choosing a destination that fails the same way on both
/// platforms is harder than it looks: an address rejected outright by one stack is often routed,
/// answered or simply accepted by the other, and inside a container a NAT gateway will answer for
/// addresses no real network would. Each test below records what was measured rather than what seemed
/// obvious, because what seemed obvious is what was wrong.
/// </para>
/// </remarks>
public class RawReactorDialTests
{
    [Fact]
    public void Connect_ListeningPort_PumpBothSides_DrainsExactlyOneConnected()
    {
        // Arrange — one Reactor plays both roles: it listens, and it dials its own listener, on loopback.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();

        // Act
        LinkId dialLink = harness.Reactor.Connect(localEndPoint);
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.PumpAndDrainAll(pumps: 3);

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Connected && evt.Link == dialLink);
    }

    [Fact]
    public void Connect_BeforeAnyPump_ProducesNoEvent()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();

        // Act — Connect registers the Link; nothing has Pumped yet.
        harness.Reactor.Connect(localEndPoint);
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Should().BeEmpty("nothing is delivered except in response to a Pump");
    }

    [Fact]
    public void Connect_ClosedPort_DrainsClosedWithRefused()
    {
        // Arrange — a port that was genuinely bound a moment ago, then released, so nothing answers it.
        using RawReactorHarness harness = new();
        IPEndPoint closedEndPoint = harness.ReserveAndReleaseLoopbackPort();

        // Act — a real RST for a refused loopback connect is not instant in every environment (observed
        // over a second in this one), so this polls with a real delay rather than a fixed Pump count;
        // see PumpAndDrainReasonsUntilClosed's remarks.
        LinkId dialLink = harness.Reactor.Connect(closedEndPoint);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained =
            harness.PumpAndDrainReasonsUntilClosed(dialLink);

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == dialLink)
            .Which.Reason.Should().Be(CloseReason.Refused);
    }

    [Fact]
    public void Connect_ClosedPort_LogsLinkFailedCarryingThePlatformSocketError()
    {
        // Arrange
        using RawReactorHarness harness = new();
        IPEndPoint closedEndPoint = harness.ReserveAndReleaseLoopbackPort();

        // Act
        LinkId dialLink = harness.Reactor.Connect(closedEndPoint);
        harness.PumpAndDrainReasonsUntilClosed(dialLink);

        // Assert
        DiagnosticRecord record = harness.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.LinkFailed).Subject;
        record.Level.Should().Be(DiagnosticLevel.Warn, "a reset or refusal is a stranger's prerogative (ADR 0013)");
        record.Link.Should().Be(dialLink);
        record.Value0.Should().NotBe(0, "Value0 carries the platform SocketError for a failed dial");
    }

    [Fact]
    public void Connect_UnreachableDestination_DrainsClosedWithUnreachable()
    {
        // Arrange — a destination the stack knows it has no way to reach, so it says so immediately
        // rather than putting a packet on the wire and waiting. That is what makes this deterministic
        // where an ordinary unrouted address is not: a TEST-NET address like 192.0.2.1 goes to the
        // default gateway and stays Pending indefinitely, whereas both of these resolve in time to be
        // read by the first Pump.
        //
        // As with the catch-all test above, the address is chosen per platform and the assertion is
        // not. Measured on two hosts each — a developer machine and a CI runner:
        //
        //                     | Windows              | Linux
        //   240.0.0.1         | NetworkUnreachable   | still pending
        //   224.0.0.1         | AddressNotAvailable  | NetworkUnreachable
        //
        // Neither row is uniform, and that is the finding: this test dialled 240.0.0.1 on the strength
        // of "no IP stack will route it, so nothing about the environment's routing table participates",
        // which is false twice over. Linux does not reject it out of hand, and inside a container the
        // NAT gateway answers for it. 224.0.0.1 fails on Windows for the opposite reason — a multicast
        // group is not an address Windows will even source a connection to, which is not the same thing
        // as being unreachable.
        //
        // 100::1, the RFC 6666 discard prefix, was the one destination measured Unreachable on all four
        // hosts, and is still not used: it reports that way only where there is no IPv6 default route,
        // so a developer with native IPv6 would watch it stay Pending instead. Each platform gets the
        // address its own stack rejects structurally, and no test here depends on IPv6 being routable.
        using RawReactorHarness harness = new();
        IPEndPoint unreachableEndPoint = OperatingSystem.IsWindows()
            ? new IPEndPoint(IPAddress.Parse("240.0.0.1"), 1)
            : new IPEndPoint(IPAddress.Parse("224.0.0.1"), 1);

        // Act
        LinkId dialLink = harness.Reactor.Connect(unreachableEndPoint);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained = harness.PumpAndDrainReasons(pumps: 1);

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == dialLink)
            .Which.Reason.Should().Be(CloseReason.Unreachable);
    }

    [Fact]
    public void Connect_AddressTheStackRejectsWithNoDedicatedReason_DrainsClosedWithFailed()
    {
        // Arrange — the catch-all arm of the reason mapping, which exists precisely for platform errors
        // the roster has no dedicated word for. Reaching it needs a destination the stack rejects
        // outright with something that is neither a refusal nor an unreachable network, and no single
        // address does that on both platforms: the two stacks partition the error space differently, so
        // every address that lands in this arm on one lands in Refused or Unreachable on the other.
        // Sixteen candidates were measured to establish that, including the limited broadcast address
        // this test used to dial — Windows answers it AddressNotAvailable, Linux NetworkUnreachable.
        //
        // So the stimulus is chosen per platform and the assertion is not: both addresses below are
        // rejected before the connect goes anywhere, both resolve by the first Pump, and both must
        // produce exactly CloseReason.Failed. Neither consults a routing table, which is what keeps
        // this deterministic where an ordinary unrouted address is not.
        //   Windows — the limited broadcast address, rejected as AddressNotAvailable.
        //   Linux   — a link-local address with no scope id, which names no interface and so cannot
        //             identify a destination at all: rejected as InvalidArgument. Windows instead
        //             accepts it and leaves the dial in flight, which is why it cannot serve for both.
        //
        // Using an IPv6 address here does not make this test depend on IPv6 working, which is the
        // obvious objection to it. fe80::1 with no scope id is rejected by connect() as a malformed
        // argument, before any address or route lookup, so its answer does not move with the host's
        // IPv6 configuration — measured identically with IPv6 up, with net.ipv6.conf.all.disable_ipv6
        // set, and with no network interfaces at all:
        //
        //                     | IPv6 up            | IPv6 disabled       | no network
        //   fe80::1           | InvalidArgument    | InvalidArgument     | InvalidArgument
        //   fe80::1%1         | NetworkUnreachable | AddressNotAvailable | NetworkUnreachable
        //   100::1            | NetworkUnreachable | AddressNotAvailable | NetworkUnreachable
        //   ::1               | Refused            | AddressNotAvailable | Refused
        //
        // Every other row moves; that is the whole reason this one was chosen over the others.
        using RawReactorHarness harness = new();
        IPEndPoint rejectedEndPoint = OperatingSystem.IsWindows()
            ? new IPEndPoint(IPAddress.Broadcast, 1)
            : new IPEndPoint(IPAddress.Parse("fe80::1"), 1);

        // Act
        LinkId dialLink = harness.Reactor.Connect(rejectedEndPoint);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained = harness.PumpAndDrainReasons(pumps: 1);

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == dialLink)
            .Which.Reason.Should().Be(CloseReason.Failed);
    }

    [Fact]
    public void Connect_ReturnsLinkWithNonZeroLocalPort()
    {
        // Arrange — TCP binds the local end implicitly on connect, before the handshake resolves.
        using RawReactorHarness harness = new();
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();

        // Act
        LinkId dialLink = harness.Reactor.Connect(localEndPoint);

        // Assert
        harness.Reactor.LocalOf(dialLink).Port.Should().NotBe(0);
    }

    [Fact]
    public void SendTo_DialledStreamLink_ReturnsFailedResult()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        LinkId dialLink = harness.Reactor.Connect(localEndPoint);
        Source destination = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 12345));

        // Act
        Result result = harness.Reactor.SendTo(dialLink, destination, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public void Pump_SeveralDialsResolving_QueuesConnectedEventsInTheOrderTheDialsWereBegun()
    {
        // Arrange — three dials begun in a known order against the same listener. However many Pumps
        // they take to resolve, their Connected events must come back in dial order: the consumer has
        // nothing else to correlate them by, and a Reactor that polled its in-flight dials backwards
        // would hand back an order that looks arbitrary from the outside.
        using RawReactorHarness harness = new(capacity: 8);
        (_, IPEndPoint serverEndPoint) = harness.ListenOnLoopback();
        LinkId[] dialledInOrder =
        [
            harness.Reactor.Connect(serverEndPoint),
            harness.Reactor.Connect(serverEndPoint),
            harness.Reactor.Connect(serverEndPoint),
        ];

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.PumpAndDrainAll(5);

        // Assert
        drained.Where(evt => evt.Kind == RawEventKind.Connected).Select(evt => evt.Link)
            .Should().Equal(dialledInOrder);
    }
}
