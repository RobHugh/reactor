using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Dialling outbound TCP connections and the two dial-failure reasons — sub-issue 2.2. Every assertion
/// is against the drained <see cref="RawEvent"/>s, a <see cref="Result"/> or a
/// <see cref="DiagnosticRecord"/>; nothing here reaches into how or when a connect was detected.
/// </summary>
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
}
