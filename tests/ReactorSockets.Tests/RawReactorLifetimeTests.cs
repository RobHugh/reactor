using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Disposal and re-entrancy: the two ways a consumer can hold a Reactor wrongly. A Reactor owns every
/// socket it bound, and it refuses to be driven from inside its own Pump.
/// </summary>
public class RawReactorLifetimeTests
{
    [Fact]
    public void Dispose_WithAnOpenLink_ReleasesItsSocketSoThePortCanBeReboundElsewhere()
    {
        // Arrange — binds are exclusive, so a second Reactor can only take the port if the first
        // Reactor really closed the socket rather than leaving it to the finalizer.
        ReactorOptions options = new() { Capacity = 2 };
        int pinnedPort;
        using (RawReactor first = new(options))
        {
            LinkId link = first.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            pinnedPort = first.LocalOf(link).Port;
        }

        // Act
        using RawReactor second = new(options);
        Action act = () => second.Bind(new IPEndPoint(IPAddress.Loopback, pinnedPort));

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        // Arrange
        RawReactor reactor = new(new ReactorOptions { Capacity = 1 });
        reactor.Dispose();

        // Act
        Action act = reactor.Dispose;

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Pump_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        RawReactor reactor = new(new ReactorOptions { Capacity = 1 });
        reactor.Dispose();

        // Act
        Action act = reactor.Pump;

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Bind_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        RawReactor reactor = new(new ReactorOptions { Capacity = 1 });
        reactor.Dispose();

        // Act
        Action act = () => reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Pump_SinkThatPumpsTheReactorItIsReportingOn_ThrowsAtTheSink()
    {
        // Arrange
        ReentrantDiagnosticsSink sink = new() { ReentrantCall = reactor => reactor.Pump() };
        using RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: sink);
        sink.Reactor = harness.Reactor;

        // Act
        harness.ProvokeTruncation();

        // Assert — the message is asserted, not just the type: a nested Pump would throw
        // InvalidOperationException anyway once it mutated the scan list mid-iteration, so type alone
        // would pass with no guard at all.
        sink.Observed.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("may not call back into the Reactor");
    }

    [Fact]
    public void Bind_FromInsideASinkCallback_ThrowsAtTheSink()
    {
        // Arrange — the guard is on re-entry itself, not on Pump specifically: no member may be
        // reached from inside a Frame.
        ReentrantDiagnosticsSink sink = new()
        {
            ReentrantCall = reactor => reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0)),
        };
        using RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: sink);
        sink.Reactor = harness.Reactor;

        // Act
        harness.ProvokeTruncation();

        // Assert
        sink.Observed.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("may not call back into the Reactor");
    }

    [Fact]
    public void Pump_SinkThatLetsTheReentrancyThrowEscape_DoesNotLetItReachTheConsumer()
    {
        // Arrange
        using RawReactorHarness harness = EscapingReentrantSinkHarness(out _);

        // Act
        Action act = () =>
        {
            harness.ProvokeTruncation();
            harness.ProvokeTruncation();
        };

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Log_SinkThatLetsTheReentrancyThrowEscape_IsInvokedExactlyOnce()
    {
        // Arrange
        using RawReactorHarness harness = EscapingReentrantSinkHarness(out ReentrantDiagnosticsSink sink);

        // Act
        harness.ProvokeTruncation();
        harness.ProvokeTruncation();

        // Assert
        sink.CallCount.Should().Be(1);
    }

    [Fact]
    public void Counters_SinkThatLetsTheReentrancyThrowEscape_ReportSinkFaultedExactlyOnce()
    {
        // Arrange
        using RawReactorHarness harness = EscapingReentrantSinkHarness(out _);

        // Act
        harness.ProvokeTruncation();
        harness.ProvokeTruncation();

        // Assert
        harness.Reactor.Counters.SinkFaulted.Should().Be(1);
    }

    /// <summary>
    /// A Reactor whose Sink re-enters it and lets the resulting exception escape. Such a Sink has
    /// thrown, so it must fault and be disabled like any other misbehaving Sink rather than taking the
    /// Frame down with it — which is three separate facts, one per test above.
    /// </summary>
    private static RawReactorHarness EscapingReentrantSinkHarness(out ReentrantDiagnosticsSink sink)
    {
        sink = new ReentrantDiagnosticsSink
        {
            ReentrantCall = reactor => reactor.Pump(),
            LetTheThrowEscape = true,
        };

        RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: sink);
        sink.Reactor = harness.Reactor;
        return harness;
    }
}
