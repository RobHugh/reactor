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
    public void Pump_SinkThatLetsTheReentrancyThrowEscape_IsFaultedAndTheFrameStillCompletes()
    {
        // Arrange — a Sink that lets the exception out has thrown, so it faults like any other
        // misbehaving Sink rather than taking the Frame down with it.
        ReentrantDiagnosticsSink sink = new()
        {
            ReentrantCall = reactor => reactor.Pump(),
            LetTheThrowEscape = true,
        };
        using RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: sink);
        sink.Reactor = harness.Reactor;

        // Act
        Action act = () =>
        {
            harness.ProvokeTruncation();
            harness.ProvokeTruncation();
        };

        // Assert
        act.Should().NotThrow();
        sink.CallCount.Should().Be(1);
        harness.Reactor.Counters.SinkFaulted.Should().Be(1);
    }
}
