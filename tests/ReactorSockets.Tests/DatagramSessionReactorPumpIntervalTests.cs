using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="DatagramSessionReactor"/> now owns a <see cref="PumpIntervalObserver"/>,
/// observed exactly once per <see cref="DatagramSessionReactor.Pump"/>, mirroring
/// <see cref="StreamSessionReactor"/>'s own use of the same type. The reading itself —
/// unseeded <c>TimeSpan.MaxValue</c>, most-recent-cadence tracking, wall-clock independence — is
/// already pinned once, against the observer directly, in <see cref="PumpIntervalObserverTests"/>;
/// re-asserting it here through a full Reactor would duplicate that contract rather than test
/// anything new. What this file pins instead is that wiring the observation into the datagram
/// Reactor's Pump body is safe: the Reactor stays usable, pumping at varying cadences under a
/// substituted <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/> raises no
/// Warn-or-above diagnostic, and a steady-state run of Pumps allocates nothing beyond warmup —
/// <c>DatagramSessionReactor.ObservedPumpInterval</c> is <see langword="internal"/> with deliberately
/// no <c>InternalsVisibleTo</c> (ADR 0015), so its value cannot be read from here at all.
/// </summary>
public class DatagramSessionReactorPumpIntervalTests
{
    [Fact]
    public void Pump_AtVaryingCadencesUnderAFakeTimeProvider_StaysUsableAndRaisesNoWarnOrAboveDiagnostic()
    {
        // Arrange — nothing connected; the observer must not disturb a Reactor with no other work to do.
        using DatagramSessionReactorHarness harness = new();

        // Act — alternate a fast cadence with a slow one, exercising the observer's
        // most-recent-cadence overwrite through the Reactor rather than asserting on it directly.
        for (int i = 0; i < 50; i++)
        {
            harness.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
            harness.Reactor.Pump();
        }

        for (int i = 0; i < 50; i++)
        {
            harness.TimeProvider.Advance(TimeSpan.FromSeconds(1));
            harness.Reactor.Pump();
        }

        // Assert — the Reactor is still usable (a throwing EnsureUsable would fail this outright) and
        // the observation itself never produced a diagnostic.
        harness.Reactor.BoundEndpoints.Should().NotBeEmpty();
        harness.Sink.Received.Should().NotContain(record => record.Level >= DiagnosticLevel.Warn);
    }

    [Fact]
    public void Pump_SteadyStateAfterWarmup_AllocatesNothingOnTheManagedHeap()
    {
        // Arrange — mirrors RawReactorPumpTests.Pump_SteadyStateAfterWarmupWithNothingReady_AllocatesNothingOnTheManagedHeap:
        // an idle Reactor's dispatch loop must reuse its scan buffers rather than reallocate every
        // Frame, and the interval observer must not be the thing that breaks that.
        using DatagramSessionReactorHarness harness = new();

        for (int warmup = 0; warmup < 10; warmup++)
        {
            harness.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
            harness.Reactor.Pump();
        }

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            harness.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
            harness.Reactor.Pump();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }
}
