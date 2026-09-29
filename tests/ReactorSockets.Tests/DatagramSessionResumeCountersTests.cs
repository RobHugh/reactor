using System.Reflection;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #389 (parent #56, inherited from #231): a Resume carries no history away. The whole
/// <see cref="PeerCounters"/> snapshot on each end, taken just before and just after a Resume, only grows,
/// and <see cref="PeerCounters.Resumes"/> rises by exactly one.
/// </summary>
/// <remarks>
/// The fields are enumerated by reflection, so a field added to <see cref="PeerCounters"/> later is covered
/// without editing this file. A field of a type this file cannot compare fails the test rather than being skipped.
/// </remarks>
public class DatagramSessionResumeCountersTests(ITestOutputHelper output)
{
    private static readonly PropertyInfo[] _fields = [.. typeof(PeerCounters)
        .GetProperties(BindingFlags.Public | BindingFlags.Instance)];

    [Fact]
    public void Pump_MatchedResume_EveryPeerCountersFieldOnBothEndsOnlyGrowsAndResumesRisesByOne()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        DriveTraffic(scenario);
        scenario.InterruptBothEnds();
        PeerCounters diallerBefore = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
        PeerCounters accepterBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);

        // Act
        scenario.ResumeThroughNewPort();
        PeerCounters diallerAfter = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
        PeerCounters accepterAfter = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);

        // Assert
        Report("dialler before", diallerBefore);
        Report("dialler after", diallerAfter);
        Report("accepter before", accepterBefore);
        Report("accepter after", accepterAfter);
        AssertGuardOnFieldSet();
        AssertNonTrivialBefore(diallerBefore);
        AssertNonTrivialBefore(accepterBefore);
        AssertSurvivesResume("dialler", diallerBefore, diallerAfter);
        AssertSurvivesResume("accepter", accepterBefore, accepterAfter);
    }

    private static void DriveTraffic(Scenario scenario)
    {
        byte[] payload = new byte[100];
        for (int i = 0; i < 3; i++)
        {
            scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, payload)
                .Should().Be(SendOutcome.Sent);
            scenario.Accepter.Reactor.Send(scenario.AccepterPeer, DeliveryMode.ReliableOrdered, payload)
                .Should().Be(SendOutcome.Sent);
            scenario.Step(Scenario.StepInterval);
        }

        for (int i = 0; i < 10; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }
    }

    private static void AssertGuardOnFieldSet()
    {
        _fields.Select(f => f.Name).Should().Contain(
            [nameof(PeerCounters.BytesSent), nameof(PeerCounters.BytesReceived), nameof(PeerCounters.Resumes)],
            "the reflected field set must not be empty or lose the fields this test is about");
        _fields.Where(f => f.PropertyType != typeof(ulong)).Select(f => $"{f.Name}: {f.PropertyType}")
            .Should().BeEmpty("every field must be a ulong, the only type this test knows how to compare");
    }

    private static void AssertNonTrivialBefore(PeerCounters before)
    {
        ReadAll(before).Count(f => f.Value > 0).Should().BeGreaterThan(3, "traffic came first, so several counters are non-zero before the Resume");
        before.BytesSent.Should().BeGreaterThan(0);
        before.BytesReceived.Should().BeGreaterThan(0);
        before.MessagesReceived.Should().BeGreaterThan(0);
        before.SocketWrites.Should().BeGreaterThan(0);
    }

    private static void AssertSurvivesResume(string end, PeerCounters before, PeerCounters after)
    {
        Dictionary<string, ulong> b = ReadAll(before);
        Dictionary<string, ulong> a = ReadAll(after);
        foreach (string name in b.Keys)
        {
            if (name == nameof(PeerCounters.Resumes))
            {
                a[name].Should().Be(b[name] + 1, $"{end}: Resumes rises by exactly one");
            }
            else
            {
                a[name].Should().BeGreaterThanOrEqualTo(b[name], $"{end}: {name} must not fall across a Resume");
            }
        }
    }

    private static Dictionary<string, ulong> ReadAll(PeerCounters counters) =>
        _fields.ToDictionary(f => f.Name, f => (ulong)f.GetValue(counters)!);

    private void Report(string label, PeerCounters counters) =>
        output.WriteLine($"{label}: " + string.Join(", ", ReadAll(counters).Select(kv => $"{kv.Key}={kv.Value}")));
}
