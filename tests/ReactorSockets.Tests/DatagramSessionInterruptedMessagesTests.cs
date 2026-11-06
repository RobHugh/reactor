using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Messages each end sends in a reliable mode while its Peer is Interrupted
/// reach the other end after the Resume through a new port, exactly once, and none before it.
/// ReliableOrdered Messages also arrive in send order.
/// </summary>
public class DatagramSessionInterruptedMessagesTests
{
    private const int MessagesPerEnd = 6;
    private const int DuplicatedRelays = 40;
    private const int RetentionBytes = 64 * 1024;

    [Fact]
    public void Pump_ReliableOrderedSentByBothEndsWhileInterrupted_ArriveOnceInSendOrderAfterTheResume()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger, retentionBytes: RetentionBytes);
        scenario.InterruptBothEnds();

        // Act
        List<byte[]> fromDialler = SendAll(scenario.Dialler, scenario.DiallerPeer, DeliveryMode.ReliableOrdered, 0xD0);
        List<byte[]> fromAccepter = SendAll(scenario.Accepter, scenario.AccepterPeer, DeliveryMode.ReliableOrdered, 0xA0);
        DuplicateRelays(scenario);
        scenario.Step(Scenario.StepInterval);
        scenario.ResumeThroughNewPort();
        StepUntilDelivered(scenario, fromDialler.Count, fromAccepter.Count);

        // Assert
        AssertDeliveredExactlyInOrder(scenario.AccepterReceived, fromDialler);
        AssertDeliveredExactlyInOrder(scenario.DiallerReceived, fromAccepter);
        AssertDuplicatesWereInjected(scenario);
    }

    [Fact]
    public void Pump_ReliableUnorderedSentByBothEndsWhileInterrupted_ArriveOnceEachAfterTheResume()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger, retentionBytes: RetentionBytes);
        scenario.InterruptBothEnds();

        // Act
        List<byte[]> fromDialler = SendAll(scenario.Dialler, scenario.DiallerPeer, DeliveryMode.ReliableUnordered, 0xD1);
        List<byte[]> fromAccepter = SendAll(scenario.Accepter, scenario.AccepterPeer, DeliveryMode.ReliableUnordered, 0xA1);
        DuplicateRelays(scenario);
        scenario.Step(Scenario.StepInterval);
        scenario.ResumeThroughNewPort();
        StepUntilDelivered(scenario, fromDialler.Count, fromAccepter.Count);

        // Assert
        AssertDeliveredExactlyOnce(scenario.AccepterReceived, fromDialler);
        AssertDeliveredExactlyOnce(scenario.DiallerReceived, fromAccepter);
        AssertDuplicatesWereInjected(scenario);
    }

    /// <summary>Queues duplicates of the next relayed datagrams in both directions, so the Resume and the retransmissions each arrive twice.</summary>
    private static void DuplicateRelays(Scenario scenario)
    {
        for (int i = 0; i < DuplicatedRelays; i++)
        {
            scenario.Proxy.DuplicateNext(RelayDirection.ClientToUpstream);
            scenario.Proxy.DuplicateNext(RelayDirection.UpstreamToClient);
        }
    }

    /// <summary>Guards against a vacuous pass: both ends really received repeated records, counted as Duplicate inside the receive window and as OutOfWindow once it has slid past.</summary>
    private static void AssertDuplicatesWereInjected(Scenario scenario)
    {
        PeerCounters accepter = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);
        PeerCounters dialler = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
        (accepter.DuplicatePackets + accepter.OutOfWindowPackets).Should().BeGreaterThan(0);
        (dialler.DuplicatePackets + dialler.OutOfWindowPackets).Should().BeGreaterThan(0);
    }

    /// <summary>Sends <see cref="MessagesPerEnd"/> Messages with distinct content, asserting each Send was accepted.</summary>
    private static List<byte[]> SendAll(DatagramSessionReactorHarness harness, PeerId peer, DeliveryMode mode, byte tag)
    {
        List<byte[]> sent = [];
        for (byte i = 0; i < MessagesPerEnd; i++)
        {
            byte[] payload = [tag, i, (byte)((i * 7) + 3), 0x5A];
            harness.Reactor.Send(peer, mode, payload).Should().Be(SendOutcome.Sent);
            sent.Add(payload);
        }

        return sent;
    }

    /// <summary>Steps until each end has received its expected count, then a few more rounds so a duplicate would show.</summary>
    private static void StepUntilDelivered(Scenario scenario, int toAccepter, int toDialler)
    {
        for (int i = 0; i < 300 && (scenario.AccepterReceived.Count < toAccepter || scenario.DiallerReceived.Count < toDialler); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        for (int i = 0; i < 30; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }
    }

    private static void AssertDeliveredExactlyOnce(List<(byte[] Payload, int ResumedBefore)> received, List<byte[]> sent)
    {
        received.Select(r => Convert.ToHexString(r.Payload)).Should().BeEquivalentTo(sent.Select(Convert.ToHexString));
        received.Should().OnlyContain(r => r.ResumedBefore >= 1, "nothing is delivered while the path is cut");
    }

    private static void AssertDeliveredExactlyInOrder(List<(byte[] Payload, int ResumedBefore)> received, List<byte[]> sent)
    {
        received.Select(r => Convert.ToHexString(r.Payload)).Should().Equal(sent.Select(Convert.ToHexString));
        received.Should().OnlyContain(r => r.ResumedBefore >= 1, "nothing is delivered while the path is cut");
    }
}
