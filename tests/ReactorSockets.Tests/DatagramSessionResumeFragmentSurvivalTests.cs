using System.Buffers.Binary;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #387 (parent #56): a Fragment set that is half arrived, with the arrived half acknowledged to the
/// sender, when the path is cut, completes after a wire-level Resume from a new port. The Message is
/// delivered once and whole, and the sender does not resend a Fragment that was acknowledged before the cut.
/// The model for the wire assertion and the Fragment sizing is
/// <see cref="DatagramSessionFragmentSurvivesInterruptionTests"/>, which predates Resume and models the
/// interruption as an outage only.
/// </summary>
/// <remarks>
/// The dialler's captured datagrams (<see cref="LossyProxy.Captures"/> records them on arrival, dropped or not)
/// are decoded by hand, as the model test does: <see cref="DatagramDecoder"/> reads only unfragmented records.
/// Fragments 0 and 1 are let through and Fragments 2 and 3, originals and retransmissions, are dropped by a
/// payload rule until the cut. <see cref="FragmentSize"/> puts one Fragment record in each datagram.
/// </remarks>
public class DatagramSessionResumeFragmentSurvivalTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;
    private const int OrderedAckBlockOffset = 4;
    private const int MaxUdpDatagramSize = 65_507;

    /// <summary>One Fragment record leaves under 12 spare bytes in a datagram, so every Fragment rides alone.</summary>
    private const int FragmentSize = MaxUdpDatagramSize - DatagramHeaderSize - FragmentRecordSize - 11;

    private const int FragmentCount = 4;
    private const int FragmentsBeforeInterruption = FragmentCount / 2;
    private const int LastFragmentRemainder = 1_000;
    private const int MessageSize = (FragmentSize * (FragmentCount - 1)) + LastFragmentRemainder;

    /// <summary>Room for the whole set plus the Pings each end retains while the path is cut.</summary>
    private const int RetentionBytes = FragmentSize * (FragmentCount + 4);

    private const int MaxSteps = 100;

    [Fact]
    public void Pump_FragmentSetHalfArrivedAndAckedWhenCutThenResumed_CompletesTheMessageOnceAfterTheResume()
    {
        // Arrange
        using Scenario scenario = EstablishScenario();
        byte[] message = SendAndCutWithHalfTheSetAcked(scenario, out _, out _);
        scenario.AccepterPayloads.Should().BeEmpty("only half the set arrived before the cut");

        // Act
        scenario.ResumeThroughNewPort();
        StepUntil(scenario, () => scenario.AccepterPayloads.Count > 0);
        for (int i = 0; i < 20; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        // Assert
        scenario.AccepterPayloads.Should().ContainSingle("the completed set is one Message, delivered once")
            .Which.Should().Equal(message);
    }

    [Fact]
    public void Pump_FragmentSetHalfArrivedAndAckedWhenCutThenResumed_NoFragmentAckedBeforeTheCutIsSentAgain()
    {
        // Arrange
        using Scenario scenario = EstablishScenario();
        SendAndCutWithHalfTheSetAcked(scenario, out HashSet<uint> ackedSequences, out int captureMark);

        // Act
        scenario.ResumeThroughNewPort();
        StepUntil(scenario, () => scenario.AccepterPayloads.Count > 0);

        // Assert
        List<uint> sentSinceTheAck = [.. scenario.Proxy.Captures
            .Skip(captureMark)
            .Where(c => c.Direction == RelayDirection.ClientToUpstream)
            .SelectMany(c => DecodeFragments(c.Payload))
            .Select(f => f.Sequence)];
        sentSinceTheAck.Should().NotContain(
            [.. ackedSequences], "a Fragment acknowledged before the cut is not resent, on the wire, by the Resume or after it");
        sentSinceTheAck.Should().Contain(
            (uint)(ackedSequences.Max() + 1), "the still-missing Fragments are what the resumed sender carries");
    }

    private static Scenario EstablishScenario() => Scenario.Establish(
        Scenario.ResumableLinger,
        maxMessageSize: MessageSize,
        retentionBytes: RetentionBytes,
        fragmentSize: FragmentSize,
        arenaSize: MessageSize,
        receiveWindowFragments: FragmentCount * 2);

    /// <summary>
    /// Sends one Message, lets Fragments 0 and 1 through while Fragments 2 and 3 are dropped, steps until a
    /// datagram the accepter sent has reached the dialler acknowledging Fragment 1, then cuts both ends.
    /// </summary>
    /// <param name="ackedSequences">The sequences of the Fragments acknowledged before the cut, read off the wire.</param>
    /// <param name="captureMark">The number of captures when the acknowledgement was proved, so later assertions read only what the dialler sent after it.</param>
    private static byte[] SendAndCutWithHalfTheSetAcked(Scenario scenario, out HashSet<uint> ackedSequences, out int captureMark)
    {
        // Live traffic first, so the dialler has a round-trip estimate: without one a Fragment is retransmitted only after a whole Linger.
        for (int i = 0; i < 4 * (int)(Scenario.SendIdleInterval / Scenario.StepInterval); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        byte[] message = new byte[MessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        scenario.Proxy.SetPayloadDropRule(
            RelayDirection.ClientToUpstream,
            datagram => DecodeFragments(datagram).Any(f => f.Index >= FragmentsBeforeInterruption));
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, message)
            .Should().Be(SendOutcome.Sent, "retention was sized to hold every Fragment plus the Pings");

        StepUntil(scenario, () => scenario.Proxy.Captures.Any(c => c.Direction == RelayDirection.ClientToUpstream
            && DecodeFragments(c.Payload).Any(f => f.Index == FragmentsBeforeInterruption - 1)));
        ackedSequences = [.. scenario.Proxy.Captures
            .Where(c => c.Direction == RelayDirection.ClientToUpstream)
            .SelectMany(c => DecodeFragments(c.Payload))
            .Where(f => f.Index < FragmentsBeforeInterruption)
            .Select(f => f.Sequence)];
        ackedSequences.Should().HaveCount(FragmentsBeforeInterruption);
        uint highestAcked = ackedSequences.Max();

        // The ReliableOrdered ack block's base is the next sequence the accepter expects, so a base above the
        // highest acknowledged Fragment's sequence in a datagram forwarded to the dialler proves the ack reached the sender.
        StepUntil(scenario, () => scenario.Proxy.Forwarded.Any(c => c.Direction == RelayDirection.UpstreamToClient
            && c.Payload.Length >= DatagramHeaderSize
            && BinaryPrimitives.ReadUInt32BigEndian(c.Payload) != 0
            && BinaryPrimitives.ReadUInt32BigEndian(c.Payload.AsSpan(OrderedAckBlockOffset)) > highestAcked));
        scenario.Step(Scenario.StepInterval);
        captureMark = scenario.Proxy.Captures.Count;
        scenario.AccepterPayloads.Should().BeEmpty("the set is only half arrived");

        scenario.InterruptBothEnds();
        return message;
    }

    private static void StepUntil(Scenario scenario, Func<bool> condition)
    {
        for (int i = 0; i < MaxSteps && !condition(); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        condition().Should().BeTrue("the condition should hold within the step budget");
    }

    /// <summary>Reads every Fragment record's sequence and index out of one captured datagram, ignoring control datagrams and unfragmented records.</summary>
    private static IEnumerable<(uint Sequence, ushort Index)> DecodeFragments(byte[] datagram)
    {
        if (datagram.Length < DatagramHeaderSize || BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4)) == 0)
        {
            yield break;
        }

        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;
            if (isFragment)
            {
                yield return (sequence, BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 7, 2)));
            }

            offset += (isFragment ? FragmentRecordSize : UnfragmentedRecordSize) + length;
        }
    }
}
