using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.5.5 (issue #204): a retained fragment is read back by its sequence number exactly as it
/// was written — the retained length, not the whole slot — so the retransmit path resends the bytes the
/// original carried. The read-back itself is L0 and internal, so every assertion here is made where the
/// seam shows it: a <see cref="LossyProxy"/> drops the only transmission of one or more reliable
/// records, and what the receiver eventually drains is whatever came back out of retention.
/// </summary>
/// <remarks>
/// The same drops also prove subtask 6.5.2's (issue #201) non-overlap claim for adjacent slots: several
/// records of deliberately different lengths, retained side by side across both reliable Spaces, each
/// come back with their own bytes and their own length. A slot bleeding into its neighbour, or a
/// read-back returning the slot's full width rather than the length written, changes at least one
/// delivered payload.
/// </remarks>
public class DatagramSessionRetentionReadBackTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved (matches
    /// <see cref="DatagramSessionRetransmitResendTests"/>).
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Pump_SeveralDroppedRecordsRetainedSideBySide_EachResendCarriesItsOwnBytesAndItsOwnLength()
    {
        // Arrange — five records of five different lengths, spread across both reliable Spaces so
        // neighbouring slots within a Space and the two Spaces' runs are both covered. Retention sized
        // for three slots per Space, the most either Space holds here.
        const int fragmentSize = 1200;
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: fragmentSize * 3);
        using DatagramSessionReactorHarness receiver = new(retentionBytes: fragmentSize * 3);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[][] ordered = [Pattern(1, 5), Pattern(2, 17), Pattern(3, 300)];
        byte[][] unordered = [Pattern(4, 9), Pattern(5, 200)];

        // Act — every one of the five is Sent in the same Frame, so they coalesce into the one Datagram
        // this drop takes off the wire; only a read-back out of retention can deliver any of them.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        foreach (byte[] message in ordered)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
        }

        foreach (byte[] message in unordered)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message).Should().Be(SendOutcome.Sent);
        }

        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        receiver.DrainPayloads().Should().BeEmpty("the one Datagram carrying all five records was dropped");

        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 16);

        // Assert — each record's own bytes, at its own length. A slot read back at the full fragment
        // width, or overlapping its neighbour, fails here rather than merely delivering the wrong count.
        delivered.Should().HaveCount(5, "each dropped record is resent once and delivered once");
        foreach (byte[] expected in ordered.Concat(unordered))
        {
            delivered.Should().ContainSingle(
                actual => actual.SequenceEqual(expected),
                "the record of {0} bytes must come back out of retention byte-for-byte",
                expected.Length);
        }
    }

    [Fact]
    public void Pump_ADroppedRecordExactlyFillingItsSlot_ArrivesByteIdenticalOnTheResend()
    {
        // Arrange — the boundary case of the read-back's "exactly the length written": a payload that
        // fills its slot completely, where an off-by-one in either direction is a truncation or a read
        // into the neighbouring slot.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness receiver = new(
            fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] exactlyOneSlot = Pattern(6, fragmentSize);

        // Act
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, exactlyOneSlot).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        receiver.DrainPayloads().Should().BeEmpty("the only transmission was dropped");

        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 16);

        // Assert
        delivered.Should().ContainSingle("the dropped Message is delivered once, by the resend")
            .Which.Should().Equal(exactlyOneSlot, "a slot filled to its width reads back at its width, neither short nor long");
    }

    /// <summary>
    /// A payload of <paramref name="length"/> bytes whose contents depend on <paramref name="seed"/>, so
    /// no two payloads in one test are equal even where their lengths would allow it.
    /// </summary>
    private static byte[] Pattern(byte seed, int length)
    {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++)
        {
            payload[i] = (byte)(seed + i);
        }

        return payload;
    }

    /// <summary>
    /// Pumps the three parties in a round <paramref name="rounds"/> times, collecting every payload the
    /// receiver drains along the way. A fixed round count rather than a wait-for-condition loop, for the
    /// reason <see cref="DatagramSessionRetransmitResendTests"/> gives: a test asserting an exact
    /// delivered count must keep Pumping past the arrivals to catch a duplicate.
    /// </summary>
    private static List<byte[]> PumpRoundsCollectingPayloads(
        DatagramSessionReactorHarness sender,
        LossyProxy proxy,
        DatagramSessionReactorHarness receiver,
        int rounds)
    {
        List<byte[]> payloads = [];
        for (int round = 0; round < rounds; round++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            payloads.AddRange(receiver.DrainPayloads());
            sender.DrainAndDiscard();
            PumpDeadline.Yield();
        }

        return payloads;
    }
}
