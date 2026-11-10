using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Goodbye ordering. Several
/// <c>ReliableOrdered</c> Messages then <see cref="DatagramSessionReactor.Close"/> cross a
/// <see cref="LossyProxy"/> that drops the first Message's datagram. Every Message and the Goodbye go in
/// their own datagram, so the Goodbye reaches the receiver while the first Message is still missing, and
/// only the receiver's ordered delivery can hold it back. The receiver must deliver every Message,
/// including the retransmitted first one, before it raises <see cref="SessionEventKind.PeerClosing"/> or
/// <see cref="SessionEventKind.PeerClosed"/>.
/// </summary>
/// <remarks>
/// Negative controls, both run when the test was written and reverted. (1) The Goodbye was moved to
/// <c>ReliableUnordered</c>: the sender queued and packed it there, and the receiver treated an unordered
/// control record as the Goodbye and acted at admission. With the test's Goodbye-datagram check widened
/// to any control record, it failed with "Expected observed to be empty because nothing is deliverable
/// while the first Message is missing, and the Goodbye must wait behind it", so the Goodbye was
/// recognised and acted before the retransmitted Message. (2) The receiver was made to act on the
/// Goodbye as soon as it is admitted instead of at its ordered turn, and the test failed with "Expected observed to be empty because nothing is deliverable while
/// the first Message is missing, and the Goodbye must wait behind it", because <c>PeerClosed</c> arrived
/// before any Message. The receive window is widened so the Goodbye is admitted above the hole rather
/// than refused as out of window, which would otherwise hide that mutation.
/// </remarks>
public class DatagramSessionGoodbyeOrderingThroughProxyTests
{
    private const int RetentionForSeveralInFlight = 16384;
    private const int Rounds = 12;
    private const int WideReceiveWindow = 16;
    private const byte ControlBit = 0b0000_1000;

    /// <summary>
    /// Moves Close off the first Message's send time, so the sender's Linger expiry (Close plus Linger)
    /// falls after the first Message's retransmit deadline (send plus RTO). While no round trip has been
    /// sampled the RTO is the Linger itself, which is the harness default of one second.
    /// </summary>
    private static readonly TimeSpan _closeAfterFirstSend = TimeSpan.FromMilliseconds(100);

    /// <summary>Past the retransmit deadline (one second after the first send) and short of the Linger expiry (one second after Close).</summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromMilliseconds(950);

    /// <summary>One receiver-side Event in the order it was drained.</summary>
    private readonly record struct Observed(SessionEventKind Kind, DisconnectReason Reason, byte[] Payload);

    private static void PumpAndCollect(
        DatagramSessionReactorHarness sender, LossyProxy proxy, DatagramSessionReactorHarness receiver, List<Observed> observed)
    {
        sender.Reactor.Pump();
        sender.DrainToList();
        proxy.Pump();
        receiver.Reactor.Pump();
        Collect(receiver, observed);
        proxy.Pump();
        PumpDeadline.Yield();
    }

    private static void Collect(DatagramSessionReactorHarness receiver, List<Observed> observed)
    {
        foreach (SessionEvent evt in receiver.Reactor.Drain())
        {
            observed.Add(new Observed(evt.Kind, evt.Reason, evt.Kind == SessionEventKind.MessageReceived ? evt.Payload.ToArray() : []));
        }
    }

    private static bool CarriesAGoodbye(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        return decoded.IsSuccess
            && decoded.Datagram!.Records.Any(r => r.Mode == DeliveryMode.ReliableOrdered && (r.Flags & ControlBit) != 0);
    }

    [Fact]
    public void Pump_FirstMessageOfSeveralDroppedBeforeAGoodbye_DeliversEveryMessageBeforeThePeerCloses()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow, timeProvider: sender.TimeProvider);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[][] messages = [[0xA0], [0xA1], [0xA2], [0xA3]];
        List<Observed> observed = [];

        // Act — the first Message's datagram is dropped, then the rest and the Goodbye each cross in a datagram of their own.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        foreach (byte[] message in messages)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
            PumpAndCollect(sender, proxy, receiver, observed);
        }

        sender.TimeProvider.Advance(_closeAfterFirstSend);
        sender.Reactor.Close(peer);
        PumpDeadline deadline = PumpDeadline.Start();
        while (!deadline.Expired && !proxy.Forwarded.Any(f => f.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(f.Payload)))
        {
            PumpAndCollect(sender, proxy, receiver, observed);
        }

        PumpAndCollect(sender, proxy, receiver, observed);

        // Assert (precondition) — the Goodbye reached the receiver while the first Message was still missing,
        // so ordered delivery is what is holding it back.
        proxy.Forwarded.Count(f => f.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(f.Payload))
            .Should().Be(1, "the Goodbye's own datagram must have been forwarded to the receiver");
        observed.Should().BeEmpty("nothing is deliverable while the first Message is missing, and the Goodbye must wait behind it");

        // The deadline passes, so the dropped Message is resent.
        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        deadline = PumpDeadline.Start();
        while (!deadline.Expired && !observed.Any(o => o.Kind == SessionEventKind.PeerClosed))
        {
            PumpAndCollect(sender, proxy, receiver, observed);
        }

        for (int round = 0; round < Rounds; round++)
        {
            PumpAndCollect(sender, proxy, receiver, observed);
        }

        // Assert
        observed.Take(messages.Length).Select(o => o.Kind).Should().OnlyContain(
            k => k == SessionEventKind.MessageReceived,
            "every Message, including the retransmitted first one, is delivered before anything else");
        observed.Take(messages.Length).Select(o => o.Payload).Should().Equal(
            messages, (actual, expected) => actual.SequenceEqual(expected), "in send order");
        observed.Skip(messages.Length).Select(o => o.Kind).Should().Contain(SessionEventKind.PeerClosed);
        observed.Skip(messages.Length).Select(o => o.Kind).Should().OnlyContain(
            k => k == SessionEventKind.PeerClosing || k == SessionEventKind.PeerClosed);
        observed.Single(o => o.Kind == SessionEventKind.PeerClosed).Reason.Should().Be(DisconnectReason.PeerGoodbye);
    }
}
