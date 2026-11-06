using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The peer's announced <see cref="DatagramSessionOptions.MaxMessageSize"/> is the boundary
/// <see cref="DatagramSessionReactor.Send"/> enforces, not this Reactor's own.
/// <see cref="DatagramSessionAsymmetricMessageSizeTests"/> proves that with the dialler sending toward
/// the accepter's smaller announced limit. This file proves the same rule in the other direction. Checking only one direction would leave a Reactor that reads
/// its own <see cref="DatagramSessionOptions.MaxMessageSize"/> instead of the peer's undetected whenever
/// that Reactor happens to be the one with the larger limit — the bug is symmetric, so the proof has to
/// be. Neither test asserts how many Fragments a Message split into — only the <see cref="SendOutcome"/>
/// and the delivered Message matter.
/// </summary>
public class DatagramSessionAsymmetricDirectionTests
{
    /// <summary>
    /// A payload under this Reactor's own limit but over the peer's, chosen once and shared by both
    /// tests below so the same number proves both shapes: sent by the dialler it is under the dialler's
    /// own 4096 but over the accepter's announced 1200, and sent by the accepter it is over the
    /// accepter's own 1200 but under the dialler's announced 4096. One size, both directions, both of
    /// the shapes issue #291 asks for.
    /// </summary>
    private const int PayloadSize = 2000;

    [Fact]
    public void Send_FromTheLargerLimitEndTowardTheSmallerAnnouncedLimit_ReturnsRemoteMessageTooLargeAndNothingArrives()
    {
        // Arrange — 2000 bytes is under the dialler's own 4096 but over the accepter's announced 1200. A
        // Reactor that checked its own MaxMessageSize instead of the peer's announced one would accept
        // this; only checking the peer's announced limit refuses it. This is the same direction #290
        // already covers; it is asserted again here only as the paired half of the test below, sharing
        // the one PayloadSize that proves both directions.
        using MismatchedMessageSizePair pair = MismatchedMessageSizePair.Build();
        byte[] overAccepterLimit = new byte[PayloadSize];

        // Act
        SendOutcome outcome = pair.Dialler.Reactor.Send(pair.DiallerPeer, DeliveryMode.ReliableOrdered, overAccepterLimit);
        (_, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents) =
            DatagramSessionReactorHarness.PumpBothRounds(pair.Dialler, pair.Accepter, rounds: 5);

        // Assert
        outcome.Should().Be(
            SendOutcome.RemoteMessageTooLarge,
            "2000 bytes exceeds the accepter's own announced 1200-byte limit even though it is under the dialler's own 4096");
        accepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.MessageReceived, "a refused Send queues no record, so nothing ever reaches the far end");
    }

    [Fact]
    public void Send_FromTheSmallerLimitEndTowardTheLargerAnnouncedLimit_ReturnsSentAndDeliversWholeEvenThoughItExceedsTheSendersOwnLimit()
    {
        // Arrange — the decisive case: 2000 bytes is OVER the accepter's own announced 1200, which is
        // exactly what the accepter would refuse if it validated a Send against its own MaxMessageSize.
        // It is also UNDER the dialler's announced 4096, so the only way this Send can complete is by
        // checking the peer's limit instead of the sender's own. A Reactor still applying its own limit
        // to an outbound Send would refuse this even though the actual receiver has room for it.
        using MismatchedMessageSizePair pair = MismatchedMessageSizePair.Build();
        byte[] overAccepterOwnLimit = Enumerable.Range(0, PayloadSize).Select(i => (byte)i).ToArray();

        // Act — reassembly across several Fragments takes more than one round, so the wait is bounded by
        // a clock, not a round count.
        SendOutcome outcome = pair.Accepter.Reactor.Send(pair.AccepterPeer, DeliveryMode.ReliableOrdered, overAccepterOwnLimit);
        List<byte[]> delivered = pair.PumpUntilDeliveredTo(pair.Dialler);

        // Assert
        outcome.Should().Be(
            SendOutcome.Sent,
            "2000 bytes is over the accepter's own 1200-byte limit but under the dialler's announced 4096, so the Send is bound by the receiver's limit, not the sender's own");
        delivered.Should().ContainSingle().Which.Should().Equal(
            overAccepterOwnLimit, "the bytes that arrive at the dialler are the exact bytes Sent, not a truncated or padded copy");
    }
}
