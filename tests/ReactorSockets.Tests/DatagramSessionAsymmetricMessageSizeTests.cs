using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// With a dialler and an accepter announcing deliberately different
/// <see cref="DatagramSessionOptions.MaxMessageSize"/> values (see <see cref="MismatchedMessageSizePair"/>),
/// each end holds the other's announced value rather than its own. There is no public accessor for the
/// peer's announced limit, so both tests below prove it through the only seam available: what
/// <see cref="DatagramSessionReactor.Send"/> returns and what the other end eventually drains. Neither
/// test asserts how many Fragments a Message split into — only the <see cref="SendOutcome"/> and the
/// delivered Message matter.
/// </summary>
public class DatagramSessionAsymmetricMessageSizeTests
{
    [Fact]
    public void Send_AMessageExactlyAtThePeersAnnouncedLimit_RoundTripsWithByteExactPayload()
    {
        // Arrange — a Message the size of the accepter's own announced limit, which the dialler could
        // not have gotten right by checking its own (larger) MaxMessageSize: it only completes because
        // the dialler is bound by what the accepter announced during the handshake. At
        // MismatchedMessageSizePair.AccepterMaxMessageSize (1200) with a 576-byte Fragment floor, this
        // Message spans three Fragments, so reassembly on the far end is exercised too.
        using MismatchedMessageSizePair pair = MismatchedMessageSizePair.Build();
        byte[] exactlyAtLimit = Enumerable.Range(0, MismatchedMessageSizePair.AccepterMaxMessageSize).Select(i => (byte)i).ToArray();

        // Act — reassembly takes more than one Frame, so the wait is bounded by a clock, not a round count.
        SendOutcome outcome = pair.Dialler.Reactor.Send(pair.DiallerPeer, DeliveryMode.ReliableOrdered, exactlyAtLimit);
        List<byte[]> delivered = pair.PumpUntilDeliveredTo(pair.Accepter);

        // Assert
        outcome.Should().Be(SendOutcome.Sent, "a Message of exactly the peer's announced limit is one the design promises can complete");
        delivered.Should().ContainSingle().Which.Should().Equal(
            exactlyAtLimit, "the bytes that arrive at the far end are the exact bytes Sent, not a truncated or padded copy");
    }

    [Fact]
    public void Send_OneByteOverThePeersAnnouncedLimit_ReturnsRemoteMessageTooLargeAndNothingArrives()
    {
        // Arrange — one byte past the accepter's announced 1200-byte limit, but well under the dialler's
        // own 4096: a Reactor still checking its own MaxMessageSize would accept this; only checking the
        // peer's announced limit refuses it.
        using MismatchedMessageSizePair pair = MismatchedMessageSizePair.Build();
        byte[] oneByteOver = new byte[MismatchedMessageSizePair.AccepterMaxMessageSize + 1];

        // Act
        SendOutcome outcome = pair.Dialler.Reactor.Send(pair.DiallerPeer, DeliveryMode.ReliableOrdered, oneByteOver);
        (_, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents) =
            DatagramSessionReactorHarness.PumpBothRounds(pair.Dialler, pair.Accepter, rounds: 5);

        // Assert
        outcome.Should().Be(
            SendOutcome.RemoteMessageTooLarge,
            "1201 bytes exceeds the peer's own announced 1200-byte limit even though it is far inside this Reactor's own 4096");
        accepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.MessageReceived, "a refused Send queues no record, so nothing ever reaches the far end");
    }
}
