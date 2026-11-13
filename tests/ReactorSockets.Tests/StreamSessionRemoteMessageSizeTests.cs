using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="StreamSessionReactor.Send(PeerId, ReadOnlySpan{byte})"/> refuses a Message longer than the
/// <c>MaxMessageSize</c> the Peer announced in its Hello with <see cref="SendOutcome.RemoteMessageTooLarge"/>,
/// copies nothing, and leaves the Session open on both ends (issue #484). A Message longer than this
/// Reactor's own <see cref="StreamSessionOptions.MaxMessageSize"/> still throws, and a Send before the
/// handshake completes still returns <see cref="SendOutcome.NotEstablished"/>, whatever its length.
/// </summary>
public class StreamSessionRemoteMessageSizeTests
{
    private const int LargeLimit = 1000;

    private const int SmallLimit = 100;

    /// <summary>
    /// Dials <paramref name="accepter"/> from <paramref name="dialler"/>, pumps both until each has raised
    /// <see cref="SessionEventKind.PeerConnected"/>, and returns each end's id for the other.
    /// </summary>
    private static (PeerId DiallerSide, PeerId AccepterSide) Establish(
        StreamSessionReactorHarness dialler,
        StreamSessionReactorHarness accepter)
    {
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) = StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        PeerId diallerSide = diallerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected).Which.Peer;
        PeerId accepterSide = accepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected).Which.Peer;
        return (diallerSide, accepterSide);
    }

    /// <summary>
    /// Pumps <paramref name="sender"/> then <paramref name="receiver"/> until <paramref name="receiver"/>
    /// has drained one <see cref="SessionEventKind.MessageReceived"/>, and returns everything each end
    /// drained. Payloads are copied out, since they alias the frame arena.
    /// </summary>
    private static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> Sender,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> Receiver)
        PumpUntilReceived(StreamSessionReactorHarness sender, StreamSessionReactorHarness receiver)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> senderEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> receiverEvents = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            sender.Reactor.Pump();
            senderEvents.AddRange(sender.DrainToListWithPayload());
            receiver.Reactor.Pump();
            receiverEvents.AddRange(receiver.DrainToListWithPayload());
            if (receiverEvents.Any(e => e.Kind == SessionEventKind.MessageReceived) || deadline.Expired)
            {
                return (senderEvents, receiverEvents);
            }

            PumpDeadline.Yield();
        }
    }

    [Fact]
    public void Send_AboveThePeersAnnouncedLimit_ReturnsRemoteMessageTooLargeAndTheSessionStaysOpen()
    {
        // Arrange
        using StreamSessionReactorHarness sender = new(bindLoopback: false, maxMessageSize: LargeLimit, retentionBytes: LargeLimit + 4);
        using StreamSessionReactorHarness receiver = new(maxMessageSize: SmallLimit, retentionBytes: SmallLimit + 4);
        (PeerId senderSide, _) = Establish(sender, receiver);

        // Act
        SendOutcome refused = sender.Reactor.Send(senderSide, StreamSessionTestRecords.PayloadOf(500, 0xAA));
        SendOutcome accepted = sender.Reactor.Send(senderSide, StreamSessionTestRecords.PayloadOf(SmallLimit, 0xBB));
        (var senderEvents, var receiverEvents) = PumpUntilReceived(sender, receiver);

        // Assert
        refused.Should().Be(SendOutcome.RemoteMessageTooLarge);
        accepted.Should().Be(SendOutcome.Sent);
        receiverEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.MessageReceived)
            .Which.Payload.Should().Equal(StreamSessionTestRecords.PayloadOf(SmallLimit, 0xBB));
        senderEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted);
        receiverEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted);
        receiver.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.MalformedFraming);
    }

    [Fact]
    public void Send_FromTheAccepterOneByteAboveTheDiallersAnnouncedLimit_ReturnsRemoteMessageTooLarge()
    {
        // Arrange
        using StreamSessionReactorHarness dialler = new(bindLoopback: false, maxMessageSize: SmallLimit, retentionBytes: SmallLimit + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: LargeLimit, retentionBytes: LargeLimit + 4);
        (_, PeerId accepterSide) = Establish(dialler, accepter);

        // Act
        SendOutcome outcome = accepter.Reactor.Send(accepterSide, StreamSessionTestRecords.PayloadOf(SmallLimit + 1, 0xCC));

        // Assert
        outcome.Should().Be(SendOutcome.RemoteMessageTooLarge);
    }

    [Fact]
    public void Send_AboveBothLimits_ThrowsForTheOwnLimit()
    {
        // Arrange
        using StreamSessionReactorHarness sender = new(bindLoopback: false, maxMessageSize: LargeLimit, retentionBytes: LargeLimit + 4);
        using StreamSessionReactorHarness receiver = new(maxMessageSize: SmallLimit, retentionBytes: SmallLimit + 4);
        (PeerId senderSide, _) = Establish(sender, receiver);

        // Act
        Action act = () => sender.Reactor.Send(senderSide, new byte[LargeLimit + 1]);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName("message");
    }

    [Fact]
    public void Send_AboveAnyRemoteLimitBeforeTheHandshakeCompletes_ReturnsNotEstablished()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: SmallLimit, retentionBytes: SmallLimit + 4);
        using StreamSessionReactorHarness dialler = new(bindLoopback: false, maxMessageSize: LargeLimit, retentionBytes: LargeLimit + 4);
        PeerId peer = dialler.Reactor.Connect(accepter.Endpoint);

        // Act
        SendOutcome outcome = dialler.Reactor.Send(peer, new byte[500]);

        // Assert
        outcome.Should().Be(SendOutcome.NotEstablished);
    }
}
