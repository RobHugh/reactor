using System.Security.Cryptography;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #481: a Message sent to an Interrupted stream Peer reaches the far end once the Peer
/// resumes, on both the dialling and the listening side, without the consumer sending anything else.
/// </summary>
/// <remarks>
/// Drives the public L2 surface only (tests/CLAUDE.md, ADR 0015). Every Pump is followed by a drain,
/// because a Pump clears Events nobody drained.
/// </remarks>
public class StreamSessionInterruptedSendFlushTests
{
    [Fact]
    public void Pump_DiallerResumesAfterASendWhileInterrupted_WritesThatMessageToTheNewConnection()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(retentionBytes: 4100);
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 44);
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        RawStreamListener.Accepted far = listener.Accept();
        dialer.Reactor.Pump();
        far.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        far.Send(StreamSessionTestRecords.BuildHello(resumeSecret: resumeSecret));

        PeerId? connected = null;
        for (int i = 0; i < 20 && connected is null; i++)
        {
            dialer.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialer.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    connected = evt.Peer;
                }
            }
        }

        connected.Should().Be(peer);

        far.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer); i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);

        dialer.Reactor.Send(peer, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Pump();
        dialer.DrainToList();

        // Act
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
        dialer.Reactor.Pump();
        dialer.DrainToList();
        listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue();
        dialer.Reactor.Pump();
        dialer.DrainToList();
        redialed.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        redialed.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(0));

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> resumeDrain = [];
        for (int i = 0; i < 20; i++)
        {
            dialer.Reactor.Pump();
            resumeDrain.AddRange(dialer.DrainToList());
        }

        // Assert
        byte[] frame = StreamSessionTestRecords.BuildFrame([1, 2, 3]);
        resumeDrain.Should().Contain(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == peer);
        redialed.TryReceiveWithinTimeout(frame.Length, out byte[] got, timeoutMs: 500).Should().BeTrue(
            "the Message sent while Interrupted is flushed once the resume completes");
        got.Should().Equal(frame);

        redialed.Dispose();
        far.Dispose();
    }

    [Fact]
    public void Pump_ListenerResumesAfterASendWhileInterrupted_WritesThatMessageAfterItsDeliveryCounter()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);
        accepter.Reactor.Send(held.Peer, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        // Act
        using StreamSessionReactorHarness.RawStreamPeer back = new(accepter.Endpoint);
        back.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret, 0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert
        byte[] frame = StreamSessionTestRecords.BuildFrame([1, 2, 3]);
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == held.Peer);
        back.TryReceiveWithinTimeout(StreamSessionTestRecords.BuildDeliveryCounterRecord(0).Length, out _, 500)
            .Should().BeTrue("the accepter's delivery counter record comes first");
        back.TryReceiveWithinTimeout(frame.Length, out byte[] got, 500).Should().BeTrue(
            "the Message sent while Interrupted is flushed once the resume completes");
        got.Should().Equal(frame);
    }

    [Fact]
    public void Pump_TwoReactorsResumeAfterSendsWhileInterrupted_DeliverEachMessageOnceInOrder()
    {
        // Arrange
        FakeTimeProvider clock = new();
        using StreamSessionReactorHarness listener = new(
            timeProvider: clock, sendIdleInterval: TimeSpan.FromMilliseconds(100), linger: TimeSpan.FromSeconds(2));
        using StreamSessionReactorHarness dialer = new(
            timeProvider: clock,
            sendIdleInterval: TimeSpan.FromMilliseconds(100),
            linger: TimeSpan.FromSeconds(2),
            entropy: new ConstantByteJitter(0xFF));
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);

        bool dialerConnected = false;
        bool listenerConnected = false;
        for (int i = 0; i < 50 && !(dialerConnected && listenerConnected); i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(10));
            dialer.Reactor.Pump();
            dialerConnected |= dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);
            listener.Reactor.Pump();
            listenerConnected |= listener.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);
        }

        dialerConnected.Should().BeTrue();
        listenerConnected.Should().BeTrue();

        bool interrupted = false;
        for (int i = 0; i < 40 && !interrupted; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(50));
            dialer.Reactor.Pump();
            interrupted = dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerInterrupted);
        }

        interrupted.Should().BeTrue();

        // Act
        dialer.Reactor.Send(peer, [1]).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Send(peer, [2]).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Send(peer, [3]).Should().Be(SendOutcome.Sent);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> dialerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> listenerEvents = [];
        for (int i = 0; i < 300; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(10));
            dialer.Reactor.Pump();
            dialerEvents.AddRange(dialer.DrainToListWithPayload());
            listener.Reactor.Pump();
            listenerEvents.AddRange(listener.DrainToListWithPayload());
            if (listenerEvents.Count(e => e.Kind == SessionEventKind.MessageReceived) >= 3
                && dialerEvents.Any(e => e.Kind == SessionEventKind.PeerResumed))
            {
                break;
            }
        }

        // Assert
        listenerEvents.Where(e => e.Kind == SessionEventKind.MessageReceived).Select(e => e.Payload)
            .Should().SatisfyRespectively(
                p => p.Should().Equal([1]),
                p => p.Should().Equal([2]),
                p => p.Should().Equal([3]));
        dialerEvents.Count(e => e.Kind == SessionEventKind.PeerResumed).Should().Be(1);
        listenerEvents.Count(e => e.Kind == SessionEventKind.PeerResumed).Should().Be(1);
        dialerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        listenerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    /// <summary>A jitter source that returns the same byte for every draw, so re-dial delays are deterministic.</summary>
    private sealed class ConstantByteJitter(byte value) : RandomNumberGenerator
    {
        public override void GetBytes(byte[] data) => GetBytes(data.AsSpan());

        public override void GetBytes(Span<byte> data) => data.Fill(value);
    }
}
