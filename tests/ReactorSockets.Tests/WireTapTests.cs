using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Proves <see cref="WireTap"/> does what #228 asks: two datagram Session Reactors complete a
/// handshake and exchange data through it, every datagram is captured byte-for-byte in arrival order
/// tagged with its direction, a capture outlives the Frame it was taken in, and neither side observes
/// any difference from a direct dialogue.
/// </summary>
public class WireTapTests
{
    [Fact]
    public void Pump_ThroughTap_DiallerAndAccepterCompleteAHandshakeAndExchangeData()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);

        // Act
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] fromDialler = [1, 2, 3];
        byte[] fromAccepter = [9, 8, 7, 6];
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, fromDialler);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, fromAccepter);

        List<byte[]> diallerPayloads = [];
        List<byte[]> accepterPayloads = [];
        PumpRoundsThroughTap(dialler, tap, accepter, rounds: 8, diallerPayloads, accepterPayloads);

        // Assert
        accepterPayloads.Should().ContainSingle().Which.Should().Equal(fromDialler);
        diallerPayloads.Should().ContainSingle().Which.Should().Equal(fromAccepter);
    }

    [Fact]
    public void Pump_ThroughTap_CapturesEveryDatagramByteForByteInArrivalOrderTaggedWithDirection()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        int capturedBeforeMessages = tap.Captures.Count;
        byte[] fromDialler = [11, 22, 33];
        byte[] fromAccepter = [44, 55];

        // Act — the accepter's reply cannot be captured until the dialler's Message has been relayed
        // and Pumped, so the two sends stay in this order rather than firing together.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, fromDialler);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, fromAccepter);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        List<TapCapture> afterHandshake = tap.Captures.Skip(capturedBeforeMessages).ToList();
        afterHandshake.Should().Contain(
            c => c.Direction == TapDirection.DiallerToAccepter && Contains(c.Payload, fromDialler),
            "the dialler's Message datagram must appear, tagged with the direction it travelled");
        afterHandshake.Should().Contain(
            c => c.Direction == TapDirection.AccepterToDialler && Contains(c.Payload, fromAccepter),
            "the accepter's reply datagram must appear, tagged with the direction it travelled");

        // The dialler-to-accepter capture carrying the Message must precede the accepter-to-dialler
        // capture carrying the reply, because the reply could only have been sent after the Message
        // was relayed — arrival order is preserved, not merely membership.
        int diallerCaptureIndex = afterHandshake.FindIndex(
            c => c.Direction == TapDirection.DiallerToAccepter && Contains(c.Payload, fromDialler));
        int accepterCaptureIndex = afterHandshake.FindIndex(
            c => c.Direction == TapDirection.AccepterToDialler && Contains(c.Payload, fromAccepter));
        diallerCaptureIndex.Should().BeLessThan(accepterCaptureIndex, "the tap preserves arrival order");
    }

    [Fact]
    public void Captures_ReadAfterSeveralLaterPumps_StillHoldsTheOriginalBytes()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] payload = [42, 42, 42];
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, payload);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] captured = tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .Single(p => p.AsSpan().IndexOf(payload) >= 0);
        byte[] expected = [.. captured];

        // Act — several more Pumps, each of which reuses the frame arena a RawEvent.Payload span would
        // still be aliasing (ADR 0008). If the tap kept such a span instead of a defensive copy, this
        // would corrupt the bytes read back below rather than leave them untouched.
        for (int i = 0; i < 5; i++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        }

        // Assert
        captured.Should().Equal(
            expected, "a capture is a defensive copy that outlives the Frame it was taken in, unaffected by later Pumps");
        Contains(captured, payload).Should().BeTrue("the original Message bytes are still present inside the captured datagram");
    }

    [Fact]
    public void Pump_ThroughTap_ProducesTheSameEventsAndCountersAsADirectDialogue()
    {
        // Arrange — a direct dialogue between two Reactors with no tap in between, as the baseline.
        using DatagramSessionReactorHarness directDialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness directAccepter = new();
        PeerId directDiallerPeer = directDialler.Reactor.Connect(directAccepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> directDiallerHandshake,
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> directAccepterHandshake) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(directDialler, directAccepter);
        PeerId directAccepterPeer = directAccepterHandshake.Single(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        byte[][] messages = [[1], [2, 2], [3, 3, 3]];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> directDiallerAll = [.. directDiallerHandshake];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> directAccepterAll = [.. directAccepterHandshake];
        List<byte[]> directAccepterPayloads = [];
        foreach (byte[] message in messages)
        {
            directDialler.Reactor.Send(directDiallerPeer, DeliveryMode.ReliableOrdered, message);
        }

        PumpDeadline directDeadline = PumpDeadline.Start();
        while (directAccepterPayloads.Count < messages.Length && !directDeadline.Expired)
        {
            directAccepter.Reactor.Pump();
            foreach (SessionEvent evt in directAccepter.Reactor.Drain())
            {
                directAccepterAll.Add((evt.Kind, evt.Peer, evt.Reason));
                if (evt.Kind == SessionEventKind.MessageReceived)
                {
                    directAccepterPayloads.Add(evt.Payload.ToArray());
                }
            }

            directDialler.Reactor.Pump();
            directDiallerAll.AddRange(directDialler.DrainToList());
            PumpDeadline.Yield();
        }

        // Arrange — the same dialogue, but relayed through a WireTap.
        using DatagramSessionReactorHarness tappedDialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness tappedAccepter = new();
        using WireTap tap = new(tappedAccepter.Endpoint, tappedDialler.TimeProvider);
        PeerId tappedDiallerPeer = tappedDialler.Reactor.Connect(tap.PublicEndPoint);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> tappedDiallerAll = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> tappedAccepterAll = [];
        PeerId tappedAccepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(tappedDialler, tap, tappedAccepter, tappedDiallerAll, tappedAccepterAll);

        List<byte[]> tappedAccepterPayloads = [];
        foreach (byte[] message in messages)
        {
            tappedDialler.Reactor.Send(tappedDiallerPeer, DeliveryMode.ReliableOrdered, message);
        }

        PumpRoundsThroughTap(
            tappedDialler,
            tap,
            tappedAccepter,
            rounds: 8,
            [],
            tappedAccepterPayloads,
            tappedDiallerAll,
            tappedAccepterAll);

        // Assert — the tap changes neither side's observable Events, delivered payloads or counters.
        tappedDiallerAll.Select(e => e.Kind).Should().Equal(
            directDiallerAll.Select(e => e.Kind), "the tap must be invisible to the dialler's Events");
        tappedAccepterAll.Select(e => e.Kind).Should().Equal(
            directAccepterAll.Select(e => e.Kind), "the tap must be invisible to the accepter's Events");
        tappedAccepterPayloads.Should().BeEquivalentTo(directAccepterPayloads, options => options.WithoutStrictOrdering());

        tappedAccepter.Reactor.CountersOf(tappedAccepterPeer).Should().Be(
            directAccepter.Reactor.CountersOf(directAccepterPeer),
            "relaying through the tap must not change what the accepter's counters record");
        tappedDialler.Reactor.CountersOf(tappedDiallerPeer).Should().Be(
            directDialler.Reactor.CountersOf(directDiallerPeer),
            "relaying through the tap must not change what the dialler's counters record");
    }

    /// <summary>Whether <paramref name="needle"/> occurs anywhere in <paramref name="haystack"/> — a plain method so a predicate calling it can be used as a FluentAssertions expression tree, which cannot itself contain a <see cref="Span{T}"/> operation.</summary>
    private static bool Contains(byte[] haystack, byte[] needle) => haystack.AsSpan().IndexOf(needle) >= 0;

    /// <summary>Pumps rounds through the tap, accumulating each side's drained Events and any delivered Message payloads, until <paramref name="rounds"/> is reached.</summary>
    private static void PumpRoundsThroughTap(
        DatagramSessionReactorHarness dialler,
        WireTap tap,
        DatagramSessionReactorHarness accepter,
        int rounds,
        List<byte[]> diallerPayloads,
        List<byte[]> accepterPayloads,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>? diallerEvents = null,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>? accepterEvents = null)
    {
        for (int round = 0; round < rounds; round++)
        {
            dialler.Reactor.Pump();
            foreach (SessionEvent evt in dialler.Reactor.Drain())
            {
                diallerEvents?.Add((evt.Kind, evt.Peer, evt.Reason));
                if (evt.Kind == SessionEventKind.MessageReceived)
                {
                    diallerPayloads.Add(evt.Payload.ToArray());
                }
            }

            tap.Pump();

            accepter.Reactor.Pump();
            foreach (SessionEvent evt in accepter.Reactor.Drain())
            {
                accepterEvents?.Add((evt.Kind, evt.Peer, evt.Reason));
                if (evt.Kind == SessionEventKind.MessageReceived)
                {
                    accepterPayloads.Add(evt.Payload.ToArray());
                }
            }

            tap.Pump();
        }
    }
}
