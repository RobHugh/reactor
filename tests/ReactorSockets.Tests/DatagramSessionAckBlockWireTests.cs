using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #235: the ack-block proof inherited from #165. Both reliable Spaces' ack blocks
/// (<see cref="ReceiveWindowSpaces.GenerateAckBlocks"/>) are written unconditionally into a live-Session
/// datagram's header, so the header is a fixed 20 bytes whether or not this Session has received
/// anything to acknowledge, rather than a shape the reader has to negotiate. These tests read the wire
/// bytes a real Session actually sends — through <see cref="WireTap"/> and decoded independently by
/// <see cref="DatagramDecoder"/> (#228, #229) — rather than the counters or Events the rest of the ack
/// suite (<see cref="DatagramSessionAckBlockTests"/>) asserts through.
/// </summary>
public class DatagramSessionAckBlockWireTests
{
    private static readonly byte[] _diallerFirstPayload = [10, 20, 30];
    private static readonly byte[] _accepterPayload = [1, 2, 3, 4, 5];
    private static readonly byte[] _diallerSecondPayload = [99, 98, 97, 96];

    [Fact]
    public void Pump_DiallerSendsButAccepterNeverDoes_DiallersDatagramCarriesBothAckBlocksAsAFreshSpaceInA20ByteHeader()
    {
        // Arrange — an established pair where only the dialler ever calls Send. The accepter admits the
        // dialler's record but never Sends anything of its own (Pump only flushes a Peer with something
        // queued), so the dialler receives nothing back — no data record, not even a pure ack-carrying
        // datagram. "Received no data records" therefore holds for the dialler across the whole test:
        // the handshake is control traffic, not a data record, and nothing else ever arrives.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — the dialler Sends once and Pumps just far enough for its own Datagram to reach the tap;
        // the accepter is deliberately never given a chance to reply within this Act, so the datagram
        // under test was built while the dialler had received nothing.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, _diallerFirstPayload);
        dialler.Reactor.Pump();
        tap.Pump();

        (byte[] raw, DecodedDatagram datagram) = FindCapturedDatagramCarrying(tap, TapDirection.DiallerToAccepter, _diallerFirstPayload);

        // Assert — both ack blocks read as a fresh Space: a zero base with nothing above it acknowledged,
        // for both reliable Spaces, exactly what a window that has classified nothing reports
        // (ReceiveWindow.DescribeAsAckBlock over an untouched window).
        datagram.ReliableOrderedAck.Should().Be(new AckBlock(Base: 0, Bits: 0));
        datagram.ReliableUnorderedAck.Should().Be(new AckBlock(Base: 0, Bits: 0));

        // Assert — the header is exactly 20 bytes: this datagram carries exactly the one record the
        // dialler Sent (unfragmented record header 7 bytes, per DatagramDecoder's UnfragmentedRecordSize,
        // plus the payload), so the raw wire length is fully accounted for by that record on top of a
        // 20-byte header, with nothing left over for the ack blocks to have grown or shrunk the header
        // by — the same 20 bytes a Session that has received traffic stamps too (the second test).
        datagram.Records.Should().ContainSingle();
        const int recordHeaderSize = 7;
        raw.Length.Should().Be(20 + recordHeaderSize + _diallerFirstPayload.Length);
    }

    [Fact]
    public void Pump_AfterTheFarEndsRecordArrives_TheDiallersNextDatagramsAckBlocksReflectIt()
    {
        // Arrange — same starting point as the fresh-Space test: an established pair where the dialler
        // has Sent but the accepter has not yet replied, so the dialler's first outbound datagram still
        // carries a fresh ReliableOrdered block. This is confirmed by construction here (not re-asserted
        // in detail — that is the previous test's job) purely to anchor what "reflects them" is relative
        // to.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, _diallerFirstPayload);
        dialler.Reactor.Pump();
        tap.Pump();
        (byte[] firstRaw, DecodedDatagram firstDatagram) = FindCapturedDatagramCarrying(tap, TapDirection.DiallerToAccepter, _diallerFirstPayload);
        firstDatagram.ReliableOrderedAck.Should().Be(new AckBlock(Base: 0, Bits: 0));

        // Act — let the tapped datagram reach the accepter, which admits the dialler's record (session
        // established, so this is not a data record the dialler itself received — it is the accepter's
        // own receive). The accepter then Sends its own ReliableOrdered record back; once the dialler
        // has received and admitted that record, the dialler Sends again, producing a second datagram
        // whose ReliableOrdered block is generated fresh from the now-advanced window rather than
        // repeating the first datagram's block.
        accepter.Reactor.Pump();
        tap.Pump();

        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, _accepterPayload);
        List<byte[]> diallerPayloads = [];
        PumpRoundsThroughTapUntilDiallerReceives(dialler, tap, accepter, diallerPayloads, _accepterPayload);
        diallerPayloads.Should().ContainSingle().Which.Should().Equal(_accepterPayload);

        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, _diallerSecondPayload);
        dialler.Reactor.Pump();
        tap.Pump();

        (byte[] secondRaw, DecodedDatagram secondDatagram) = FindCapturedDatagramCarrying(tap, TapDirection.DiallerToAccepter, _diallerSecondPayload);

        // Assert — the far end's single ReliableOrdered record (sequence 0) admitted at the dialler
        // advances that Space's base from 0 to 1, with nothing above it yet acknowledged; the untouched
        // ReliableUnordered Space still reads as a fresh Space. This is what shows the blocks are
        // generated fresh on each Datagram rather than fixed once and repeated.
        secondDatagram.ReliableOrderedAck.Should().Be(new AckBlock(Base: 1, Bits: 0));
        secondDatagram.ReliableUnorderedAck.Should().Be(new AckBlock(Base: 0, Bits: 0));
        secondDatagram.ReliableOrderedAck.Should().NotBe(firstDatagram.ReliableOrderedAck);

        // Assert — the header is still exactly 20 bytes now that the dialler has received traffic, the
        // same as it was in the fresh-Space datagram above: each datagram carries exactly one record, so
        // its raw length is fully accounted for by that record's own size on top of a 20-byte header,
        // whatever the ack blocks inside that header now say.
        const int recordHeaderSize = 7;
        firstDatagram.Records.Should().ContainSingle();
        secondDatagram.Records.Should().ContainSingle();
        firstRaw.Length.Should().Be(20 + recordHeaderSize + _diallerFirstPayload.Length);
        secondRaw.Length.Should().Be(20 + recordHeaderSize + _diallerSecondPayload.Length);
    }

    /// <summary>
    /// Finds the one capture in <paramref name="direction"/> whose decoded records include
    /// <paramref name="payload"/>, returning both the raw captured bytes and the decoded value — the
    /// same payload-matching strategy <see cref="WireTapTests"/> uses to pick a Message datagram out of a
    /// tap's captures, since a control (handshake) datagram never decodes to a record carrying this
    /// payload.
    /// </summary>
    private static (byte[] Raw, DecodedDatagram Datagram) FindCapturedDatagramCarrying(WireTap tap, TapDirection direction, byte[] payload)
    {
        foreach (byte[] captured in tap.PayloadsInDirection(direction))
        {
            DatagramDecodeResult result = DatagramDecoder.TryDecode(captured);
            if (result is { IsSuccess: true, Datagram: DecodedDatagram datagram }
                && datagram.Records.Any(record => record.Payload.AsSpan().SequenceEqual(payload)))
            {
                return (captured, datagram);
            }
        }

        throw new InvalidOperationException($"No captured {direction} datagram decoded with a record carrying the expected payload.");
    }

    /// <summary>Pumps a dialler/tap/accepter round at a time until <paramref name="expectedPayload"/> is among the dialler's delivered Messages.</summary>
    private static void PumpRoundsThroughTapUntilDiallerReceives(
        DatagramSessionReactorHarness dialler,
        WireTap tap,
        DatagramSessionReactorHarness accepter,
        List<byte[]> diallerPayloads,
        byte[] expectedPayload)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (!diallerPayloads.Any(payload => payload.AsSpan().SequenceEqual(expectedPayload)))
        {
            dialler.Reactor.Pump();
            foreach (SessionEvent evt in dialler.Reactor.Drain())
            {
                if (evt.Kind == SessionEventKind.MessageReceived)
                {
                    diallerPayloads.Add(evt.Payload.ToArray());
                }
            }

            tap.Pump();

            accepter.Reactor.Pump();
            accepter.DrainToList();

            tap.Pump();

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The accepter's record did not reach the dialler within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }
}
