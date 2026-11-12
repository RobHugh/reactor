using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Stream reads: Segments, the read share, and <c>EndOfStream</c> — sub-issue 2.3. Every assertion is
/// against the drained <see cref="RawEvent"/>s, a <see cref="Result"/> or a <see cref="DiagnosticRecord"/>;
/// nothing here reaches into how or when a read was detected.
/// </summary>
public class RawReactorStreamReadTests
{
    [Fact]
    public void Drain_CounterpartyWritesOnce_SegmentsConcatenateToTheWrite()
    {
        // Arrange — a counterparty writing N bytes in one call may be drained as one or several
        // SegmentReceived events; this asserts the concatenation, never a per-event size.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();

        byte[] payload = new byte[5000];
        Random.Shared.NextBytes(payload);
        counterparty.Send(payload);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained =
            harness.PumpUntilDrained(e => e.Where(x => x.Kind == RawEventKind.SegmentReceived && x.Link == acceptedLink).Sum(x => x.Payload.Length) >= payload.Length);

        // Assert
        byte[] received = drained
            .Where(evt => evt.Kind == RawEventKind.SegmentReceived && evt.Link == acceptedLink)
            .SelectMany(evt => evt.Payload)
            .ToArray();
        received.Should().Equal(payload);
    }

    [Fact]
    public void Drain_CounterpartyWritesMoreThanTheScratchBufferSize_NoSegmentExceedsTheScratchBufferSize()
    {
        // Arrange — the scratch buffer is one byte longer than scratchBufferSize so a UDP read can
        // detect truncation. A stream read must still be given only scratchBufferSize bytes.
        const int scratchSize = 64;
        using RawReactorHarness harness = new(capacity: 4, scratchBufferSize: scratchSize);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();

        byte[] payload = new byte[scratchSize * 20];
        Random.Shared.NextBytes(payload);
        counterparty.Send(payload);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained =
            harness.PumpUntilDrained(e => e.Where(x => x.Kind == RawEventKind.SegmentReceived && x.Link == acceptedLink).Sum(x => x.Payload.Length) >= payload.Length);

        // Assert
        List<byte[]> segments = drained
            .Where(evt => evt.Kind == RawEventKind.SegmentReceived && evt.Link == acceptedLink)
            .Select(evt => evt.Payload)
            .ToList();
        segments.SelectMany(segment => segment).Should().Equal(payload);
        segments.Should().OnlyContain(segment => segment.Length <= scratchSize);
    }

    [Fact]
    public void Drain_OneLinkDeliveringALargeRun_IsServedAcrossSeveralPumpsWithoutStarvingAnother()
    {
        // Arrange — a small scratch buffer so a run much larger than one read needs several Pumps, and
        // an arena with headroom well beyond one read (unlike the arena's *own* bound, exercised
        // separately below: that one is about a single read not fitting at all, this one is about the
        // dispatcher giving every ready Link its own turn rather than looping one Link to exhaustion
        // first) so a second, quiet Link is never made to wait for the loud one to finish.
        using RawReactorHarness harness = new(capacity: 6, scratchBufferSize: 4096, arenaSize: 32_768);
        (LinkId loudListener, IPEndPoint loudEndPoint) = harness.ListenOnLoopback();
        (LinkId quietListener, IPEndPoint quietEndPoint) = harness.ListenOnLoopback();
        using Socket loudCounterparty = harness.ConnectCounterpartyTo(loudEndPoint);
        using Socket quietCounterparty = harness.ConnectCounterpartyTo(quietEndPoint);

        harness.Reactor.Pump();
        LinkId loudLink = default;
        LinkId quietLink = default;
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            if (evt.Kind != RawEventKind.Accepted)
            {
                continue;
            }

            if (evt.ListeningLink == loudListener)
            {
                loudLink = evt.Link;
            }
            else if (evt.ListeningLink == quietListener)
            {
                quietLink = evt.Link;
            }
        }

        byte[] loudPayload = new byte[20_000]; // several times the 4 096-byte scratch buffer/arena
        Random.Shared.NextBytes(loudPayload);
        byte[] quietPayload = [1, 2, 3];
        loudCounterparty.Send(loudPayload);
        quietCounterparty.Send(quietPayload);

        // Act — Pump until the loud Link's entire run has arrived, recording which Pump (1-based)
        // delivered the quiet Link's Segment along the way. Loopback delivery of a 20 000-byte write is
        // not guaranteed to have reached the kernel's receive queue by the very next statement — unlike
        // every other outcome in this file, this one genuinely depends on real OS scheduling, not just
        // Pump count, so this polls with a bound rather than assuming the first Pump sees everything.
        List<byte> loudReceived = [];
        int quietDeliveredOnPump = -1;
        int pump = 0;
        for (; pump < 30 && loudReceived.Count < loudPayload.Length; pump++)
        {
            harness.Reactor.Pump();
            foreach ((RawEventKind Kind, LinkId Link, byte[] Payload) evt in harness.DrainToList())
            {
                if (evt.Kind == RawEventKind.SegmentReceived && evt.Link == loudLink)
                {
                    evt.Payload.Length.Should().BeLessThan(loudPayload.Length,
                        "one Link must not consume its whole run in a single read");
                    loudReceived.AddRange(evt.Payload);
                }
                else if (evt.Kind == RawEventKind.SegmentReceived && evt.Link == quietLink)
                {
                    quietDeliveredOnPump.Should().Be(-1, "the quiet Link's whole write fits in one read");
                    evt.Payload.Should().Equal(quietPayload);
                    quietDeliveredOnPump = pump;
                }
            }
        }

        // Assert
        loudReceived.Should().Equal(loudPayload);
        pump.Should().BeGreaterThan(1, "a run this large cannot fit in a single Pump's read");
        quietDeliveredOnPump.Should().BeGreaterThanOrEqualTo(0, "the quiet Link must be served at all");
        quietDeliveredOnPump.Should().BeLessThan(pump - 1,
            "the quiet Link must not be made to wait for the loud Link's multi-Pump run to finish");
    }

    [Fact]
    public void Drain_CounterpartyShutsDownSendHalf_DrainsEndOfStreamNotClosed()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();

        // Act
        counterparty.Shutdown(SocketShutdown.Send);
        harness.Reactor.Pump();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.EndOfStream && evt.Link == acceptedLink);
        drained.Should().NotContain(evt => evt.Kind == RawEventKind.Closed,
            "EndOfStream is a statement about the read direction only, never Closed");
    }

    [Fact]
    public void Drain_CounterpartyShutsDownSendHalf_LinkStillAcceptsASendCallAfterwards()
    {
        // Arrange — the half-open state is real: EndOfStream is a statement about the read direction
        // only, so this Link's own send half must be entirely unaffected by the counterparty shutting
        // down theirs.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        counterparty.Shutdown(SocketShutdown.Send);
        harness.Reactor.Pump();
        harness.DrainToList();

        // Act
        SendResult result = harness.Reactor.Send(acceptedLink, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeTrue("a half-open Link's own send half is unaffected by the peer's half-close");
        result.BytesSent.Should().Be(3);
    }

    [Fact]
    public void Drain_NothingFurtherAfterEndOfStream_ProducesNoRepeatEvent()
    {
        // Arrange — the FIN that produces EndOfStream leaves the socket permanently readable, so this
        // proves the Reactor does not re-queue the same event every later Pump.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        harness.DrainToList();
        counterparty.Shutdown(SocketShutdown.Send);
        harness.Reactor.Pump();
        harness.DrainToList();

        // Act
        harness.Reactor.Pump();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Should().BeEmpty();
    }

    [Fact]
    public void Drain_CounterpartyShutsDownThenClosesGracefully_LeavesTheLinkOpenUntilWeCloseIt()
    {
        // Arrange — driven from the counterparty exactly as the parent spec asks: shut down its write
        // half first (EndOfStream, not Closed). Once the write half is shut, a peer's own FIN was
        // already sent, so a subsequent graceful close puts nothing further on the wire — there is
        // nothing left for us to detect, which is precisely the leak the parent spec documents rather
        // than defends against: no Linger in Raw means this Link and its slot stay ours to close.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        counterparty.Shutdown(SocketShutdown.Send);
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle(evt => evt.Kind == RawEventKind.EndOfStream);

        // Act — the counterparty goes away gracefully; nothing further arrives for several Pumps.
        counterparty.Dispose();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> afterCounterpartyGone = harness.PumpAndDrainAll(pumps: 3);

        // Assert — the half-open Link is still ours: no Closed arrived on its own, so we close it.
        afterCounterpartyGone.Should().NotContain(evt => evt.Kind == RawEventKind.Closed);
        harness.Reactor.Close(acceptedLink);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained = harness.DrainToReasonList();
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == acceptedLink)
            .Which.Reason.Should().Be(CloseReason.LocalClose);
    }

    [Fact]
    public void Drain_CounterpartyClosesAbruptlyWithoutPriorShutdown_DrainsClosedWithResetByRemoteAndNoEndOfStream()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();

        // Act
        RawReactorHarness.CloseCounterpartyAbruptly(counterparty);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained = harness.PumpAndDrainReasons(pumps: 1);

        // Assert
        drained.Should().NotContain(evt => evt.Kind == RawEventKind.EndOfStream);
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == acceptedLink)
            .Which.Reason.Should().Be(CloseReason.ResetByRemote);
    }

    [Fact]
    public void Close_AcceptedStreamLink_DrainsClosedWithLocalClose()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        harness.DrainToList();

        // Act
        harness.Reactor.Close(acceptedLink);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained = harness.DrainToReasonList();

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == acceptedLink)
            .Which.Reason.Should().Be(CloseReason.LocalClose);
    }

    [Fact]
    public void Drain_DialledLinkCounterpartySendsBytes_DrainsSegmentReceived()
    {
        // Arrange — the dial side of a stream Link's reads, distinct from the accept side every other
        // test in this file exercises: reads only begin once PollConnect resolves the handshake.
        using RawReactorHarness harness = new(capacity: 4);
        (Socket counterpartyListener, IPEndPoint serverEndPoint) = harness.CreateCounterpartyListener();
        LinkId dialLink = harness.Reactor.Connect(serverEndPoint);
        harness.PumpAndDrainAll(pumps: 3);
        using Socket counterpartyAccepted = counterpartyListener.Accept();

        byte[] payload = [10, 20, 30, 40, 50];
        counterpartyAccepted.Send(payload);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained =
            harness.PumpUntilDrained(e => e.Where(x => x.Kind == RawEventKind.SegmentReceived && x.Link == dialLink).Sum(x => x.Payload.Length) >= payload.Length);

        // Assert
        byte[] received = drained
            .Where(evt => evt.Kind == RawEventKind.SegmentReceived && evt.Link == dialLink)
            .SelectMany(evt => evt.Payload)
            .ToArray();
        received.Should().Equal(payload);
    }

    [Fact]
    public void Pump_ArenaWithNoRoomForASecondStreamLinksSegment_ReportsArenaExhausted()
    {
        // Arrange — the stream counterpart of RawReactorFrameTests' datagram arena-exhaustion case: two
        // accepted Links ready in one Frame, sharing an arena that cannot hold both Segments.
        using RawReactorHarness harness = new(capacity: 3, scratchBufferSize: 1200, arenaSize: 1200);
        (_, IPEndPoint endPoint) = harness.ListenOnLoopback();
        using Socket counterpartyA = harness.ConnectCounterpartyTo(endPoint);
        using Socket counterpartyB = harness.ConnectCounterpartyTo(endPoint);
        harness.Reactor.Pump();
        harness.DrainToList().Count(evt => evt.Kind == RawEventKind.Accepted).Should().Be(2);

        byte[] payload = new byte[1000];
        counterpartyA.Send(payload);
        counterpartyB.Send(payload);

        // Act
        harness.Reactor.Pump();

        // Assert
        DiagnosticRecord record = harness.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ArenaExhausted).Subject;
        record.Value0.Should().Be(1000, "the refused Segment's size is reported");
        record.Value1.Should().Be(200, "so is the arena's free space at the moment it was refused");
    }
}
