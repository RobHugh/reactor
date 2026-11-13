using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Goodbye, Closing, and the <see cref="DisconnectReason"/> roster. Goodbye is
/// the announcement that a Session is ending on purpose (ADR 0010); Closing is reached only when a
/// Goodbye arrives while something is still outstanding — either this end's own outbound retention
/// for that Peer still has bytes to deliver, or the consumer has not yet drained Messages this end
/// already reassembled from it. The counterparty in every test here is a hand-built raw TCP peer — mirrors
/// <see cref="StreamSessionMalformedFramingTests"/>'s <c>MalformedPeer</c> and
/// <see cref="StreamSessionRetentionFlushTests"/>'s <c>FlushPeer</c>, the house pattern for a hostile
/// or otherwise raw counterparty — since only <see cref="GoodbyePeer"/> can put the bare
/// <c>uint.MaxValue</c> sentinel on the wire without going through <see cref="StreamSessionReactor.Close"/>.
/// </summary>
public class StreamSessionGoodbyeTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;
    private const uint GoodbyeSentinel = uint.MaxValue;

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] BuildGoodbye() => StreamSessionTestRecords.BuildPrefix(GoodbyeSentinel);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/>
    /// event, returning the established Peer's id, and reads the accepter's own Hello record off
    /// <paramref name="peer"/>'s socket first, so later reads see only what the flush writes.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, GoodbyePeer peer, TimeSpan? budget = null, uint announcedMaxMessageSize = 4096)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, budget, announcedMaxMessageSize);
        peer.Receive(RecordSize);
        return id;
    }

    // =========================================================================================
    // Local Close: ordering, Send-after-Close, and the flush-then-close sequence
    // =========================================================================================

    [Fact]
    public void Close_ADialledPeerBeforeItsConnectResolves_ClosesWithLocalClose()
    {
        // Arrange — Connect hands back a PeerId before the TCP connect resolves, and Close is called on
        // it at once, before any Pump has advanced the dial.
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new();
        PeerId id = dialler.Reactor.Connect(accepter.Endpoint);

        // Act
        dialler.Reactor.Close(id);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntilDrained(
            dialler,
            accepter,
            (diallerSoFar, _) => diallerSoFar.Any(e => e.Kind == SessionEventKind.PeerClosed));

        // Assert
        diallerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Should().Be((SessionEventKind.PeerClosed, id, DisconnectReason.LocalClose));
    }

    [Fact]
    public void Send_AfterClose_ReturnsClosedOutcome()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        accepter.Reactor.Close(id);

        // Act — a Send after Close fails outright: nothing further is queued behind the Goodbye.
        SendOutcome afterClose = accepter.Reactor.Send(id, PayloadOf(16, 3));

        // Assert
        afterClose.Should().Be(SendOutcome.Closed);
    }

    [Fact]
    public void Close_WithMessagesQueuedBeforeIt_DeliversThemToTheWireAheadOfTheGoodbyeInOrder()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] first = PayloadOf(16, 1);
        byte[] second = PayloadOf(16, 2);

        // Act — two Messages queued, then Close, all before any Pump: everything queued ahead of the
        // Goodbye in this end's own retention must reach the wire first and whole.
        SendOutcome firstOutcome = accepter.Reactor.Send(id, first);
        SendOutcome secondOutcome = accepter.Reactor.Send(id, second);
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildGoodbye().Length);

        // Assert — both Sends were accepted, and both Messages arrived whole and in order, with the
        // Goodbye sentinel immediately behind them and nothing else.
        firstOutcome.Should().Be(SendOutcome.Sent);
        secondOutcome.Should().Be(SendOutcome.Sent);
        onWire.Should().Equal([.. BuildFrame(first), .. BuildFrame(second), .. BuildGoodbye()]);
    }

    [Fact]
    public void Close_WithMessagesQueuedBeforeIt_ClosesLocallyWithLocalCloseAndNoPeerClosing()
    {
        // Arrange — the queued Messages themselves are not this test's subject; only that Close was
        // preceded by outstanding retention.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        accepter.Reactor.Send(id, PayloadOf(16, 1));
        accepter.Reactor.Send(id, PayloadOf(16, 2));

        // Act — this end closes itself once its own retention (Messages plus Goodbye) has drained.
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosing,
            "nothing was outstanding for the far end to still be delivering — this is a local close, not a Goodbye received");
    }

    [Fact]
    public void Close_CalledTwice_IsIdempotentAndSendsExactlyOneGoodbye()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act
        accepter.Reactor.Close(id);
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildGoodbye().Length);

        // Assert — exactly one Goodbye's worth of bytes, not two.
        onWire.Should().Equal(BuildGoodbye());
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }

    // =========================================================================================
    // Goodbye received with nothing outstanding: closes directly (PeerGoodbye)
    // =========================================================================================

    [Fact]
    public void GoodbyeReceived_WithNothingOutstandingInThisEndsOwnRetention_ClosesDirectlyWithNoObservableClosingState()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — the peer says Goodbye with nothing ever queued for it to still be delivering.
        peer.Send(BuildGoodbye());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closed directly with PeerGoodbye, never a PeerClosing event first.
        drained.Should().ContainSingle(e => e.Peer == id)
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, id, DisconnectReason.PeerGoodbye));
    }

    // =========================================================================================
    // Goodbye received while this end's own retention still has bytes to deliver: Closing, then
    // Closed once that retention drains
    // =========================================================================================

    [Fact]
    public void GoodbyeReceived_WhileThisEndsOwnRetentionForThatPeerIsStillNonEmpty_ObservesClosingThenClosesWithPeerGoodbyeOnceDrained()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = PayloadOf(16, 7);

        // Act — the peer's Goodbye is on the wire, and this end's own retention for the peer holds a
        // Message not yet flushed, before the single Pump that reads the one and sees the other:
        // Reactor.Send commits straight into retention synchronously, and a Pump's dispatcher read
        // (which classifies the Goodbye) always runs before that same Pump's own flush step
        // (FlushPendingRetentionWrites), so this ordering is exactly what a Goodbye arriving with
        // outstanding retention looks like — not a race against how fast the bytes cross loopback.
        peer.Send(BuildGoodbye());
        SendOutcome sendOutcome = accepter.Reactor.Send(id, message);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(message).Length);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — both the Closing observation and the Closed event landed once the outstanding
        // Message had a chance to flush, in that order, within however many Frames it took.
        sendOutcome.Should().Be(SendOutcome.Sent);
        onWire.Should().Equal(BuildFrame(message));
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing && e.Peer == id);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosing))
            .Should().BeLessThan(drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosed)),
                "the Closing observation must precede the Closed event that follows it");
    }

    // =========================================================================================
    // Goodbye received with Messages still queued ahead of it for the consumer: Closing observes,
    // then closes once those Messages count as delivered (issue #23 review finding)
    // =========================================================================================

    [Fact]
    public void GoodbyeReceived_WithMessagesQueuedAheadOfItInTheSameRead_DeliversThemThenObservesClosingBeforeClosingOnALaterPump()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] first = PayloadOf(16, 1);
        byte[] second = PayloadOf(16, 2);

        // Act — two Messages and the peer's Goodbye all arrive in a single write, so one Pump's
        // DeliverCompleteMessages call reassembles all three in order: the Messages this end already
        // holds for the consumer must still count as outstanding even though each already reached
        // MessageReceived before the Goodbye immediately behind it was classified.
        peer.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildGoodbye()]);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosing); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — both Messages were delivered, whole and in order, before Closing was ever
        // observed — the Goodbye never jumps ahead of Messages that arrived before it in the same read.
        drained.Count(e => e.Kind == SessionEventKind.MessageReceived).Should().Be(2);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing && e.Peer == id);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "something this end already reassembled from the Peer was still outstanding to the consumer");
        drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosing))
            .Should().BeGreaterThan(drained.FindLastIndex(e => e.Kind == SessionEventKind.MessageReceived),
                "Closing must be observed only after every Message queued ahead of the Goodbye");

        // Act — a later Pump, once the Frame that delivered those Messages has passed, finally closes it.
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closes with PeerGoodbye, and Closing is never raised a second time for the same Goodbye.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing);
    }

    // =========================================================================================
    // Close silently dropping its own Goodbye when retention has no room for it (issue #23 review
    // finding): the append must retry until it commits, and this end must never close locally as if
    // it had announced when it had not
    // =========================================================================================

    [Fact]
    public void Close_WhenRetentionHasNoRoomForTheGoodbye_RetriesTheAppendOnALaterPumpAndAnnouncesBeforeClosing()
    {
        // Arrange — retention sized to hold exactly one maximum-size Message frame and nothing more
        // (the smallest legal pair under issue #16's cross-check, retention >= MaxMessageSize + 4):
        // a single 16-byte Send occupies the whole 20-byte ring, leaving the 4-byte Goodbye sentinel
        // Close tries to append no room at all. Retention is released only by a peer's delivery-counter
        // confirmation — handing bytes to the socket is not enough — so this test's peer must actually
        // send one back once it has read the Message, or the Goodbye append would stay Exhausted forever
        // rather than getting the retry-and-commit this test is about.
        const int maxMessageSize = 16;
        const int retentionBytes = maxMessageSize + 4; // 4-byte length prefix, matching BuildFrame.
        byte[] message = PayloadOf(maxMessageSize, 7);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — fill retention exactly, then Close: the Goodbye append finds RetentionOutcome.Exhausted.
        SendOutcome sendOutcome = accepter.Reactor.Send(id, message);
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(message).Length);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drainedAfterFirstPump = accepter.DrainToList();
        bool goodbyeArrivedEarly = peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200);

        // Assert — the Message already queued reaches the wire and this end is durably waiting on
        // confirmation before it can even attempt the Goodbye, but the Goodbye Close promised has not
        // been sent: the bug this closes would have this end close itself locally right here, having
        // never announced the ending, because retention happened to look drained on this exact Pump
        // (the retry that lands the Goodbye runs at the *start* of the next Pump, before that Pump's
        // own flush carries it to the wire).
        sendOutcome.Should().Be(SendOutcome.Sent);
        onWire.Should().Equal(BuildFrame(message));
        drainedAfterFirstPump.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        goodbyeArrivedEarly.Should().BeFalse(
            "the Goodbye must not reach the wire until its own retention append actually commits");

        // Act — the peer confirms the Message it just read, freeing the one Message's worth of room
        // the Goodbye needs; a later Pump then retries the deferred append and its own flush step
        // carries the now-committed Goodbye to the wire in that same Pump.
        peer.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(1));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        byte[] goodbyeOnWire = peer.Receive(BuildGoodbye().Length);

        // Assert — the Goodbye actually reached the wire, and this end closed itself only once it did:
        // FlushRetention hands the Goodbye's bytes to the socket before it ever calls
        // TryCloseAfterRetentionDrained, so a successful read here is a genuine wire observation, not
        // an artifact of the Peer having already closed.
        goodbyeOnWire.Should().Equal(BuildGoodbye());
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }

    // =========================================================================================
    // Send refusing a Peer already Closing from an inbound Goodbye, not just one this end told to
    // Close (issue #23 review finding — Closing means no further Sends worth making, either direction)
    // =========================================================================================

    [Fact]
    public void Send_ToAPeerStillClosingFromAnInboundGoodbye_ReturnsClosedAndPutsNothingOnTheWire()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = PayloadOf(16, 9);

        // Act — a Message queued ahead of the peer's own Goodbye in the same read keeps this end's
        // Peer in Closing for the rest of this Frame (HasUndeliveredInboundWork stays true), so the
        // Peer is still resolvable — but already Closing — when Send is called against it below.
        peer.Send([.. BuildFrame(message), .. BuildGoodbye()]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosing); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — the Peer reached Closing from the inbound Goodbye alone, without this end ever
        // calling Close.
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerClosing);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — this end never called Close; only the far end's Goodbye arrived. Pumping on past the
        // refusal is what makes the wire check mean something: a Send that had wrongly been accepted
        // would have queued bytes into retention, and it is the following Pump's flush that would
        // carry them out. Reading after the Peer has closed is safe — the close is orderly, so
        // anything written ahead of the FIN is still delivered (the deferred-Goodbye test above
        // reads its Goodbye off the wire exactly that way).
        SendOutcome outcome = accepter.Reactor.Send(id, PayloadOf(16, 3));
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        bool anythingFollowedTheGoodbye = peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200);

        // Assert — refused all the same: a Goodbye has already passed for this Peer, so nothing
        // further belongs on the wire behind it.
        outcome.Should().Be(SendOutcome.Closed);
        anythingFollowedTheGoodbye.Should().BeFalse(
            "no Message may follow a Goodbye onto the wire, whichever end sent it");
    }

    // =========================================================================================
    // Data after Goodbye: a Peer that announces it is leaving and keeps sending is a protocol
    // violation, not an infinite grant of ordinary delivery (issue #93)
    // =========================================================================================

    [Fact]
    public void DataAfterGoodbye_MessageArrivesBehindGoodbyeInTheSameWrite_ClosesWithProtocolViolationAndNeverDeliversIt()
    {
        // Arrange — a preceding legitimate Message keeps HasUndeliveredInboundWork true for the Frame
        // the Goodbye is classified in, so GoodbyeReceived defers the close instead of taking it
        // immediately, and the parse loop keeps going to the bytes right behind the Goodbye — exactly
        // the state the bug this closes left unguarded.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] precedingMessage = PayloadOf(16, 1);
        byte[] trailingMessage = PayloadOf(16, 2);

        // Act — one write: a Message, the Goodbye, then another Message immediately behind it.
        peer.Send([.. BuildFrame(precedingMessage), .. BuildGoodbye(), .. BuildFrame(trailingMessage)]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closed with ProtocolViolation, and only the Message ahead of the Goodbye was ever
        // delivered — the one behind it never reaches MessageReceived.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
        drained.Count(e => e.Kind == SessionEventKind.MessageReceived).Should().Be(1);
    }

    [Fact]
    public void DataAfterGoodbye_ContinuousStreamOfMessagesBehindGoodbye_ClosesWithProtocolViolationWithinBoundedPumps()
    {
        // Arrange — same deferral as the single-Message case above, but fifty Messages queued behind
        // the Goodbye in one write: the regression case for the hole this closes, where every one of
        // them would otherwise have been delivered as an ordinary MessageReceived event, forever,
        // rather than the Peer ever reaching PeerClosed.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] precedingMessage = PayloadOf(16, 1);

        List<byte> burst = [.. BuildFrame(precedingMessage), .. BuildGoodbye()];
        for (int i = 0; i < 50; i++)
        {
            burst.AddRange(BuildFrame(PayloadOf(16, (byte)i)));
        }

        // Act
        peer.Send([.. burst]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closed with ProtocolViolation within the Pump budget, and only the one legitimate
        // Message ahead of the Goodbye was ever delivered — none of the fifty behind it.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
        drained.Count(e => e.Kind == SessionEventKind.MessageReceived).Should().Be(1);
    }

    [Fact]
    public void DataAfterGoodbye_MessageArrivesBehindGoodbyeInTheSameWrite_LogsDataAfterGoodbyeAtWarnWithThePeerSubject()
    {
        // Arrange — issue #106: DataAfterGoodbye must log its own diagnostic shape rather than
        // reusing ForMalformedFraming's claimed-length shape, since Classify returns before any
        // prefix is read and there is no claimed length to report.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] precedingMessage = PayloadOf(16, 1);
        byte[] trailingMessage = PayloadOf(16, 2);
        accepter.Sink.Received.Clear();

        // Act
        peer.Send([.. BuildFrame(precedingMessage), .. BuildGoodbye(), .. BuildFrame(trailingMessage)]);
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.DataAfterGoodbye));

        // Assert
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DataAfterGoodbye).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Peer.Should().Be(id);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.MalformedFraming,
            "DataAfterGoodbye is a distinct diagnostic shape, not a reuse of MalformedFraming's claimed-length one");
    }

    [Fact]
    public void DataAfterGoodbye_MessageArrivesInASeparateLaterWrite_ClosesWithProtocolViolation()
    {
        // Arrange — this end's own outbound retention is driven to genuine, durable backpressure
        // against a counterparty whose receive window is shrunk and that never reads (mirrors
        // StreamSessionRetentionFlushTests' Send_AgainstACounterpartyThatNeverReadsAfterTheHandshake_
        // EventuallyReturnsRetentionExhausted). Retention is now released only by a peer's
        // delivery-counter confirmation (issue #113), which this GoodbyePeer never sends, so a bound
        // sized for only one Message would reach SendOutcome.RetentionExhausted immediately on the
        // second Send — before the kernel's own send buffer has had any chance to back up, which is
        // what this test actually needs: RetentionIsDrained (now "has everything been handed to the
        // transport", not "has the peer confirmed it") must stay false across Pump boundaries so the
        // peer's Goodbye alone cannot close this end directly and the Peer sits in Closing for more
        // than one Pump — the only way a genuinely later, separate write can still reach
        // DeliverCompleteMessages after the Goodbye rather than arriving at an already-closed socket.
        // A bound wide enough for about a hundred Messages gives real TCP flow control room to stall a
        // write against the peer's shrunk, never-drained receive window long before this end's own
        // confirmed-retention exhausts, so both conditions — RetentionExhausted and genuinely unsent
        // bytes — are true together, as they need to be here.
        //
        // Shrinking the peer's receive window is not on its own enough to produce that stall, which is
        // what issue #135 found: it bounds what the peer will accept, not what this end's own kernel
        // will absorb on its behalf, and Linux's send autotuning happily swallowed all ~820KB into the
        // accepter's send queue, advanced MarkSent for every byte of it, and let the Goodbye close this
        // end outright with PeerGoodbye rather than ever reaching Closing. Windows' smaller default send
        // buffer hid that. Capping SendBufferSize here bounds the absorbing side too, so the stall this
        // test is built on is a property of the configuration rather than of the platform's own defaults.
        const int maxMessageSize = 8192;
        const int retentionBytes = 100 * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes, sendBufferSize: 1024);
        using GoodbyePeer peer = new(accepter.Endpoint, receiveBufferSize: 1024);
        PeerId id = EstablishSession(accepter, peer, announcedMaxMessageSize: maxMessageSize);
        byte[] stuckMessage = PayloadOf(maxMessageSize, 1);
        byte[] trailingMessage = PayloadOf(16, 9);

        // Act — queue and flush against the unread socket until retention is genuinely, durably full.
        SendOutcome outcome = SendOutcome.Sent;
        for (int attempt = 0; attempt < 500 && outcome == SendOutcome.Sent; attempt++)
        {
            outcome = accepter.Reactor.Send(id, stuckMessage);
            accepter.Reactor.Pump();
        }

        outcome.Should().Be(SendOutcome.RetentionExhausted,
            "the test's own precondition — retention must be durably non-empty across Pumps");

        // Act — the peer's Goodbye, on its own Pump: Closing is observed, not a close, since this
        // end's own retention still has bytes it cannot get onto the wire.
        peer.Send(BuildGoodbye());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        accepter.Reactor.Pump();
        drained.AddRange(accepter.DrainToList());

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — a genuinely separate write, on a later Pump, behind the already-classified Goodbye. A
        // sleep-on-no-progress retry, not a tight sleepless Pump loop: this end's own outbound side is
        // already saturated, so the OS needs real wall-clock time to schedule and deliver the peer's
        // small trailing write on top of that congestion — the same idiom StreamSessionDeliveryCounterTests'
        // CountersOf_AfterThreeDeliveredMessages_MessagesReceivedReadsThree uses for Linux's longer
        // ack latency, itself mirroring RawReactorSendTests' sleep-on-no-progress idiom.
        peer.Send(BuildFrame(trailingMessage));
        for (int attempt = 0; attempt < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); attempt++)
        {
            if (attempt > 0)
            {
                Thread.Sleep(20);
            }

            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.MessageReceived);
    }

    [Fact]
    public void InboundMessage_AfterThisEndCallsClose_IsStillDeliveredAndIsNotAViolation()
    {
        // Arrange — this end announces its own departure via Close, but the far end has not sent (and
        // in this test never sends) a Goodbye of its own. RemoteGoodbyeReceived must stay false from a
        // purely local Close — a Message the far end sent before it had any way to know this end was
        // leaving is legitimate, not a violation. This is the distinction issue #93 requires between
        // an inbound Goodbye and this end's own PendingCloseReasonAfterRetentionDrains/LocalGoodbyeSent,
        // which a local Close sets without any inbound Goodbye ever having arrived.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = PayloadOf(16, 4);

        // Act — this end closes locally, then the peer's own Message (sent before it could possibly
        // have seen this end's Goodbye) arrives.
        accepter.Reactor.Close(id);
        peer.Send(BuildFrame(message));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.MessageReceived); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToListWithPayload());
        }

        // Assert — delivered normally, not a violation.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.MessageReceived)
            .Which.Payload.Should().Equal(message);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.ProtocolViolation);
    }

    [Fact]
    public void FinAfterGoodbye_WithNothingElseBuffered_ClosesWithPeerGoodbyeNotProtocolViolation()
    {
        // Arrange — a preceding legitimate Message keeps this end's Peer in Closing rather than closing
        // immediately when the Goodbye is classified, so the connection is still open when the peer's
        // clean FIN follows with nothing else on the wire — exactly the shape ClassifyEndOfStream must
        // still treat as an ordinary, unreported end of stream (issue #23), not a violation, now that
        // DataAfterGoodbye exists (issue #93): ClassifyEndOfStream is untouched by that change and
        // still keys only off whether anything is left buffered, never off RemoteGoodbyeReceived.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] precedingMessage = PayloadOf(16, 1);

        // Act
        peer.Send([.. BuildFrame(precedingMessage), .. BuildGoodbye()]);
        peer.CloseGracefully();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that can put a raw Goodbye sentinel on the wire directly — something no consumer of the public
    /// <see cref="StreamSessionReactor"/> surface can do, since <see cref="StreamSessionReactor.Close"/>
    /// is the only way this library ever writes one. Mirrors <see cref="StreamSessionRetentionFlushTests"/>'s
    /// <c>FlushPeer</c>, minus the shrunk receive buffer that file's backpressure tests need and this
    /// one does not.
    /// </summary>
    private sealed class GoodbyePeer : IDisposable
    {
        private readonly Socket _socket;

        /// <param name="destination">The Reactor's bound endpoint to connect to.</param>
        /// <param name="receiveBufferSize">
        /// Applied before <c>Connect</c>, so the negotiated receive window is small from the handshake
        /// onward — never applied after connecting (mirrors <c>StreamSessionRetentionFlushTests</c>'s
        /// <c>FlushPeer</c>, the house pattern for shrinking a counterparty's window so this end's own
        /// outbound retention cannot fully flush in one write). Omit for the OS default.
        /// </param>
        public GoodbyePeer(IPEndPoint destination, int? receiveBufferSize = null)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
            };
            if (receiveBufferSize is int size)
            {
                _socket.ReceiveBufferSize = size;
            }

            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>
        /// Ends the connection gracefully — a bare FIN with nothing further on the wire — mirrors
        /// <see cref="StreamSessionMalformedFramingTests"/>'s <c>MalformedPeer.CloseGracefully</c>.
        /// </summary>
        public void CloseGracefully() => _socket.Shutdown(SocketShutdown.Send);

        /// <summary>Blocks until exactly <paramref name="count"/> bytes have been read off the socket.</summary>
        public byte[] Receive(int count)
        {
            byte[] buffer = new byte[count];
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = _socket.Receive(buffer.AsSpan(totalRead));
                if (read == 0)
                {
                    throw new InvalidOperationException("The Reactor closed the connection before sending everything expected.");
                }

                totalRead += read;
            }

            return buffer;
        }

        /// <summary>
        /// Reads <paramref name="count"/> bytes within a short timeout, answering whether it succeeded
        /// rather than throwing or blocking indefinitely — used to assert nothing has arrived yet.
        /// Mirrors <c>HandshakePeer.TryReceiveWithinTimeout</c> in
        /// <see cref="StreamSessionHandshakeGuardTests"/>, the house pattern for this assertion.
        /// </summary>
        public bool TryReceiveWithinTimeout(int count, out byte[] record, int timeoutMs = 300)
        {
            _socket.ReceiveTimeout = timeoutMs;
            byte[] buffer = new byte[count];
            int totalRead = 0;
            try
            {
                while (totalRead < count)
                {
                    int read = _socket.Receive(buffer.AsSpan(totalRead));
                    if (read == 0)
                    {
                        record = [];
                        return false;
                    }

                    totalRead += read;
                }
            }
            catch (SocketException)
            {
                record = [];
                return false;
            }
            finally
            {
                _socket.ReceiveTimeout = 5000;
            }

            record = buffer;
            return true;
        }

        public void Dispose() => _socket.Dispose();
    }
}
