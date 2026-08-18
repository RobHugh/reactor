using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Partial sends, the <c>Writable</c> arming rule, and <c>Shutdown</c> — sub-issue 2.4. Every
/// assertion is against a drained <see cref="RawEvent"/>, a <see cref="SendResult"/>/<see cref="Result"/>
/// or a counterparty BCL socket; nothing here reaches into how or when a partial send was detected or
/// when <c>Select</c> reported writability (tests/CLAUDE.md's testing decisions).
/// </summary>
public class RawReactorSendTests
{
    [Fact]
    public void Send_SmallerThanTheSocketsAvailableBuffer_TakesEverythingAndArmsNothing()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        byte[] payload = [1, 2, 3, 4, 5];

        // Act
        SendResult result = harness.Reactor.Send(acceptedLink, payload);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.BytesSent.Should().Be(payload.Length);

        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.PumpAndDrainAll(pumps: 20);
        drained.Should().NotContain(evt => evt.Kind == RawEventKind.Writable,
            "a Link never over-sent on must produce no Writable at all");
    }

    [Fact]
    public void Send_AgainstACounterpartyThatNeverReads_EventuallyReturnsFewerBytesThanOffered()
    {
        // Arrange — provoking a short send needs a payload sized against the actual socket buffers,
        // not a magic constant: this keeps sending fixed-size chunks to a counterparty that never
        // reads, with a shrunk receive buffer to make the buffers fill sooner, and stops the moment a
        // call reports fewer bytes taken than offered. No test may pin how many chunks that takes.
        using RawReactorHarness harness = new(capacity: 4);
        (Socket counterpartyListener, IPEndPoint serverEndPoint) = harness.CreateCounterpartyListener(receiveBufferSize: 1024);
        LinkId link = harness.Reactor.Connect(serverEndPoint);
        harness.PumpAndDrainAll(pumps: 3);
        using Socket counterpartyAccepted = counterpartyListener.Accept();

        byte[] chunk = new byte[65_536];
        Random.Shared.NextBytes(chunk);

        // Act
        SendResult? shortSend = null;
        for (int attempt = 0; attempt < 200 && shortSend is null; attempt++)
        {
            SendResult result = harness.Reactor.Send(link, chunk);
            result.IsSuccess.Should().BeTrue("nothing about this Link has failed — the counterparty simply is not reading");
            if (result.BytesSent < chunk.Length)
            {
                shortSend = result;
            }
        }

        // Assert
        shortSend.Should().NotBeNull("a counterparty that never reads must eventually force a short send");

        // `!` works around a limitation of the assertion library rather than the framework: the line
        // above already establishes the value is present, but NotBeNull does not narrow nullability
        // the way an `is not null` check the compiler can see would.
        shortSend!.Value.BytesSent.Should().BeLessThan(chunk.Length);
    }

    [Fact]
    public void Send_ShortSendFollowedByCounterpartyResumingReads_ArmsExactlyOneWritable()
    {
        // Arrange — same short-send provocation as above, then the counterparty resumes reading on a
        // background thread so the socket's own send buffer drains and becomes writable again. Whether
        // a real drain is observed is genuine OS-driven asynchrony, not a loopback-local decision, so
        // this bounds wall-clock attempts rather than Pump count alone — the same shape already used by
        // RawReactorHarness.PumpAndDrainReasonsUntilClosed for the same reason. How long the drain
        // takes to register is a platform matter too: Linux calls a TCP socket writable only once
        // roughly half its send buffer is free, so the counterparty has to get real bytes off the wire
        // before this can pass, not merely start reading. CreateCounterpartyListener's receive-buffer
        // parameter documents why that shrink has to happen before the handshake for it to.
        using RawReactorHarness harness = new(capacity: 4);
        (Socket counterpartyListener, IPEndPoint serverEndPoint) = harness.CreateCounterpartyListener(receiveBufferSize: 1024);
        LinkId link = harness.Reactor.Connect(serverEndPoint);
        harness.PumpAndDrainAll(pumps: 3);
        using Socket counterpartyAccepted = counterpartyListener.Accept();

        byte[] chunk = new byte[65_536];
        Random.Shared.NextBytes(chunk);

        bool sawShortSend = false;
        for (int attempt = 0; attempt < 200 && !sawShortSend; attempt++)
        {
            SendResult result = harness.Reactor.Send(link, chunk);

            // A failed send reports zero bytes and so satisfies the short-send test below without
            // arming anything; only a successful one that took less than it was offered arms Writable,
            // and it is that arming this test goes on to make its assertions about.
            result.IsSuccess.Should().BeTrue("nothing about this Link has failed — the counterparty simply is not reading");
            sawShortSend = result.BytesSent < chunk.Length;
        }

        sawShortSend.Should().BeTrue("a counterparty that never reads must eventually force a short send");

        // No Writable yet: the counterparty still has not read anything back off the wire.
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> beforeResume = harness.PumpAndDrainAll(pumps: 3);
        beforeResume.Should().NotContain(evt => evt.Kind == RawEventKind.Writable);

        // Act — resume reading on a background thread so the send buffer actually drains.
        bool stopDraining = false;
        Thread drainThread = new(() =>
        {
            byte[] sink = new byte[8192];
            try
            {
                while (!Volatile.Read(ref stopDraining))
                {
                    int read = counterpartyAccepted.Receive(sink);
                    if (read == 0)
                    {
                        break;
                    }
                }
            }
            catch (SocketException)
            {
                // Expected once the socket is shut down/disposed below to unblock this loop.
            }
            catch (ObjectDisposedException)
            {
                // Expected for the same reason.
            }
        });
        drainThread.Start();

        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> afterResume = [];
        for (int i = 0; i < 100 && !afterResume.Exists(evt => evt.Kind == RawEventKind.Writable); i++)
        {
            harness.Reactor.Pump();
            afterResume.AddRange(harness.DrainToList());
            if (!afterResume.Exists(evt => evt.Kind == RawEventKind.Writable))
            {
                Thread.Sleep(20);
            }
        }

        // Assert — exactly one Writable for this Link, and no second one without another short send.
        afterResume.Should().ContainSingle(evt => evt.Kind == RawEventKind.Writable && evt.Link == link);

        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> further = harness.PumpAndDrainAll(pumps: 10);
        further.Should().NotContain(evt => evt.Kind == RawEventKind.Writable);

        Volatile.Write(ref stopDraining, true);
        counterpartyAccepted.Close();
        drainThread.Join(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void Pump_SixtyFourIdleLinksNeverSentOn_ProduceZeroWritableEventsAcrossManyPumps()
    {
        // Arrange — this is the test that pins the arming rule: a level-triggered Writable would
        // produce up to one non-event per Link per Pump here, and this asserts there are none.
        using RawReactorHarness harness = new(capacity: 70);
        (Socket counterpartyListener, IPEndPoint serverEndPoint) = harness.CreateCounterpartyListener();
        List<LinkId> links = [];
        for (int i = 0; i < 64; i++)
        {
            links.Add(harness.Reactor.Connect(serverEndPoint));
        }

        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> establishing = harness.PumpAndDrainAll(pumps: 10);
        establishing.Count(evt => evt.Kind == RawEventKind.Connected).Should().Be(64);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> afterIdling = harness.PumpAndDrainAll(pumps: 20);

        // Assert
        afterIdling.Should().NotContain(evt => evt.Kind == RawEventKind.Writable);
        counterpartyListener.Dispose();
    }

    [Fact]
    public void Shutdown_AcceptedStreamLink_SucceedsAndCounterpartyObservesEndOfStream()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        harness.DrainToList();

        // Act
        Result result = harness.Reactor.Shutdown(acceptedLink);

        // Assert
        result.IsSuccess.Should().BeTrue();
        byte[] buffer = new byte[16];
        int read = counterparty.Receive(buffer);
        read.Should().Be(0, "our FIN reaches the counterparty as ordinary end-of-stream");
    }

    [Fact]
    public void Shutdown_AcceptedStreamLink_LinkStaysOpenForReading()
    {
        // Arrange — half-close, not full close: the counterparty can still send to us afterwards.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        harness.DrainToList();
        harness.Reactor.Shutdown(acceptedLink).IsSuccess.Should().BeTrue();

        byte[] payload = [9, 8, 7];
        counterparty.Send(payload);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.PumpAndDrainAll(pumps: 3);

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.SegmentReceived && evt.Link == acceptedLink)
            .Which.Payload.Should().Equal(payload);
    }

    [Fact]
    public void Send_AfterShutdown_FailsWithoutThrowing()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        harness.DrainToList();
        harness.Reactor.Shutdown(acceptedLink).IsSuccess.Should().BeTrue();

        // Act
        SendResult result = harness.Reactor.Send(acceptedLink, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.BytesSent.Should().Be(0);
    }

    [Fact]
    public void Shutdown_AlreadyShutdownLink_ReturnsFailedResultRatherThanThrowing()
    {
        // Arrange — Shutdown is a one-way door.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        harness.DrainToList();
        harness.Reactor.Shutdown(acceptedLink).IsSuccess.Should().BeTrue();

        // Act
        Result result = harness.Reactor.Shutdown(acceptedLink);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public void Send_UpdatesCountersOfByExactlyTheBytesTheSocketTook()
    {
        // Arrange — the counter tracks what the socket actually took, not what was offered.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        harness.DrainToList();
        LinkCounters before = harness.Reactor.CountersOf(acceptedLink);
        byte[] payload = [1, 2, 3, 4, 5, 6, 7, 8];

        // Act
        SendResult result = harness.Reactor.Send(acceptedLink, payload);

        // Assert
        result.IsSuccess.Should().BeTrue();
        LinkCounters after = harness.Reactor.CountersOf(acceptedLink);
        (after.BytesSent - before.BytesSent).Should().Be((ulong)result.BytesSent);
    }
}
