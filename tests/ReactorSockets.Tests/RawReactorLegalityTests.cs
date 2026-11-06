using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The Link legality matrix, <c>RemoteOf</c>, and the remaining <c>Close</c>/stale-<see cref="LinkId"/>
/// cells sub-issue 2.5 asks be looked at as a whole rather than per sub-task. Every cell
/// <c>Send</c>/<c>SendTo</c>/<c>Shutdown</c>/<c>Close</c>/<c>RemoteOf</c>/<c>LocalOf</c> against
/// datagram/stream/listening already has coverage scattered through the sub-2.1-2.4 test files; this
/// file fills the cells nothing else exercises — <c>Close</c> on a listener,
/// <see cref="RawReactor.RemoteOf"/> against every Link kind, and a stale <see cref="LinkId"/> whose
/// slot was reused by a different Link kind — plus the wrong-kind consistency check the parent spec
/// asks for by name.
/// </summary>
public class RawReactorLegalityTests
{
    [Fact]
    public void SendTo_DefaultLinkId_FailsEvenWhenTheFirstBoundLinkOccupiesSlotZero()
    {
        // Arrange — the first Link bound occupies slot 0, so a default LinkId (0, 0) would address it
        // if generations began at 0. They begin at 1, which is what makes default unaddressable: no
        // live slot ever carries generation 0, so nothing has to check for it.
        using RawReactorHarness harness = new();
        (LinkId first, _) = harness.BindLoopbackLink();

        // Act
        Result result = harness.Reactor.SendTo(default, Source.FromIPEndPoint(harness.Reactor.LocalOf(first)), [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
        first.Should().NotBe(default(LinkId));
    }

    [Fact]
    public void LocalOf_DefaultLinkId_ThrowsRatherThanAnsweringForSlotZero()
    {
        // Arrange — the returning-data counterpart to the Result-returning case above: a default
        // LinkId must be rejected identically whichever channel the member reports through.
        using RawReactorHarness harness = new();
        harness.BindLoopbackLink();

        // Act
        Action localOfDefault = () => harness.Reactor.LocalOf(default);

        // Assert
        localOfDefault.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Close_ListeningLink_DrainsExactlyOneClosedEventWithLocalClose()
    {
        // Arrange — the one Close cell no other file exercises: a listening Link is a Link like any
        // other and must close the same way a datagram or stream Link does.
        using RawReactorHarness harness = new();
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        harness.Reactor.Close(listener);
        List<(RawEventKind Kind, LinkId Link, CloseReason Reason)> drained = harness.DrainToReasonList();

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Closed && evt.Link == listener)
            .Which.Reason.Should().Be(CloseReason.LocalClose);
    }

    [Fact]
    public void RemoteOf_AcceptedStreamLink_ReturnsCounterpartysLocalAddress()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();

        // Act
        IPEndPoint remote = harness.Reactor.RemoteOf(acceptedLink);

        // Assert — `!` works around a framework limitation: Socket.LocalEndPoint is typed EndPoint?
        // for a Socket's whole lifetime, but this one is connected by the time Pump above returns.
        remote.Port.Should().Be(((IPEndPoint)counterparty.LocalEndPoint!).Port);
    }

    [Fact]
    public void RemoteOf_DialledStreamLink_ReturnsTheEndpointConnectWasGiven()
    {
        // Arrange — the dial side: RemoteOf is answerable the instant Connect returns, before the
        // handshake resolves, exactly like LocalOf already is.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint serverEndPoint) = harness.ListenOnLoopback();

        // Act
        LinkId dialLink = harness.Reactor.Connect(serverEndPoint);

        // Assert
        harness.Reactor.RemoteOf(dialLink).Should().Be(serverEndPoint);
    }

    [Fact]
    public void RemoteOf_DatagramLink_ThrowsArgumentException()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Action act = () => harness.Reactor.RemoteOf(link);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*not a stream Link*");
    }

    [Fact]
    public void RemoteOf_ListeningLink_ThrowsArgumentException()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Action act = () => harness.Reactor.RemoteOf(listener);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*not a stream Link*");
    }

    [Fact]
    public void RemoteOf_LinkIdThatIsNotOpen_ThrowsArgumentExceptionSayingSo()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        harness.Reactor.Close(link);

        // Act
        Action act = () => harness.Reactor.RemoteOf(link);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*is not open*");
    }

    [Fact]
    public void RemoteOf_StaleLinkIdWhoseSlotWasReusedByADatagramLink_ThrowsRatherThanAddressingTheNewOccupant()
    {
        // Arrange — the generation check is generic across Link kinds: a slot a stream Link vacated
        // and a datagram Link then took over must not let the old LinkId answer for the new occupant.
        using RawReactorHarness harness = new(capacity: 1);
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));
        harness.Reactor.Close(listener);
        harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Action act = () => harness.Reactor.RemoteOf(listener);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*is not open*");
    }

    [Fact]
    public void Send_StaleLinkIdWhoseSlotWasReusedByADifferentStreamLink_FailsRatherThanAddressingTheNewOccupant()
    {
        // Arrange — the same generation check exercised through a Result-returning member instead of
        // a throwing one: a stale id must fail cleanly rather than reach whatever now sits in the slot.
        using RawReactorHarness harness = new(capacity: 2);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        LinkId original = harness.Reactor.Connect(localEndPoint);
        harness.Reactor.Close(original);
        LinkId replacement = harness.Reactor.Connect(localEndPoint);

        // Act
        SendResult result = harness.Reactor.Send(original, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().Contain("is not open");
        replacement.Should().NotBe(original);
    }

    [Fact]
    public void SendTo_AcceptedStreamLink_ReturnsFailedResult()
    {
        // Arrange — the accept side of the SendTo-on-stream cell; RawReactorDialTests already covers
        // the dial side, and this proves the same rule holds for the other way a stream Link is born.
        using RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        LinkId acceptedLink = harness.FirstAcceptedLink();
        Source destination = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 12345));

        // Act
        Result result = harness.Reactor.SendTo(acceptedLink, destination, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().Contain("stream Link");
    }

    [Fact]
    public void Dispose_WithLinksOfEveryKindOpen_ReleasesEverySocketSoTheirPortsCanBeRebound()
    {
        // Arrange — one Reactor dialling its own listener holds all three registered kinds at once: a
        // datagram Link, the listening Link, the accepted connection it produced and the dialling Link
        // that produced it — then disposed with all four still open.
        ReactorOptions options = new() { Capacity = 6 };
        int datagramPort;
        int listenerPort;
        using (RawReactor first = new(options))
        {
            LinkId datagram = first.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            LinkId listener = first.Listen(new IPEndPoint(IPAddress.Loopback, 0));
            first.Connect(first.LocalOf(listener));
            first.Pump();
            first.Pump();
            first.Pump();

            datagramPort = first.LocalOf(datagram).Port;
            listenerPort = first.LocalOf(listener).Port;

            // Act (still inside the using block: Dispose runs with every kind of Link still open)
        }

        // Assert — every socket first bound was truly closed, not merely dropped, so a second Reactor
        // can claim the exact same ports.
        using RawReactor second = new(options);
        Action rebindDatagram = () => second.Bind(new IPEndPoint(IPAddress.Loopback, datagramPort));
        Action rebindListener = () => second.Listen(new IPEndPoint(IPAddress.Loopback, listenerPort));
        rebindDatagram.Should().NotThrow();
        rebindListener.Should().NotThrow();
    }

    [Fact]
    public void Dispose_WithAHalfOpenStreamLinkFromEndOfStream_DisposesWithoutThrowing()
    {
        // Arrange — the half-open state the parent spec documents as leakable: a Link that received
        // EndOfStream and was never Closed must still be disposed cleanly, not left to fault Dispose.
        RawReactorHarness harness = new(capacity: 4);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        counterparty.Shutdown(SocketShutdown.Send);
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle(evt => evt.Kind == RawEventKind.EndOfStream);

        // Act
        Action act = harness.Dispose;

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_WithLinksOfEveryKindOpen_LeavesNoEventsToDrainRatherThanQueueingAClosedForEach()
    {
        // Arrange — the other half of disposal: closing every Link must produce no
        // Events, because by the time Dispose returns there is nobody left to drain them. Disposal
        // also discards Events already queued, so this pends a real Closed first and proves even that
        // one is unreachable afterwards, rather than only proving Dispose queued nothing new.
        RawReactor reactor = new(new ReactorOptions { Capacity = 6 });
        LinkId datagram = reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        LinkId listener = reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));
        reactor.Connect(reactor.LocalOf(listener));
        reactor.Close(datagram);

        // Act
        reactor.Dispose();

        // Assert — Drain is the only way to observe an Event at all, so a disposed Reactor refusing it
        // is what "produces no Events" means from the outside.
        Action drain = () =>
        {
            foreach (RawEvent _ in reactor.Drain())
            {
            }
        };
        drain.Should().Throw<ObjectDisposedException>();
    }
}
