using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Listening Links, accepting inside a Pump, and the shared admission cap — sub-issue 2.1. Every
/// assertion is against the drained <see cref="RawEvent"/>s, a <see cref="Result"/> or a
/// <see cref="DiagnosticRecord"/>; nothing here reaches into how or when a connection was detected.
/// </summary>
public class RawReactorListenTests
{
    [Fact]
    public void Listen_PortZero_ReturnsNonZeroLocalPort()
    {
        // Arrange
        using RawReactorHarness harness = new();

        // Act
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));

        // Assert
        harness.Reactor.LocalOf(listener).Port.Should().NotBe(0);
    }

    [Fact]
    public void Pump_CounterpartyConnects_DrainsOneAcceptedNamingListenerAndNewLink()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId listener, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);

        // Act
        harness.Reactor.Pump();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Accepted);
    }

    [Fact]
    public void Pump_CounterpartyConnects_AcceptedLinkIsNewAndDistinctFromListener()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId listener, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(localEndPoint);

        // Act
        harness.Reactor.Pump();
        RawEvent evt = FirstAccepted(harness);

        // Assert
        evt.Listener.Should().Be(listener);
        evt.Link.Should().NotBe(listener);
    }

    [Fact]
    public void Pump_TwoListenersOnDifferentPorts_AcceptedEventsAreDistinguishableByListener()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 6);
        (LinkId firstListener, IPEndPoint firstEndPoint) = harness.ListenOnLoopback();
        (LinkId secondListener, IPEndPoint secondEndPoint) = harness.ListenOnLoopback();
        using Socket firstCounterparty = harness.ConnectCounterpartyTo(firstEndPoint);
        using Socket secondCounterparty = harness.ConnectCounterpartyTo(secondEndPoint);

        // Act
        harness.Reactor.Pump();
        List<LinkId> listenersSeen = [];
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            if (evt.Kind == RawEventKind.Accepted)
            {
                listenersSeen.Add(evt.Listener);
            }
        }

        // Assert
        listenersSeen.Should().BeEquivalentTo([firstListener, secondListener]);
    }

    [Fact]
    public void Pump_MoreConnectionsPendingThanTheAdmissionCap_AcceptsExactlyTheCapInOnePump()
    {
        // Arrange — cap of 2 with 4 connections already sitting in the OS backlog before the first Pump.
        using RawReactorHarness harness = new(capacity: 8, admissionCap: 2);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket first = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket second = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket third = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket fourth = harness.ConnectCounterpartyTo(localEndPoint);

        // Act
        harness.Reactor.Pump();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Count(evt => evt.Kind == RawEventKind.Accepted).Should().Be(2);
    }

    [Fact]
    public void Pump_MoreConnectionsPendingThanTheAdmissionCap_LogsAdmissionCapReachedExactlyOnce()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 8, admissionCap: 2);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket first = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket second = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket third = harness.ConnectCounterpartyTo(localEndPoint);

        // Act
        harness.Reactor.Pump();

        // Assert
        harness.Sink.Received.Should().ContainSingle(record => record.Id == DiagnosticId.AdmissionCapReached)
            .Which.Value0.Should().Be(2);
    }

    [Fact]
    public void Pump_MoreConnectionsPendingThanTheAdmissionCap_RemainderArriveOnALaterPump()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 8, admissionCap: 2);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket first = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket second = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket third = harness.ConnectCounterpartyTo(localEndPoint);
        harness.Reactor.Pump();
        harness.DrainToList();

        // Act — the connection deferred past the cap is still sitting in the backlog, unrefused.
        harness.Reactor.Pump();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Should().ContainSingle(evt => evt.Kind == RawEventKind.Accepted);
    }

    [Fact]
    public void Send_ListeningLink_ReturnsFailedResult()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        SendResult result = harness.Reactor.Send(listener, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public void SendTo_ListeningLink_ReturnsFailedResult()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));
        Source destination = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 12345));

        // Act
        Result result = harness.Reactor.SendTo(listener, destination, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public void Shutdown_ListeningLink_ReturnsFailedResult()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId listener = harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Result result = harness.Reactor.Shutdown(listener);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public void Listen_ConsumesACapacitySlot_SoBindFailsOnceCapacityIsSpent()
    {
        // Arrange — capacity 1: the listener alone should exhaust it, proving a listening Link is a
        // registered share-holder like any other Link.
        using RawReactorHarness harness = new(capacity: 1);
        harness.Reactor.Listen(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Action act = () => harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*capacity*");
    }

    [Fact]
    public void Pump_MorePendingThanFreeSlotsWithAdmissionCapAboveCapacity_AcceptsOnlyAsManyAsThereAreFreeSlots()
    {
        // Arrange — capacity 2 (the listener plus exactly one free slot) with the admission cap raised
        // above capacity, so the slot count, not the cap, is what runs out first.
        using RawReactorHarness harness = new(capacity: 2, admissionCap: 5);
        (_, IPEndPoint localEndPoint) = harness.ListenOnLoopback();
        using Socket first = harness.ConnectCounterpartyTo(localEndPoint);
        using Socket second = harness.ConnectCounterpartyTo(localEndPoint);

        // Act
        harness.Reactor.Pump();
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Count(evt => evt.Kind == RawEventKind.Accepted).Should().Be(1);
    }

    private static RawEvent FirstAccepted(RawReactorHarness harness)
    {
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            if (evt.Kind == RawEventKind.Accepted)
            {
                return evt;
            }
        }

        throw new InvalidOperationException("No Accepted event was drained.");
    }
}
