using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class RawReactorTests
{
    [Fact]
    public void Bind_PortZero_ReturnsNonZeroLocalPort()
    {
        // Arrange
        using RawReactorHarness harness = new();

        // Act
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Assert
        harness.Reactor.LocalOf(link).Port.Should().NotBe(0);
    }

    [Fact]
    public void Bind_SamePinnedPortTwice_ThrowsNamingThePort()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId first = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        int pinnedPort = harness.Reactor.LocalOf(first).Port;

        // Act
        Action act = () => harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, pinnedPort));

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage($"*{pinnedPort}*");
    }

    [Fact]
    public void Bind_IPv6AnyAndIPv4AnyOnSamePort_ReturnsTwoIndependentLinks()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId ipv6Link = harness.Reactor.Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
        int pinnedPort = harness.Reactor.LocalOf(ipv6Link).Port;

        // Act
        LinkId ipv4Link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Any, pinnedPort));

        // Assert
        ipv4Link.Should().NotBe(ipv6Link);
        harness.Reactor.LocalOf(ipv4Link).Port.Should().Be(pinnedPort);
        harness.Reactor.LocalOf(ipv6Link).Port.Should().Be(pinnedPort);
    }

    [Fact]
    public void Drain_CounterpartySendsIPv4ToIPv6AnyLink_NeverYieldsDatagramReceived()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId ipv6Link = harness.Reactor.Bind(new IPEndPoint(IPAddress.IPv6Any, 0));
        int pinnedPort = harness.Reactor.LocalOf(ipv6Link).Port;

        using Socket ipv4Counterparty = harness.CreateCounterpartySocket();
        ipv4Counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        ipv4Counterparty.SendTo([1, 2, 3], new IPEndPoint(IPAddress.Loopback, pinnedPort));

        // Act
        bool datagramReceivedSeen = false;
        for (int i = 0; i < 5; i++)
        {
            harness.Reactor.Pump();
            foreach (RawEvent evt in harness.Reactor.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    datagramReceivedSeen = true;
                }
            }
        }

        // Assert
        datagramReceivedSeen.Should().BeFalse();
    }

    [Fact]
    public void Drain_AfterCounterpartySendsAndOnePump_YieldsOneDatagramReceivedWithCorrectSource()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint localEndPoint = harness.Reactor.LocalOf(link);

        using Socket counterparty = harness.CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] payload = [1, 2, 3, 4, 5];
        counterparty.SendTo(payload, localEndPoint);

        // Act
        harness.Reactor.Pump();

        // Assert
        int eventCount = 0;
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            eventCount++;
            evt.Kind.Should().Be(RawEventKind.DatagramReceived);
            evt.Link.Should().Be(link);
            evt.Source.Port.Should().Be(((IPEndPoint)counterparty.LocalEndPoint!).Port);
            evt.Payload.ToArray().Should().Equal(payload);
        }

        eventCount.Should().Be(1);
    }

    [Fact]
    public void Drain_AtDefaultScratchSize_LargeDatagramArrivesWholeWithoutTruncation()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint localEndPoint = harness.Reactor.LocalOf(link);

        using Socket counterparty = harness.CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] payload = new byte[RawReactor.MaxUdpDatagramSize];
        Random.Shared.NextBytes(payload);
        counterparty.SendTo(payload, localEndPoint);

        // Act
        harness.Reactor.Pump();

        // Assert
        int eventCount = 0;
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            eventCount++;
            evt.Kind.Should().Be(RawEventKind.DatagramReceived);
            evt.Payload.ToArray().Should().Equal(payload);
        }

        eventCount.Should().Be(1);
        harness.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramTruncated);
    }

    [Fact]
    public void Drain_WithLoweredScratchSize_OversizedDatagramIsTruncatedAndReported()
    {
        // Arrange
        using RawReactorHarness harness = new(scratchBufferSize: 16);
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint localEndPoint = harness.Reactor.LocalOf(link);

        using Socket counterparty = harness.CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] payload = new byte[100];
        Random.Shared.NextBytes(payload);
        counterparty.SendTo(payload, localEndPoint);

        // Act
        harness.Reactor.Pump();

        // Assert
        int eventCount = 0;
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            eventCount++;
        }

        eventCount.Should().Be(0);
        harness.Sink.Received.Should().ContainSingle(record => record.Id == DiagnosticId.DatagramTruncated);
    }

    [Fact]
    public void Pump_SendToClosedPort_ProducesNoClosedEventAndOneDestinationUnreachableRecord()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        int closedPort;
        using (Socket probe = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp))
        {
            probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            closedPort = ((IPEndPoint)probe.LocalEndPoint!).Port;
        }

        Source destination = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, closedPort));
        Result sendResult = harness.Reactor.SendTo(link, destination, [1, 2, 3]);
        sendResult.IsSuccess.Should().BeTrue();

        // Act
        bool closedEventSeen = false;
        for (int i = 0; i < 20 && harness.Sink.Received.Count == 0; i++)
        {
            harness.Reactor.Pump();
            foreach (RawEvent evt in harness.Reactor.Drain())
            {
                if (evt.Kind == RawEventKind.Closed)
                {
                    closedEventSeen = true;
                }
            }
        }

        // Assert
        closedEventSeen.Should().BeFalse();
        harness.Sink.Received.Should().ContainSingle(record => record.Id == DiagnosticId.DestinationUnreachable);
    }

    [Fact]
    public void Send_OnDatagramLink_ReturnsFailedResultNamingTheIllegality()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Result result = harness.Reactor.Send(link, [1, 2, 3]);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void Shutdown_OnDatagramLink_ReturnsFailedResultNamingTheIllegality()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Result result = harness.Reactor.Shutdown(link);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void Close_OpenLink_QueuesExactlyOneClosedEventWithLocalClose()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        harness.Reactor.Close(link);

        // Assert
        List<CloseReason> reasons = [];
        foreach (RawEvent evt in harness.Reactor.Drain())
        {
            evt.Kind.Should().Be(RawEventKind.Closed);
            evt.Link.Should().Be(link);
            reasons.Add(evt.Reason);
        }

        reasons.Should().Equal(CloseReason.LocalClose);
    }

    [Fact]
    public void Bind_AfterClosingPreviousLinkInSameSlot_StaleLinkIdNoLongerAddressesTheSlot()
    {
        // Arrange
        using RawReactorHarness harness = new(capacity: 1);
        LinkId original = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        harness.Reactor.Close(original);

        // Act
        LinkId replacement = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Assert
        replacement.Should().NotBe(original);
        Action act = () => harness.Reactor.LocalOf(original);
        act.Should().Throw<ArgumentException>();
    }
}
