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
            // `!` works around a framework limitation: Socket.LocalEndPoint is typed EndPoint? for a
            // Socket's whole lifetime, but this one was bound in the Arrange above.
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
    public void Drain_WithLoweredScratchSizeAndAnOversizedDatagram_YieldsNothing()
    {
        // Arrange
        using RawReactorHarness harness = new(scratchBufferSize: 16);
        SendOversizedDatagramAndPump(harness);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.DrainToList();

        // Assert
        drained.Should().BeEmpty();
    }

    [Fact]
    public void Pump_WithLoweredScratchSizeAndAnOversizedDatagram_RecordsDatagramTruncated()
    {
        // Arrange
        using RawReactorHarness harness = new(scratchBufferSize: 16);

        // Act
        SendOversizedDatagramAndPump(harness);

        // Assert
        harness.Sink.Received.Should().ContainSingle(record => record.Id == DiagnosticId.DatagramTruncated);
    }

    [Fact]
    public void Pump_WithQueuedDatagramsTotallingMoreThanTheScratchBuffer_DeliversEveryDatagram()
    {
        // Arrange
        const int scratchSize = 2048;
        const int datagramCount = 10;
        using RawReactorHarness harness = new(scratchBufferSize: scratchSize);
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint localEndPoint = harness.Reactor.LocalOf(link);

        using Socket counterparty = harness.CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        List<byte[]> sent = [];
        for (int i = 0; i < datagramCount; i++)
        {
            byte[] payload = new byte[500];
            Array.Fill(payload, (byte)(i + 1));
            sent.Add(payload);
            counterparty.SendTo(payload, localEndPoint);
        }

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.PumpUntilDrained(
            events => events.Count >= datagramCount);

        // Assert
        drained.Should().HaveCount(datagramCount);
        drained.Select(evt => evt.Kind).Should().OnlyContain(kind => kind == RawEventKind.DatagramReceived);
        drained.Select(evt => evt.Payload).Should().Equal(sent, (actual, expected) => actual.AsSpan().SequenceEqual(expected));
        harness.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramTruncated);
    }

    [Fact]
    public void Pump_WithADatagramOfExactlyTheScratchBufferSize_DeliversItWhole()
    {
        // Arrange
        const int scratchSize = 64;
        using RawReactorHarness harness = new(scratchBufferSize: scratchSize);
        (byte[] payload, _) = SendDatagramOfSize(harness, scratchSize);

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = harness.PumpUntilDrained(
            events => events.Count >= 1);

        // Assert
        drained.Should().ContainSingle().Which.Payload.Should().Equal(payload);
        harness.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramTruncated);
    }

    [Fact]
    public void Pump_WithADatagramOneByteOverTheScratchBufferSize_RecordsOneDatagramTruncatedReportingTheScratchSize()
    {
        // Arrange
        const int scratchSize = 64;
        using RawReactorHarness harness = new(scratchBufferSize: scratchSize);
        (_, IPEndPoint sender) = SendDatagramOfSize(harness, scratchSize + 1);

        // Act
        harness.PumpUntil(() => harness.Sink.Received.Any(record => record.Id == DiagnosticId.DatagramTruncated));

        // Assert
        DiagnosticRecord truncated = harness.Sink.Received
            .Where(record => record.Id == DiagnosticId.DatagramTruncated)
            .Should().ContainSingle().Subject;
        truncated.Value0.Should().Be(scratchSize);
        truncated.Source.Should().Be(Source.FromIPEndPoint(sender));
    }

    [Fact]
    public void Drain_AfterSendToAClosedPort_NeverYieldsAClosedEvent()
    {
        // Arrange
        using RawReactorHarness harness = new();

        // Act
        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained =
            SendToAClosedPortAndPumpUntilReported(harness);

        // Assert
        drained.Should().NotContain(evt => evt.Kind == RawEventKind.Closed);
    }

    [Fact]
    public void Pump_AfterSendToAClosedPort_RecordsDestinationUnreachable()
    {
        // Arrange
        using RawReactorHarness harness = new();

        // Act
        SendToAClosedPortAndPumpUntilReported(harness);

        // Assert
        harness.Sink.Received.Should().ContainSingle(record => record.Id == DiagnosticId.DestinationUnreachable);
    }

    [Fact]
    public void Send_OnAnOpenDatagramLink_ReturnsFailedResultNamingTheIllegality()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        SendResult result = harness.Reactor.Send(link, [1, 2, 3]);

        // Assert — the message, not just the failure: a stale LinkId also fails here, and these two
        // tests only distinguish the datagram rule from the not-open rule if they read what it says.
        result.Error.Should().Contain("illegal on a datagram Link");
    }

    [Fact]
    public void Send_OnALinkIdThatIsNotOpen_ReturnsFailedResultSayingSo()
    {
        // Arrange — a Link closed under the caller, which is the ordinary race rather than misuse.
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        harness.Reactor.Close(link);

        // Act
        SendResult result = harness.Reactor.Send(link, [1, 2, 3]);

        // Assert
        result.Error.Should().Contain("is not open");
    }

    [Fact]
    public void Shutdown_OnAnOpenDatagramLink_ReturnsFailedResultNamingTheIllegality()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        Result result = harness.Reactor.Shutdown(link);

        // Assert
        result.Error.Should().Contain("illegal on a datagram Link");
    }

    [Fact]
    public void Shutdown_OnALinkIdThatIsNotOpen_ReturnsFailedResultSayingSo()
    {
        // Arrange
        using RawReactorHarness harness = new();
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        harness.Reactor.Close(link);

        // Act
        Result result = harness.Reactor.Shutdown(link);

        // Assert
        result.Error.Should().Contain("is not open");
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

    /// <summary>
    /// Binds a Link, sends it a datagram far larger than the harness's scratch buffer, and Pumps once.
    /// Shared so the two truncation facts — nothing is delivered, and one record is written — can be
    /// asserted by one test each without either restating the setup.
    /// </summary>
    private static void SendOversizedDatagramAndPump(RawReactorHarness harness)
    {
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint localEndPoint = harness.Reactor.LocalOf(link);

        using Socket counterparty = harness.CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] payload = new byte[100];
        Random.Shared.NextBytes(payload);
        counterparty.SendTo(payload, localEndPoint);

        harness.Reactor.Pump();
    }

    /// <summary>
    /// Binds a Link on the harness's Reactor and sends it one random datagram of <paramref name="size"/>
    /// bytes. Returns the payload and the endpoint it was sent from.
    /// </summary>
    private static (byte[] Payload, IPEndPoint Sender) SendDatagramOfSize(RawReactorHarness harness, int size)
    {
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint localEndPoint = harness.Reactor.LocalOf(link);

        using Socket counterparty = harness.CreateCounterpartySocket();
        counterparty.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        byte[] payload = new byte[size];
        Random.Shared.NextBytes(payload);
        counterparty.SendTo(payload, localEndPoint);
        return (payload, (IPEndPoint)counterparty.LocalEndPoint!); // non-null: the socket was bound above
    }

    /// <summary>
    /// How many ports this will try before giving up and letting the assertion fail. A port that
    /// nothing holds produces the record on the first attempt; each retry exists only for the port
    /// having been taken between the probe and the send, and a fresh probe gets a different one.
    /// </summary>
    private const int ClosedPortAttempts = 3;

    /// <summary>
    /// Sends to a port nothing is listening on and Pumps until the resulting ICMP port-unreachable
    /// comes back as a record, returning everything drained along the way. Bounded by a real-time
    /// deadline rather than a Pump count (issue #153, <see cref="PumpDeadline"/>): an ICMP reply is
    /// delivered on the OS clock, not on how many times this process asks for it, so a platform that
    /// never reports it fails the assertion rather than hanging.
    /// </summary>
    /// <remarks>
    /// <b>Why this retries.</b> The destination is an ephemeral port a probe socket has just released,
    /// and nothing keeps it released — any of the ~1000 tests in this run that binds port 0 can be
    /// handed that exact port in the window before the datagram arrives. The datagram is then
    /// delivered rather than refused, no ICMP is generated, and the pump loop spends its whole budget
    /// on a port that was never closed. That is what failed on the Windows CI runner, where the
    /// parallel test load and the two cores make the window far wider than on a developer machine.
    /// A retry is sound rather than a papered-over flake because a taken port generates nothing
    /// that can arrive late: loopback ICMP comes back in microseconds, so an attempt that recorded
    /// nothing in two seconds recorded nothing at all, and the next attempt starts from an empty Sink.
    /// </remarks>
    private static List<(RawEventKind Kind, LinkId Link, byte[] Payload)> SendToAClosedPortAndPumpUntilReported(
        RawReactorHarness harness)
    {
        LinkId link = harness.Reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        List<(RawEventKind Kind, LinkId Link, byte[] Payload)> drained = [];
        for (int attempt = 1; ; attempt++)
        {
            Source destination = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, ProbeForAClosedPort()));
            harness.Reactor.SendTo(link, destination, [1, 2, 3]).IsSuccess.Should().BeTrue();
            drained.AddRange(harness.PumpUntilDrained(_ => harness.Sink.Received.Count > 0));

            if (harness.Sink.Received.Count > 0 || attempt == ClosedPortAttempts)
            {
                return drained;
            }
        }
    }

    /// <summary>
    /// A loopback port that was free a moment ago: bound by a probe socket and released when it
    /// closes. This is as close to "closed" as a test can ask the OS for — a port cannot be reserved
    /// and closed at the same time — which is why the caller retries.
    /// </summary>
    private static int ProbeForAClosedPort()
    {
        using Socket probe = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        // `!` works around a framework limitation: Socket.LocalEndPoint is typed EndPoint? for a
        // Socket's whole lifetime, but this one has just returned from Bind.
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
    }
}
