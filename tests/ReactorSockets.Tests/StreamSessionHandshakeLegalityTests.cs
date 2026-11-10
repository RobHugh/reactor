using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A stream handshake record is held to the same legality rules the datagram profile's
/// control codec applies, so both transports reject the same malformed inputs. A Hello announcing a
/// zero <c>MaxMessageSize</c>, a Refusal naming no known reason, and a Refusal or Resume whose padding
/// is not zero are all malformed. Driven from outside the L2 seam (tests/CLAUDE.md) by plain sockets
/// writing a hand-built record. The announced intervals are covered by
/// <see cref="StreamSessionHostileAnnouncedIntervalTests"/>.
/// </summary>
public class StreamSessionHandshakeLegalityTests
{
    private const byte RefusalReasonCapacity = 2;
    private const byte RefusalReasonProtocolViolation = 3;

    /// <summary>
    /// A zero <c>MaxMessageSize</c> is malformed whatever version the Hello offers, as it is in the
    /// datagram profile's HelloWithCookie.
    /// </summary>
    public static TheoryData<uint> VersionsAZeroMaxMessageSizeIsMalformedFor =>
        new() { StreamSessionTestRecords.ProtocolVersion, 2 };

    /// <summary>Reason bytes no member of the wire roster names: below, just above and far above it.</summary>
    public static TheoryData<byte> UnknownReasons => new() { 0, 4, byte.MaxValue };

    [Theory]
    [MemberData(nameof(VersionsAZeroMaxMessageSizeIsMalformedFor))]
    public void Pump_HelloAnnouncingAZeroMaxMessageSize_DiscardsItUnansweredAndKeepsPumping(uint version)
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using Socket hostile = ConnectTo(accepter.Endpoint);

        // Act
        hostile.Send(StreamSessionTestRecords.BuildHello(version: version, maxMessageSize: 0));
        (int bytesAnswered, List<SessionEventKind> events) = PumpUntilClosedBy(accepter, hostile);
        Action pump = accepter.Reactor.Pump;

        // Assert — no Hello or Refusal came back, and no Peer was ever raised or closed.
        pump.Should().NotThrow();
        bytesAnswered.Should().Be(0);
        events.Should().BeEmpty();
    }

    [Fact]
    public void Pump_HelloAnnouncingAZeroMaxMessageSizeToADialler_ClosesThePeerWithoutEstablishingIt()
    {
        // Arrange
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        using Socket listener = Listen();
        using Socket hostile = AcceptDialFrom(dialler, listener);

        // Act
        hostile.Send(StreamSessionTestRecords.BuildHello(maxMessageSize: 0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));

        // Assert
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerClosed && evt.Reason == DisconnectReason.TransportFailed);
    }

    [Theory]
    [MemberData(nameof(UnknownReasons))]
    public void Pump_RefusalNamingAnUnknownReason_ClosesThePeerAsTransportFailedRatherThanADefaultReason(byte reason)
    {
        // Arrange
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        using Socket listener = Listen();
        using Socket hostile = AcceptDialFrom(dialler, listener);

        // Act
        hostile.Send(StreamSessionTestRecords.BuildRefusal(reason));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));

        // Assert
        events.Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }

    [Fact]
    public void Pump_RefusalNamingAProtocolViolation_ClosesThePeerAsProtocolViolation()
    {
        // Arrange
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        using Socket listener = Listen();
        using Socket refuser = AcceptDialFrom(dialler, listener);

        // Act
        refuser.Send(StreamSessionTestRecords.BuildRefusal(RefusalReasonProtocolViolation, value0: StreamSessionTestRecords.HandshakeRecordSize));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));

        // Assert
        events.Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    [Fact]
    public void Pump_RefusalWithNonZeroPadding_ClosesThePeerAsTransportFailedRatherThanForItsReason()
    {
        // Arrange — a well-formed capacity Refusal in every byte but the last.
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        using Socket listener = Listen();
        using Socket hostile = AcceptDialFrom(dialler, listener);
        byte[] refusal = StreamSessionTestRecords.BuildRefusal(RefusalReasonCapacity);
        refusal[^1] = 1;

        // Act
        hostile.Send(refusal);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed));

        // Assert
        events.Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }

    [Fact]
    public void Pump_ResumeWithNonZeroPadding_IsNotMatchedOntoTheHeldPeerItsSecretNames()
    {
        // Arrange — a Resume presenting the held Peer's genuine secret, well-formed in every byte but
        // the last.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        using Socket hostile = ConnectTo(accepter.Endpoint);
        byte[] resume = StreamSessionTestRecords.BuildResume(held[0].ResumeSecret);
        resume[^1] = 1;

        // Act
        hostile.Send(resume);
        PumpUntilClosedBy(accepter, hostile);

        // Assert
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.ResumeMatched);

        held[1].Counterparty.Dispose();
    }

    private static Socket ConnectTo(IPEndPoint destination)
    {
        Socket socket = new(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        socket.Connect(destination);
        return socket;
    }

    private static Socket Listen()
    {
        Socket listener = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(1);
        return listener;
    }

    /// <summary>Has <paramref name="dialler"/> dial <paramref name="listener"/> and returns the far end of that dial.</summary>
    private static Socket AcceptDialFrom(StreamSessionReactorHarness dialler, Socket listener)
    {
        IPEndPoint listenerEndpoint = listener.LocalEndPoint as IPEndPoint
            ?? throw new InvalidOperationException("A bound IPv4 listener has an IPEndPoint.");
        dialler.Reactor.Connect(listenerEndpoint);
        dialler.PumpUntil(() => listener.Poll(0, SelectMode.SelectRead));
        return listener.Accept();
    }

    /// <summary>
    /// Pumps <paramref name="reactor"/> until it closes <paramref name="counterparty"/>'s connection,
    /// returning how many bytes it wrote first and every Event it raised along the way.
    /// </summary>
    private static (int BytesAnswered, List<SessionEventKind> Events) PumpUntilClosedBy(
        StreamSessionReactorHarness reactor, Socket counterparty)
    {
        int bytesAnswered = 0;
        List<SessionEventKind> events = [];
        byte[] buffer = new byte[256];
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            reactor.Reactor.Pump();
            events.AddRange(reactor.DrainToList().Select(evt => evt.Kind));
            if (counterparty.Poll(0, SelectMode.SelectRead))
            {
                int read = counterparty.Receive(buffer);
                if (read == 0)
                {
                    return (bytesAnswered, events);
                }

                bytesAnswered += read;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The Reactor did not close the connection within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }
}
