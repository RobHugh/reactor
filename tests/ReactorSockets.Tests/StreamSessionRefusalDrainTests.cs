using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The drain a capacity refusal runs before it closes reads a bounded number of bytes (issue #455). A
/// conforming dialler sends nothing behind its first record until it is answered, so the bound is one
/// further handshake record. A sender past it is refused with a protocol-violation Refusal naming the
/// bound rather than a capacity one, and a sender still writing cannot hold a Pump open.
/// </summary>
public class StreamSessionRefusalDrainTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;
    private const int DrainLimitBytes = RecordSize;
    private const byte RefusalType = StreamSessionTestRecords.RefusalRecordType;
    private const byte RefusalReasonCapacity = 2;
    private const byte RefusalReasonProtocolViolation = 3;

    /// <summary>
    /// How long the flooding sender keeps writing at most. A Pump with no read limit lasts this long,
    /// so the assertion below takes half of it as the line between bounded and not.
    /// </summary>
    private static readonly TimeSpan _floodDuration = TimeSpan.FromSeconds(10);

    [Fact]
    public void Pump_ARefusedSenderStillWritingDuringThePump_ReturnsBeforeTheSenderStops()
    {
        // Arrange — the only Slot is occupied, so the flooder's Hello is refused for capacity.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        using Socket flooder = new(accepter.Endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        flooder.Connect(accepter.Endpoint);
        flooder.Send(StreamSessionTestRecords.BuildHello());
        Thread writer = new(() => Flood(flooder)) { IsBackground = true };
        writer.Start();
        Thread.Sleep(50);

        // Act — time every Pump until the refusal has happened.
        TimeSpan longestPump = TimeSpan.Zero;
        PumpDeadline deadline = PumpDeadline.Start(_floodDuration * 2);
        while (!accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused) && !deadline.Expired)
        {
            Stopwatch pump = Stopwatch.StartNew();
            accepter.Reactor.Pump();
            longestPump = TimeSpan.FromTicks(Math.Max(longestPump.Ticks, pump.Elapsed.Ticks));
        }

        flooder.Dispose();
        writer.Join(_floodDuration * 2);

        // Assert
        accepter.Sink.Received.Should().Contain(r => r.Id == DiagnosticId.PeerRefused);
        longestPump.Should().BeLessThan(_floodDuration / 2);

        held.ForEach(peer => peer.Counterparty.Dispose());
    }

    [Fact]
    public void Pump_ARefusedSenderOneBytePastTheDrainLimit_IsRefusedForAProtocolViolationNamingTheLimit()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        using StreamSessionReactorHarness.RawStreamPeer sender = new(accepter.Endpoint);

        // Act
        sender.Send([.. StreamSessionTestRecords.BuildHello(), .. new byte[DrainLimitBytes + 1]]);
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));
        byte[] reply = sender.Receive(RecordSize);

        // Assert
        reply[0].Should().Be(RefusalType);
        reply[1].Should().Be(RefusalReasonProtocolViolation);
        BinaryPrimitives.ReadUInt32BigEndian(reply.AsSpan(2, 4)).Should().Be(DrainLimitBytes);

        held.ForEach(peer => peer.Counterparty.Dispose());
    }

    [Fact]
    public void Pump_ARefusedSenderPastTheDrainLimitWithNothingLeftUnread_IsClosedCleanlyRatherThanReset()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        using StreamSessionReactorHarness.RawStreamPeer sender = new(accepter.Endpoint);
        sender.Send([.. StreamSessionTestRecords.BuildHello(), .. new byte[DrainLimitBytes + 1]]);
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));
        sender.Receive(RecordSize);

        // Act
        Action act = sender.ExpectGracefulClose;

        // Assert
        act.Should().NotThrow<SocketException>();

        held.ForEach(peer => peer.Counterparty.Dispose());
    }

    [Fact]
    public void Pump_ARefusedSenderExactlyAtTheDrainLimit_IsStillRefusedForCapacity()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        using StreamSessionReactorHarness.RawStreamPeer sender = new(accepter.Endpoint);

        // Act
        sender.Send([.. StreamSessionTestRecords.BuildHello(), .. new byte[DrainLimitBytes]]);
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));
        byte[] reply = sender.Receive(RecordSize);

        // Assert
        reply[0].Should().Be(RefusalType);
        reply[1].Should().Be(RefusalReasonCapacity);

        held.ForEach(peer => peer.Counterparty.Dispose());
    }

    /// <summary>
    /// Writes as fast as the socket accepts until <see cref="_floodDuration"/> passes or the
    /// connection fails, which is how it ends once the Reactor closes it.
    /// </summary>
    private static void Flood(Socket socket)
    {
        byte[] chunk = new byte[64 * 1024];
        Stopwatch elapsed = Stopwatch.StartNew();
        try
        {
            while (elapsed.Elapsed < _floodDuration)
            {
                socket.Send(chunk);
            }
        }
        catch (SocketException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
