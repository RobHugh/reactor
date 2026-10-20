using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 5.2 (issue #33): the control-datagram branch and the malformed-input surface. Every test
/// here sends bytes a remote can produce at will — never a well-formed protocol exchange — from a
/// <see cref="HostilePeer"/> to a bound <see cref="DatagramSessionReactor"/>, and asserts the bytes
/// land on the Diagnostic Sink as <see cref="DiagnosticId.DatagramMalformed"/> and never on the event
/// stream. See the wire format fixed in the parent slice spec: a 256-byte, big-endian, zero-padded
/// control datagram — session id, control type, body, padding.
/// </summary>
public class DatagramSessionMalformedInputTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte UnknownType = 200;

    private const int HelloBodyLength = 12;

    private const uint ProtocolVersion = 1;

    /// <summary>
    /// Builds a 256-byte control datagram: the four-byte session id, the one-byte control type, the
    /// given body, and zero padding out to <see cref="ControlDatagramSize"/> — so tests read as
    /// statements about a control type and a body rather than raw byte arithmetic.
    /// </summary>
    private static byte[] BuildControlDatagram(uint sessionId, byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, sessionId);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(5));
        return datagram;
    }

    /// <summary>A well-formed Hello body: protocol version 1 and an arbitrary client nonce.</summary>
    private static byte[] BuildHelloBody(ulong clientNonce = 0x0123_4567_89AB_CDEFUL)
    {
        byte[] body = new byte[HelloBodyLength];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return body;
    }

    /// <summary>A well-formed, exactly-256-byte Hello control datagram.</summary>
    private static byte[] BuildWellFormedHello() => BuildControlDatagram(ControlSession, HelloType, BuildHelloBody());

    [Fact]
    public void Pump_ZeroLengthDatagram_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();

        // Act
        Action act = () => peer.SendDatagram(accepter.Endpoint, []);
        act.Should().NotThrow();
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: 0);
    }

    [Fact]
    public void Pump_OneByteDatagram_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();

        // Act
        peer.SendDatagram(accepter.Endpoint, [0xFF]);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: 1);
    }

    [Fact]
    public void Pump_ThreeByteDatagram_ShorterThanTheSessionIdItself_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();

        // Act
        peer.SendDatagram(accepter.Endpoint, [0x01, 0x02, 0x03]);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: 3);
    }

    [Fact]
    public void Pump_256ZeroBytes_ControlType0IsOffTheRoster_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] datagram = new byte[ControlDatagramSize];

        // Act
        peer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: ControlDatagramSize);
    }

    [Fact]
    public void Pump_256BytesOfRandomNoise_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] datagram = new byte[ControlDatagramSize];
        new Random(12345).NextBytes(datagram);

        // Act
        peer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: ControlDatagramSize);
    }

    [Fact]
    public void Pump_WellFormedHelloTruncatedToFewerThan256Bytes_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] truncated = BuildWellFormedHello()[..200];

        // Act
        peer.SendDatagram(accepter.Endpoint, truncated);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: 200);
    }

    [Fact]
    public void Pump_WellFormedHelloPaddedToMoreThan256Bytes_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] hello = BuildWellFormedHello();
        byte[] oversized = new byte[hello.Length + 8];
        hello.CopyTo(oversized, 0);

        // Act
        peer.SendDatagram(accepter.Endpoint, oversized);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: oversized.Length);
    }

    [Fact]
    public void Pump_UnknownControlType_IsDiscardedWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] datagram = BuildControlDatagram(ControlSession, UnknownType, BuildHelloBody());

        // Act
        peer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: ControlDatagramSize);
    }

    [Fact]
    public void Pump_WellFormedHelloWithOneNonZeroPaddingByte_IsDiscardedNotAccepted()
    {
        // Arrange — a Hello whose body is otherwise entirely valid, but the zero padding that must
        // fill the datagram out to 256 bytes carries one non-zero byte: unknown flag bits / trailing
        // garbage is a protocol violation, not something to accept and ignore.
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] datagram = BuildWellFormedHello();
        datagram[^1] = 0x01;

        // Act
        peer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: ControlDatagramSize);
    }

    [Fact]
    public void Pump_DatagramNamingAnUnknownNonZeroSessionId_IsDiscardedAndNoSessionIsEverCreated()
    {
        // Arrange — a session id that names no Session at all: a data datagram cannot be an
        // introduction, only the handshake creates a Session.
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] datagram = BuildControlDatagram(0xDEAD_BEEF, HelloType, BuildHelloBody());

        // Act
        peer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> further = accepter.PumpAndDrainAll(5);

        // Assert — the drain immediately after the discard, and every further Pump's drain, are both
        // empty: no PeerConnected ever appears for a session id nothing introduced.
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: ControlDatagramSize);
        further.Should().BeEmpty();
    }

    [Fact]
    public void Pump_LargeMixedBatchOfMalformedShapes_NeverReachesTheEventStreamOnlyTheSink()
    {
        // Arrange — anything a remote can generate at will goes to the Sink, never the event stream.
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();

        List<byte[]> batch =
        [
            [],
            [0xAA],
            [0x01, 0x02, 0x03],
            new byte[ControlDatagramSize],
            BuildDatagramOfRandomNoise(new Random(999)),
            BuildWellFormedHello()[..150],
            [.. BuildWellFormedHello(), 0x00, 0x00, 0x00, 0x00],
            BuildControlDatagram(ControlSession, UnknownType, BuildHelloBody()),
            WithOneNonZeroPaddingByte(BuildWellFormedHello()),
            BuildControlDatagram(0x1234_5678, HelloType, BuildHelloBody()),
        ];

        // Act
        foreach (byte[] datagram in batch)
        {
            peer.SendDatagram(accepter.Endpoint, datagram);
        }

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(
                _ => accepter.Sink.Received.Count(r => r.Id == DiagnosticId.DatagramMalformed) >= batch.Count);

        // Assert
        drained.Should().BeEmpty();
        accepter.Sink.Received.Count(record => record.Id == DiagnosticId.DatagramMalformed)
            .Should().Be(batch.Count);
    }

    [Fact]
    public void Pump_2000RandomDatagrams_NeverThrowsNeverProducesASessionEventAndTheReactorStaysUsable()
    {
        // Arrange — a fixed number of deterministic iterations: seeded with the constant 20260806 so
        // this test is reproducible across runs and machines.
        const int iterations = 2000;
        const int batchSize = 100;
        Random random = new(20260806);
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allDrained = [];

        // Act
        Action fuzz = () =>
        {
            for (int i = 0; i < iterations; i++)
            {
                int length = random.Next(0, 512);
                byte[] datagram = new byte[length];
                random.NextBytes(datagram);
                peer.SendDatagram(accepter.Endpoint, datagram);

                if ((i + 1) % batchSize == 0)
                {
                    allDrained.AddRange(accepter.PumpAndDrainAll(1));
                }
            }

            allDrained.AddRange(accepter.PumpAndDrainAll(3));
        };

        // Assert
        fuzz.Should().NotThrow();
        allDrained.Should().BeEmpty();

        // The Reactor is still usable afterwards: a legitimate handshake still completes.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        PeerId dialled = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(
            dialler, accepter);

        diallerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
        accepterEvents.Should().ContainSingle()
            .Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_WellFormed256ByteHello_IsAcceptedAndAnsweredWithACookie()
    {
        // Arrange — the positive control: proves the negative tests above are testing the guard
        // rather than a blanket rejection of every datagram.
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer peer = new();
        byte[] datagram = BuildWellFormedHello();
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;

        // Act
        peer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);

        // Assert
        accepter.Reactor.Counters.DatagramsSent.Should().BeGreaterThan(sentBefore);
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    private static byte[] BuildDatagramOfRandomNoise(Random random)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        random.NextBytes(datagram);
        return datagram;
    }

    private static byte[] WithOneNonZeroPaddingByte(byte[] wellFormedDatagram)
    {
        byte[] copy = [.. wellFormedDatagram];
        copy[^1] = 0x7F;
        return copy;
    }

    /// <summary>
    /// The shared assertion every malformed-shape test makes: exactly one
    /// <see cref="DiagnosticId.DatagramMalformed"/> record reached the Sink, naming the claimed
    /// loopback Source, carrying the bytes received; the drain is empty; and the discard counter rose
    /// by exactly one. Filters by <see cref="DiagnosticId"/> rather than asserting the Sink's whole
    /// contents, per tests/CLAUDE.md and the platform note on <see cref="DiagnosticId.DestinationUnreachable"/>.
    /// </summary>
    private static void AssertOneMalformedDiscard(DatagramSessionReactorHarness accepter, int expectedBytesReceived)
    {
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.Counters.UnknownSourceDatagrams.Should().Be(1);

        DiagnosticRecord record = accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed)
            .Which;

        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.ToIPEndPoint().Address.Should().Be(IPAddress.Loopback);
        record.Value0.Should().Be(expectedBytesReceived);
    }
}
