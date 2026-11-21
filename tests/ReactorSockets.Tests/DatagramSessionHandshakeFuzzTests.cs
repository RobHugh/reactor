using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The handshake parser is total: whatever bytes a stranger sends, no exception escapes
/// <see cref="DatagramSessionReactor.Pump"/>, either on receipt or on any later Pump that uses a
/// value the handshake accepted. A <see cref="RawReactor"/> plays the hostile counterparty on each
/// side of the handshake and sends a seeded corpus of handshake datagrams whose announced fields are
/// drawn by <see cref="HandshakeFuzzValues"/>. Random noise never carries a genuine cookie, so the
/// accept-side corpus obtains real cookies first. The cookie is an anti-amplification token any sender
/// can obtain, so it is no protection against what the Hello-with-cookie announces.
/// </summary>
public class DatagramSessionHandshakeFuzzTests
{
    private const int ControlDatagramSize = 256;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;
    private const uint ProtocolVersion = DatagramSessionWire.ProtocolVersion;

    private const byte HelloType = 1;
    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;
    private const byte AcceptType = 4;
    private const byte RefusalType = 5;

    /// <summary>How many handshakes run side by side, each under its own client nonce.</summary>
    private const int Handshakes = 32;

    /// <summary>How many fuzzed datagrams each handshake receives.</summary>
    private const int DatagramsPerHandshake = 64;

    /// <summary>How many datagrams go out between Pumps, so no receive queue overflows.</summary>
    private const int BatchSize = 50;

    /// <summary>The largest control type byte the corpus aims at directly: PathResponse.</summary>
    private const int LargestControlType = 8;

    [Fact]
    public void Pump_FuzzedHelloWithCookieCorpus_NeverThrowsAndTheReactorStaysUsable()
    {
        // Arrange. The largest heartbeat count, so an accepted interval meets the largest multiplier.
        using DatagramSessionReactorHarness accepter = new(capacity: Handshakes, missedHeartbeats: int.MaxValue);
        using RawCounterparty hostile = new();
        Random random = new(HandshakeFuzzValues.Seed);
        Source accepterSource = Source.FromIPEndPoint(accepter.Endpoint);
        Dictionary<ulong, (long Epoch, byte[] Cookie)> cookies = ObtainCookies(accepter, hostile, firstNonce: 1, count: Handshakes);
        List<byte[]> corpus = [];
        foreach ((ulong clientNonce, (long epoch, byte[] cookie)) in cookies)
        {
            for (int i = 0; i < DatagramsPerHandshake; i++)
            {
                byte[] datagram = random.Next(6) == 0
                    ? BuildArbitraryDatagram(random)
                    : BuildFuzzedHelloWithCookie(random, clientNonce, epoch, cookie);
                corpus.Add(random.Next(5) == 0 ? HandshakeFuzzValues.Mutate(random, datagram) : datagram);
            }
        }

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act
        Action fuzz = () =>
        {
            SendInBatches(
                corpus,
                datagram => hostile.SendTo(accepterSource, datagram),
                () =>
                {
                    events.AddRange(accepter.PumpAndDrainAll(1));
                    hostile.DiscardReceived();
                });
            events.AddRange(AdvanceThroughClockSteps(accepter, hostile));
        };

        // Assert. Nothing throws, and the corpus reached both the discard and the establish path.
        fuzz.Should().NotThrow();
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);

        // Assert. The Reactor still answers a Hello from a fresh source.
        using RawCounterparty stranger = new();
        Action freshHello = () => ObtainCookies(accepter, stranger, firstNonce: 1_000_000, count: 1);
        freshHello.Should().NotThrow();
    }

    [Fact]
    public void Pump_FuzzedAcceptAndRefusalCorpus_NeverThrowsAndTheReactorStaysUsable()
    {
        // Arrange. One Slot beyond the dials, kept free for the genuine dial at the end.
        using DatagramSessionReactorHarness dialler = new(capacity: Handshakes + 1, bindLoopback: false, missedHeartbeats: int.MaxValue);
        using RawCounterparty hostile = new();
        Random random = new(HandshakeFuzzValues.Seed);
        (Source dialSource, List<ulong> clientNonces) = DialUpToAccept(dialler, hostile, Handshakes);
        List<byte[]> corpus = [];
        foreach (ulong clientNonce in clientNonces)
        {
            for (int i = 0; i < DatagramsPerHandshake; i++)
            {
                byte[] reply = random.Next(8) switch
                {
                    0 => BuildArbitraryDatagram(random),
                    1 => BuildFuzzedCookie(random, clientNonce),
                    2 or 3 => BuildFuzzedRefusal(random, clientNonce),
                    _ => BuildFuzzedAccept(random, clientNonce),
                };
                corpus.Add(random.Next(5) == 0 ? HandshakeFuzzValues.Mutate(random, reply) : reply);
            }
        }

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act
        Action fuzz = () =>
        {
            SendInBatches(
                corpus,
                datagram => hostile.SendTo(dialSource, datagram),
                () =>
                {
                    events.AddRange(dialler.PumpAndDrainAll(1));
                    hostile.DiscardReceived();
                });
            events.AddRange(AdvanceThroughClockSteps(dialler, hostile));
        };

        // Assert. Nothing throws, and the corpus reached both the discard and the establish path.
        fuzz.Should().NotThrow();
        dialler.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);

        // Assert. The Reactor still completes a genuine dial.
        using DatagramSessionReactorHarness accepter = new();
        dialler.Reactor.Connect(accepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents, _) =
            DatagramSessionReactorHarness.PumpBothUntilDrained(
                dialler, accepter, (diallerSoFar, _) => diallerSoFar.Any(evt => evt.Kind == SessionEventKind.PeerConnected));
        diallerEvents.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    /// <summary>
    /// Sends every datagram in <paramref name="corpus"/>, calling <paramref name="pump"/> after every
    /// <see cref="BatchSize"/> datagrams and a few times at the end.
    /// </summary>
    private static void SendInBatches(List<byte[]> corpus, Action<byte[]> send, Action pump)
    {
        for (int i = 0; i < corpus.Count; i++)
        {
            send(corpus[i]);
            if ((i + 1) % BatchSize == 0)
            {
                pump();
            }
        }

        for (int i = 0; i < 5; i++)
        {
            pump();
        }
    }

    /// <summary>
    /// Advances the clock through <see cref="HandshakeFuzzValues.ClockSteps"/>, Pumping after each
    /// step and dropping whatever reached <paramref name="counterparty"/>.
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AdvanceThroughClockSteps(
        DatagramSessionReactorHarness harness, RawCounterparty counterparty)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        foreach (TimeSpan step in HandshakeFuzzValues.ClockSteps)
        {
            harness.TimeProvider.Advance(step);
            events.AddRange(harness.PumpAndDrainAll(2));
            counterparty.DiscardReceived();
        }

        return events;
    }

    /// <summary>
    /// Sends <paramref name="count"/> Hellos under consecutive client nonces from
    /// <paramref name="firstNonce"/> and Pumps until a Cookie for each has come back, returning each
    /// nonce's epoch and cookie.
    /// </summary>
    private static Dictionary<ulong, (long Epoch, byte[] Cookie)> ObtainCookies(
        DatagramSessionReactorHarness accepter, RawCounterparty counterparty, ulong firstNonce, int count)
    {
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);
        for (int i = 0; i < count; i++)
        {
            byte[] body = new byte[4 + 8];
            BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
            BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), firstNonce + (ulong)i);
            counterparty.SendTo(destination, BuildControlDatagram(HelloType, body));
        }

        Dictionary<ulong, (long Epoch, byte[] Cookie)> cookies = [];
        foreach ((_, byte[] cookieDatagram) in counterparty.PumpUntilReceived(accepter.Reactor.Pump, CookieType, count))
        {
            ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
            long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
            cookies[clientNonce] = (epoch, cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray());
        }

        return cookies;
    }

    /// <summary>
    /// Starts <paramref name="count"/> dials from <paramref name="dialler"/> to
    /// <paramref name="counterparty"/> and plays the accepting end by hand until each is waiting on an
    /// Accept. A dialler never validates a Cookie's contents, so these bytes need not be genuine.
    /// </summary>
    private static (Source DialSource, List<ulong> ClientNonces) DialUpToAccept(
        DatagramSessionReactorHarness dialler, RawCounterparty counterparty, int count)
    {
        for (int i = 0; i < count; i++)
        {
            dialler.Reactor.Connect(counterparty.Endpoint);
        }

        List<(Source Source, byte[] Payload)> hellos = counterparty.PumpUntilReceived(dialler.Reactor.Pump, HelloType, count);
        Source dialSource = hellos[0].Source;
        List<ulong> clientNonces = [];
        foreach ((_, byte[] hello) in hellos)
        {
            ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(hello.AsSpan(ControlBodyOffset + 4, 8));
            clientNonces.Add(clientNonce);
            byte[] body = new byte[8 + 8 + CookieLength];
            BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
            BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8, 8), 1);
            counterparty.SendTo(dialSource, BuildControlDatagram(CookieType, body));
        }

        counterparty.PumpUntilReceived(dialler.Reactor.Pump, HelloWithCookieType, count);
        return (dialSource, clientNonces);
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    /// <summary>
    /// A Hello-with-cookie carrying a genuine nonce, epoch and cookie, and a fuzzed version, session
    /// id, limits and resume secret.
    /// </summary>
    private static byte[] BuildFuzzedHelloWithCookie(Random random, ulong clientNonce, long epoch, ReadOnlySpan<byte> cookie)
    {
        uint version = HandshakeFuzzValues.NextVersion(random, DatagramSessionWire.ProtocolVersion);
        uint sessionIdForMe = HandshakeFuzzValues.NextUInt32(random);
        uint maxMessageSize = HandshakeFuzzValues.NextMaxMessageSize(random);
        ulong sendIdleIntervalMilliseconds = HandshakeFuzzValues.NextInterval(random);
        ulong lingerMilliseconds = HandshakeFuzzValues.NextInterval(random);
        byte[] resumeSecret = new byte[ResumeSecretLength];
        random.NextBytes(resumeSecret);
        byte[] body = new byte[DatagramSessionWire.HelloWithCookieBodyLength];
        DatagramSessionWire.WriteHelloWithCookieBody(
            body,
            version,
            clientNonce,
            epoch,
            cookie,
            sessionIdForMe,
            maxMessageSize,
            sendIdleIntervalMilliseconds,
            lingerMilliseconds,
            resumeSecret);
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>An Accept carrying the dial's own nonce and a fuzzed session id, limits and resume secret.</summary>
    private static byte[] BuildFuzzedAccept(Random random, ulong clientNonce)
    {
        byte[] body = new byte[DatagramSessionWire.AcceptBodyLength];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        WriteFuzzedTerms(random, body.AsSpan(8));
        return BuildControlDatagram(AcceptType, body);
    }

    /// <summary>
    /// Noise of a random length, or a control datagram of any type up to PathResponse (or an
    /// unlisted one) whose body is random bytes of a random length followed by zero padding.
    /// </summary>
    private static byte[] BuildArbitraryDatagram(Random random)
    {
        if (random.Next(3) == 0)
        {
            return HandshakeFuzzValues.NextBytes(random, random.Next(0, 600));
        }

        byte controlType = random.Next(4) == 0 ? (byte)random.Next(256) : (byte)random.Next(1, LargestControlType + 1);
        return BuildControlDatagram(controlType, HandshakeFuzzValues.NextBytes(random, random.Next(0, ControlDatagramSize - ControlBodyOffset + 1)));
    }

    /// <summary>A Cookie carrying the dial's own nonce and a fuzzed epoch and cookie.</summary>
    private static byte[] BuildFuzzedCookie(Random random, ulong clientNonce)
    {
        byte[] body = new byte[8 + 8 + CookieLength];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8, 8), HandshakeFuzzValues.NextUInt64(random));
        random.NextBytes(body.AsSpan(16, CookieLength));
        return BuildControlDatagram(CookieType, body);
    }

    /// <summary>A Refusal carrying the dial's own nonce and a fuzzed reason and version pair.</summary>
    private static byte[] BuildFuzzedRefusal(Random random, ulong clientNonce)
    {
        byte[] body = new byte[8 + 1 + 4 + 4];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        body[8] = HandshakeFuzzValues.NextRefusalReason(random);
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(9, 4), HandshakeFuzzValues.NextVersion(random, DatagramSessionWire.ProtocolVersion));
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(13, 4), HandshakeFuzzValues.NextVersion(random, DatagramSessionWire.ProtocolVersion));
        return BuildControlDatagram(RefusalType, body);
    }

    /// <summary>
    /// Writes the terms a Hello-with-cookie and an Accept share: session id, MaxMessageSize, send-idle
    /// interval, Linger, FragmentSize and ReceiveWindowFragments, then the resume secret.
    /// </summary>
    private static void WriteFuzzedTerms(Random random, Span<byte> destination)
    {
        int offset = DatagramSessionWire.WriteTerms(
            destination,
            0,
            HandshakeFuzzValues.NextUInt32(random),
            HandshakeFuzzValues.NextMaxMessageSize(random),
            HandshakeFuzzValues.NextInterval(random),
            HandshakeFuzzValues.NextInterval(random),
            HandshakeFuzzValues.NextFragmentSize(random),
            HandshakeFuzzValues.NextUInt32(random));
        random.NextBytes(destination.Slice(offset, ResumeSecretLength));
    }

    /// <summary>
    /// A counterparty built only from the public Raw surface, holding one Link open so replies come back
    /// to the address it sent from.
    /// </summary>
    private sealed class RawCounterparty : IDisposable
    {
        private readonly RawReactor _reactor = new(new ReactorOptions { Capacity = 4 });
        private readonly LinkId _link;

        /// <summary>Binds the counterparty's one Link on loopback at an ephemeral port.</summary>
        public RawCounterparty()
        {
            _link = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        }

        /// <summary>The address the counterparty sends from and receives at.</summary>
        public IPEndPoint Endpoint => _reactor.LocalOf(_link);

        /// <summary>Sends <paramref name="payload"/> to <paramref name="destination"/>, throwing if the send fails.</summary>
        public void SendTo(Source destination, ReadOnlySpan<byte> payload)
        {
            Result result = _reactor.SendTo(_link, destination, payload);
            if (!result.IsSuccess)
            {
                throw new InvalidOperationException($"The counterparty failed to send: {result.Error}");
            }
        }

        /// <summary>
        /// Pumps this counterparty and drops whatever arrived, so the replies to a corpus never fill
        /// its receive queue.
        /// </summary>
        public void DiscardReceived()
        {
            _reactor.Pump();
            foreach (RawEvent _ in _reactor.Drain())
            {
            }
        }

        /// <summary>
        /// Pumps <paramref name="pumpReactorUnderTest"/> and this counterparty alternately until
        /// <paramref name="count"/> control datagrams of <paramref name="expectedType"/> have arrived,
        /// returning who sent each and its bytes.
        /// </summary>
        public List<(Source Source, byte[] Payload)> PumpUntilReceived(Action pumpReactorUnderTest, byte expectedType, int count)
        {
            List<(Source Source, byte[] Payload)> received = [];
            PumpDeadline deadline = PumpDeadline.Start();
            while (received.Count < count)
            {
                pumpReactorUnderTest();
                _reactor.Pump();
                foreach (RawEvent evt in _reactor.Drain())
                {
                    if (evt.Kind == RawEventKind.DatagramReceived && evt.Payload.Length == ControlDatagramSize && evt.Payload[4] == expectedType)
                    {
                        received.Add((evt.Source, evt.Payload.ToArray()));
                    }
                }

                if (deadline.Expired)
                {
                    throw new InvalidOperationException(
                        $"{count} control datagrams of type {expectedType} did not arrive within the Pump budget. {received.Count} did.");
                }

                PumpDeadline.Yield();
            }

            return received;
        }

        /// <summary>Disposes the counterparty's Raw Reactor.</summary>
        public void Dispose() => _reactor.Dispose();
    }
}
