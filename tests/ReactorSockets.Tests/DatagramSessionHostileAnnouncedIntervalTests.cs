using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A send-idle interval or Linger announced in a handshake is a <c>ulong</c> off the wire,
/// and an out-of-range one used to throw out of <see cref="DatagramSessionReactor.Pump"/>, either on
/// receipt or on every later Pump once the value had survived the handshake. Every assertion is made
/// from outside the L2 seam (tests/CLAUDE.md): a <see cref="RawReactor"/> plays the hostile counterparty
/// and builds its control datagrams byte by byte from the layout ADR 0009 documents.
/// </summary>
public class DatagramSessionHostileAnnouncedIntervalTests
{
    private const int ControlDatagramSize = 256;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const uint ProtocolVersion = DatagramSessionWire.ProtocolVersion;
    private const uint CounterpartyMaxMessageSize = 4096;
    private const uint CounterpartySessionId = 0xBEEF_0003;

    private const byte HelloType = 1;
    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;
    private const byte RefusalType = 5;
    private const byte RefusalReasonVersion = 1;

    private const ulong LegalInterval = 60_000;

    /// <summary>The largest legal announcement: the largest count of milliseconds 32 bits can hold.</summary>
    private const ulong MaximumLegalInterval = uint.MaxValue;

    /// <summary>Inside the window that used to survive the handshake and then overflow every Pump.</summary>
    private const ulong SurvivesConversionOverflowsDeadline = 600_000_000_000_000;

    /// <summary>Each illegal value in each announced field, with the other field legal.</summary>
    public static TheoryData<ulong, ulong> IllegalAnnouncements => new()
    {
        { 0, LegalInterval },
        { LegalInterval, 0 },
        { MaximumLegalInterval + 1, LegalInterval },
        { LegalInterval, MaximumLegalInterval + 1 },
        { SurvivesConversionOverflowsDeadline, LegalInterval },
        { LegalInterval, SurvivesConversionOverflowsDeadline },
        { ulong.MaxValue, LegalInterval },
        { LegalInterval, ulong.MaxValue },
    };

    [Theory]
    [MemberData(nameof(IllegalAnnouncements))]
    public void Pump_HelloWithCookieAnnouncingAnIllegalInterval_DiscardsItAsMalformedAndKeepsPumping(
        ulong sendIdleIntervalMilliseconds, ulong lingerMilliseconds)
    {
        // Arrange — a genuine cookie, because the cookie is an anti-amplification token any sender can
        // obtain and so is no protection against this announcement.
        using DatagramSessionReactorHarness accepter = new();
        using RawCounterparty hostile = new();
        const ulong clientNonce = 42;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, hostile, clientNonce);

        // Act
        hostile.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, sendIdleIntervalMilliseconds: sendIdleIntervalMilliseconds, lingerMilliseconds: lingerMilliseconds));
        Action pump = () =>
        {
            accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);
            accepter.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        accepter.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Theory]
    [MemberData(nameof(IllegalAnnouncements))]
    public void Pump_AcceptAnnouncingAnIllegalInterval_DiscardsItAsMalformedAndKeepsPumping(
        ulong sendIdleIntervalMilliseconds, ulong lingerMilliseconds)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using RawCounterparty hostile = new();
        (Source dialSource, ulong clientNonce) = DialUpToAccept(dialler, hostile);

        // Act
        hostile.SendTo(dialSource, DatagramSessionWire.BuildAccept(clientNonce, CounterpartySessionId, CounterpartyMaxMessageSize, sendIdleIntervalMilliseconds, lingerMilliseconds));
        Action pump = () =>
        {
            dialler.PumpUntil(() => dialler.Reactor.Counters.UnknownSourceDatagrams > 0);
            dialler.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        dialler.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        dialler.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    /// <summary>Each illegal Fragment size or receive window (ADR 0024), with the other field legal.</summary>
    public static TheoryData<ushort, uint> IllegalFragmentTerms => new()
    {
        { 575, DatagramSessionWire.DefaultReceiveWindowFragments },
        { 65_508, DatagramSessionWire.DefaultReceiveWindowFragments },
        { DatagramSessionWire.DefaultFragmentSize, 0 },
    };

    /// <summary>The edges of the legal Fragment size range, which are accepted.</summary>
    public static TheoryData<ushort> LegalFragmentSizeBoundaries => new() { 576, 65_507 };

    [Theory]
    [MemberData(nameof(IllegalFragmentTerms))]
    public void Pump_HelloWithCookieAnnouncingAnIllegalFragmentTerm_DiscardsItAsMalformedAndKeepsPumping(
        ushort fragmentSize, uint receiveWindowFragments)
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawCounterparty hostile = new();
        const ulong clientNonce = 47;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, hostile, clientNonce);

        // Act
        hostile.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, fragmentSize: fragmentSize, receiveWindowFragments: receiveWindowFragments));
        Action pump = () =>
        {
            accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);
            accepter.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        accepter.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Theory]
    [MemberData(nameof(IllegalFragmentTerms))]
    public void Pump_AcceptAnnouncingAnIllegalFragmentTerm_DiscardsItAsMalformedAndKeepsPumping(
        ushort fragmentSize, uint receiveWindowFragments)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using RawCounterparty hostile = new();
        (Source dialSource, ulong clientNonce) = DialUpToAccept(dialler, hostile);

        // Act
        hostile.SendTo(dialSource, DatagramSessionWire.BuildAccept(clientNonce, CounterpartySessionId, CounterpartyMaxMessageSize, fragmentSize: fragmentSize, receiveWindowFragments: receiveWindowFragments));
        Action pump = () =>
        {
            dialler.PumpUntil(() => dialler.Reactor.Counters.UnknownSourceDatagrams > 0);
            dialler.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        dialler.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        dialler.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Theory]
    [MemberData(nameof(LegalFragmentSizeBoundaries))]
    public void Pump_HelloWithCookieAnnouncingALegalFragmentSizeBoundary_Establishes(ushort fragmentSize)
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawCounterparty counterparty = new();
        const ulong clientNonce = 48;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, counterparty, clientNonce);

        // Act
        counterparty.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, fragmentSize: fragmentSize));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));

        // Assert
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Theory]
    [MemberData(nameof(LegalFragmentSizeBoundaries))]
    public void Pump_AcceptAnnouncingALegalFragmentSizeBoundary_Establishes(ushort fragmentSize)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using RawCounterparty counterparty = new();
        (Source dialSource, ulong clientNonce) = DialUpToAccept(dialler, counterparty);

        // Act
        counterparty.SendTo(dialSource, DatagramSessionWire.BuildAccept(clientNonce, CounterpartySessionId, CounterpartyMaxMessageSize, fragmentSize: fragmentSize));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));

        // Assert
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_HelloWithCookieOfAnotherVersionAnnouncingAnIllegalInterval_IsStillRefusedForItsVersion()
    {
        // Arrange — this end cannot judge the fields of a version it does not speak, and the sender can
        // act on a version Refusal.
        using DatagramSessionReactorHarness accepter = new();
        using RawCounterparty counterparty = new();
        const ulong clientNonce = 46;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, counterparty, clientNonce);

        // Act
        counterparty.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, sendIdleIntervalMilliseconds: ulong.MaxValue, lingerMilliseconds: ulong.MaxValue, version: ProtocolVersion + 1));
        (_, byte[] refusal) = counterparty.PumpUntilReceived(accepter.Reactor.Pump, RefusalType);

        // Assert — clientNonce(8) comes first in a Refusal's body, then the reason.
        refusal[ControlBodyOffset + 8].Should().Be(RefusalReasonVersion);
    }

    [Fact]
    public void Pump_AfterDiscardingAnIllegalInterval_StillAdmitsAGenuineDial()
    {
        // Arrange — one Slot, so a discard that wrongly kept the Slot would refuse the genuine dial.
        using DatagramSessionReactorHarness accepter = new(capacity: 1);
        using RawCounterparty hostile = new();
        const ulong clientNonce = 43;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, hostile, clientNonce);
        hostile.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, sendIdleIntervalMilliseconds: SurvivesConversionOverflowsDeadline, lingerMilliseconds: LegalInterval));
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents, _) =
            DatagramSessionReactorHarness.PumpBothUntilDrained(
                dialler, accepter, (diallerSoFar, _) => diallerSoFar.Any(evt => evt.Kind == SessionEventKind.PeerConnected));

        // Assert
        diallerEvents.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_HelloWithCookieAnnouncingTheMaximumAgainstTheLargestHeartbeatCount_EstablishesAndKeepsPumping()
    {
        // Arrange — the largest legal announcement meets the largest count a consumer can configure, so
        // the receive-idle deadline the two imply is far outside what a TimeSpan can represent.
        using DatagramSessionReactorHarness accepter = new(missedHeartbeats: int.MaxValue);
        using RawCounterparty counterparty = new();
        const ulong clientNonce = 44;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, counterparty, clientNonce);

        // Act
        counterparty.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, sendIdleIntervalMilliseconds: MaximumLegalInterval, lingerMilliseconds: MaximumLegalInterval));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        Action pump = () =>
        {
            events = accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));
            accepter.TimeProvider.Advance(TimeSpan.FromDays(1));
            accepter.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
        accepter.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerInterrupted);
    }

    [Fact]
    public void Pump_AcceptAnnouncingTheMaximumAgainstTheLargestHeartbeatCount_EstablishesAndKeepsPumping()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, missedHeartbeats: int.MaxValue);
        using RawCounterparty counterparty = new();
        (Source dialSource, ulong clientNonce) = DialUpToAccept(dialler, counterparty);

        // Act
        counterparty.SendTo(dialSource, DatagramSessionWire.BuildAccept(clientNonce, CounterpartySessionId, CounterpartyMaxMessageSize, MaximumLegalInterval, MaximumLegalInterval));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        Action pump = () =>
        {
            events = dialler.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));
            dialler.TimeProvider.Advance(TimeSpan.FromDays(1));
            dialler.Reactor.Pump();
        };

        // Assert
        pump.Should().NotThrow();
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
        dialler.DrainToList().Should().NotContain(evt => evt.Kind == SessionEventKind.PeerInterrupted);
    }

    [Fact]
    public void Pump_SilenceReachingTheAnnouncedIntervalTimesTheHeartbeatCount_InterruptsThePeerExactlyThere()
    {
        // Arrange — 1000 ms announced and a count of 3 make a deadline of exactly three seconds, stated
        // here as a literal so the arithmetic is checked rather than repeated.
        using DatagramSessionReactorHarness accepter = new(missedHeartbeats: 3);
        using RawCounterparty counterparty = new();
        const ulong clientNonce = 45;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, counterparty, clientNonce);
        counterparty.SendTo(
            Source.FromIPEndPoint(accepter.Endpoint),
            DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: CounterpartySessionId, sendIdleIntervalMilliseconds: 1000, lingerMilliseconds: LegalInterval));
        accepter.PumpUntilDrained(drained => drained.Any(evt => evt.Kind == SessionEventKind.PeerConnected));

        // Act
        accepter.TimeProvider.Advance(TimeSpan.FromSeconds(3) - TimeSpan.FromTicks(1));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> oneTickShort = accepter.PumpAndDrainAll(1);
        accepter.TimeProvider.Advance(TimeSpan.FromTicks(1));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> atTheDeadline = accepter.PumpAndDrainAll(1);

        // Assert
        oneTickShort.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerInterrupted);
        atTheDeadline.Should().Contain(evt => evt.Kind == SessionEventKind.PeerInterrupted);
    }

    /// <summary>
    /// Sends a Hello from <paramref name="counterparty"/> and Pumps both ends until the matching Cookie
    /// comes back, returning the epoch and cookie it carried.
    /// </summary>
    private static (long Epoch, byte[] Cookie) ObtainCookie(
        DatagramSessionReactorHarness accepter, RawCounterparty counterparty, ulong clientNonce)
    {
        byte[] body = new byte[4 + 8];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        counterparty.SendTo(Source.FromIPEndPoint(accepter.Endpoint), BuildControlDatagram(HelloType, body));

        (_, byte[] cookieDatagram) = counterparty.PumpUntilReceived(accepter.Reactor.Pump, CookieType);
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        return (epoch, cookie);
    }

    /// <summary>
    /// Walks <paramref name="dialler"/> to the point where it is waiting on an Accept, playing the
    /// accepting end by hand. A dialler never validates a Cookie's contents, so these bytes need not be
    /// genuine.
    /// </summary>
    private static (Source DialSource, ulong ClientNonce) DialUpToAccept(
        DatagramSessionReactorHarness dialler, RawCounterparty counterparty)
    {
        dialler.Reactor.Connect(counterparty.Endpoint);
        (Source dialSource, byte[] hello) = counterparty.PumpUntilReceived(dialler.Reactor.Pump, HelloType);
        ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(hello.AsSpan(ControlBodyOffset + 4, 8));

        byte[] body = new byte[8 + 8 + CookieLength];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8, 8), 1);
        counterparty.SendTo(dialSource, BuildControlDatagram(CookieType, body));
        counterparty.PumpUntilReceived(dialler.Reactor.Pump, HelloWithCookieType);

        return (dialSource, clientNonce);
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    /// <summary>
    /// A counterparty built only from the public Raw surface, holding one Link open so replies come back
    /// to the address it sent from.
    /// </summary>
    private sealed class RawCounterparty : IDisposable
    {
        private readonly RawReactor _reactor = new(new ReactorOptions { Capacity = 4 });
        private readonly LinkId _link;

        public RawCounterparty()
        {
            _link = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        }

        public IPEndPoint Endpoint => _reactor.LocalOf(_link);

        public void SendTo(Source destination, ReadOnlySpan<byte> payload)
        {
            Result result = _reactor.SendTo(_link, destination, payload);
            if (!result.IsSuccess)
            {
                throw new InvalidOperationException($"The counterparty failed to send: {result.Error}");
            }
        }

        /// <summary>
        /// Pumps <paramref name="pumpReactorUnderTest"/> and this counterparty alternately until a
        /// control datagram of <paramref name="expectedType"/> arrives, returning who sent it and its
        /// bytes.
        /// </summary>
        public (Source Source, byte[] Payload) PumpUntilReceived(Action pumpReactorUnderTest, byte expectedType)
        {
            PumpDeadline deadline = PumpDeadline.Start();
            while (true)
            {
                pumpReactorUnderTest();
                _reactor.Pump();
                foreach (RawEvent evt in _reactor.Drain())
                {
                    if (evt.Kind == RawEventKind.DatagramReceived && evt.Payload.Length == ControlDatagramSize && evt.Payload[4] == expectedType)
                    {
                        return (evt.Source, evt.Payload.ToArray());
                    }
                }

                if (deadline.Expired)
                {
                    throw new InvalidOperationException($"A control datagram of type {expectedType} did not arrive within the Pump budget.");
                }

                PumpDeadline.Yield();
            }
        }

        public void Dispose() => _reactor.Dispose();
    }
}
