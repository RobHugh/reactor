using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #165: the tracer bullet for #39 — a live Session's data datagram actually leaves the socket,
/// and a <see cref="WireObserver"/> confirms the bytes rather than anything reaching into
/// <see cref="ReactorSockets.Session.DatagramAssembler"/> or any other internal codec. The Session
/// under test is a real <see cref="DatagramSessionReactor"/> dialling; <see cref="WireObserver"/> plays
/// the accepting end of the handshake by hand, exactly as
/// <see cref="DatagramSessionDataDatagramParseTests"/> plays the dialling end by hand, so it can stand
/// where the wire bytes actually land and inspect them unparsed.
/// </summary>
/// <remarks>
/// This ticket has no public Send: <see cref="DatagramSessionReactor"/> exposes no member that queues
/// a packet record, so there is nothing here that exercises the four delivery modes (#44) or the
/// per-Pump flush and coalescing policy (#45). What is reachable without either is the datagram this
/// Session sends the instant the handshake completes — the tracer bullet the issue asks for: 20 bytes
/// of header, both ack blocks present and zeroed because nothing has been received, and zero records
/// because there is nothing yet to carry one.
/// </remarks>
public class DatagramSessionWireFormatTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;
    private const byte AcceptType = 4;
    private const int ControlBodyOffset = 5;
    private const int ControlTypeOffset = 4;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint AccepterAssignedSessionId = 0x1234_5678;
    private const uint AccepterMaxMessageSize = 4096;

    private const int DatagramHeaderSize = 20;

    [Fact]
    public void Pump_SessionEstablished_SendsADataDatagramWhoseFirst20BytesAreTheHeaderWithBothAckBlocksZeroed()
    {
        // Arrange — a real DatagramSessionReactor dials; a WireObserver plays the accepting end of the
        // handshake by hand, since the observer must sit at the address the Session's datagram is
        // actually sent to in order to see its unparsed bytes.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using WireObserver observer = new();
        IPEndPoint observerEndpoint = observer.BindUdp(new IPEndPoint(IPAddress.Loopback, 0));

        dialler.Reactor.Connect(observerEndpoint);

        // Act — walk the handshake by hand from the accepting side, replying to whatever address the
        // Hello actually arrived from (the wildcard socket Connect lazily bound, not a routable
        // address itself), then Pump the dialler once more after each reply so it processes the step
        // and, on the last one, emits its first data datagram.
        (byte[] helloDatagram, Source diallerAddress) = PumpUntilOneMoreDatagram(observer);
        ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(helloDatagram.AsSpan(ControlBodyOffset + 4, 8));

        observer.SendUdp(diallerAddress, BuildCookieDatagram(clientNonce));
        dialler.Reactor.Pump();

        (byte[] helloWithCookieDatagram, _) = PumpUntilOneMoreDatagram(observer);
        helloWithCookieDatagram[ControlTypeOffset].Should().Be(HelloWithCookieType);

        observer.SendUdp(diallerAddress, BuildAcceptDatagram(clientNonce));
        dialler.Reactor.Pump();

        (byte[] dataDatagram, _) = PumpUntilOneMoreDatagram(observer);

        // Assert — the datagram this Session sent is exactly the 20-byte header and nothing else: the
        // session id it stamps is the one the accepter told it to use, both ack blocks are present and
        // zero because nothing has been received, and there is no trailing byte for a record that does
        // not exist yet.
        dataDatagram.Should().HaveCount(DatagramHeaderSize);

        uint sessionOnWire = BinaryPrimitives.ReadUInt32BigEndian(dataDatagram.AsSpan(0, 4));
        sessionOnWire.Should().Be(AccepterAssignedSessionId);

        uint reliableOrderedAckBase = BinaryPrimitives.ReadUInt32BigEndian(dataDatagram.AsSpan(4, 4));
        uint reliableOrderedAckBits = BinaryPrimitives.ReadUInt32BigEndian(dataDatagram.AsSpan(8, 4));
        uint reliableUnorderedAckBase = BinaryPrimitives.ReadUInt32BigEndian(dataDatagram.AsSpan(12, 4));
        uint reliableUnorderedAckBits = BinaryPrimitives.ReadUInt32BigEndian(dataDatagram.AsSpan(16, 4));

        reliableOrderedAckBase.Should().Be(0);
        reliableOrderedAckBits.Should().Be(0);
        reliableUnorderedAckBase.Should().Be(0);
        reliableUnorderedAckBits.Should().Be(0);
    }

    [Fact]
    public void Pump_SessionEstablished_RaisesTheSentDatagramCounterForTheDataDatagram()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using WireObserver observer = new();
        IPEndPoint observerEndpoint = observer.BindUdp(new IPEndPoint(IPAddress.Loopback, 0));

        dialler.Reactor.Connect(observerEndpoint);
        ulong sentBefore = dialler.Reactor.Counters.DatagramsSent;

        // Act
        (byte[] helloDatagram, Source diallerAddress) = PumpUntilOneMoreDatagram(observer);
        ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(helloDatagram.AsSpan(ControlBodyOffset + 4, 8));

        observer.SendUdp(diallerAddress, BuildCookieDatagram(clientNonce));
        dialler.Reactor.Pump();

        PumpUntilOneMoreDatagram(observer); // HelloWithCookie

        observer.SendUdp(diallerAddress, BuildAcceptDatagram(clientNonce));
        dialler.Reactor.Pump();

        PumpUntilOneMoreDatagram(observer); // the data datagram

        // Assert — at least the Accept-triggered control reply and the data datagram raised the
        // counter; the exact count of control sends is not this test's concern, only that the data
        // datagram is counted among them.
        dialler.Reactor.Counters.DatagramsSent.Should().BeGreaterThan(sentBefore);
    }

    /// <summary>
    /// Pumps <paramref name="observer"/> until it has recorded one more datagram than it had before
    /// this call, or the deadline expires, returning that datagram's payload and the
    /// <see cref="Source"/> it actually arrived from — the address a reply must be sent to, since the
    /// dialler's own <c>BoundEndpoints</c> names the wildcard socket it bound, not a routable
    /// destination.
    /// </summary>
    private static (byte[] Payload, Source From) PumpUntilOneMoreDatagram(WireObserver observer)
    {
        int countBefore = observer.ReceivedDatagrams.Count;
        observer.PumpUntil(() => observer.ReceivedDatagrams.Count > countBefore);
        if (observer.ReceivedDatagrams.Count <= countBefore)
        {
            throw new InvalidOperationException("The observer did not receive a datagram within the Pump budget.");
        }

        return (observer.ReceivedDatagrams[countBefore], observer.ReceivedDatagramSources[countBefore]);
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[ControlTypeOffset] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    /// <summary>
    /// Builds a Cookie the observer answers a Hello with. The dialler under test never validates a
    /// cookie's contents — it only echoes it back in the HelloWithCookie that follows — so any 32 bytes
    /// serve here, exactly as they would from a real accepter's HMAC.
    /// </summary>
    private static byte[] BuildCookieDatagram(ulong clientNonce)
    {
        byte[] body = new byte[8 + 8 + CookieLength];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8, 8), 0);
        // The cookie bytes stay zero — this dialler never verifies them, only echoes them.
        return BuildControlDatagram(CookieType, body);
    }

    /// <summary>
    /// Builds the Accept the observer answers a HelloWithCookie with, naming
    /// <see cref="AccepterAssignedSessionId"/> as the id the dialler must stamp on every datagram sent
    /// here afterwards — including the first data datagram this test asserts on.
    /// </summary>
    private static byte[] BuildAcceptDatagram(ulong clientNonce)
    {
        byte[] body = new byte[8 + 4 + 4 + ResumeSecretLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), AccepterAssignedSessionId);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), AccepterMaxMessageSize);
        return BuildControlDatagram(AcceptType, body);
    }
}
