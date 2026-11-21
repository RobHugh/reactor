using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Accept carries the accepting end's Linger beside its send-idle interval. Every assertion here is made from outside the L2 seam
/// (tests/CLAUDE.md): a real <see cref="DatagramSessionReactorHarness"/> plays the dialling end, and a
/// hand-built <see cref="RawReactor"/> plays a counterparty that answers with control datagrams built
/// byte by byte from <c>ControlDatagramCodec</c>'s documented layout (read-only from this file, per
/// tests/CLAUDE.md) — never by calling the internal codec itself.
/// </summary>
public class DatagramSessionAcceptLingerTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;

    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint AccepterSessionId = 0xBEEF_0002;
    private const uint AccepterMaxMessageSize = 4096;
    private const ulong AccepterSendIdleIntervalMilliseconds = 60_000;

    [Fact]
    public void OnAccept_AnnouncingAZeroLinger_IsDiscardedAsMalformedAndNoPeerConnected()
    {
        // Arrange — a real dialler talking to a fake accepter this test drives entirely by hand, so
        // the Accept it receives can announce whatever this test chooses, including the zero Linger
        // TryReadAccept must refuse exactly as it already refuses a zero maxMessageSize or a zero
        // send-idle interval. The dialler never validates a Cookie's contents (only the real accepting
        // end does), so this fake accepter's cookie bytes need not be genuine.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using RawReactor fakeAccepter = new(new ReactorOptions { Capacity = 4 });
        LinkId fakeAccepterLink = fakeAccepter.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        IPEndPoint fakeAccepterEndpoint = fakeAccepter.LocalOf(fakeAccepterLink);

        dialler.Reactor.Connect(fakeAccepterEndpoint);

        (Source dialSource, ulong clientNonce) = PumpBothUntilReceived(
            dialler, fakeAccepter, HelloType, out _);

        byte[] cookie = new byte[CookieLength];
        const long epoch = 1;
        fakeAccepter.SendTo(fakeAccepterLink, dialSource, BuildCookie(clientNonce, epoch, cookie));

        PumpBothUntilReceived(dialler, fakeAccepter, HelloWithCookieType, out _);

        byte[] zeroLingerAccept = DatagramSessionWire.BuildAccept(
            clientNonce,
            AccepterSessionId,
            AccepterMaxMessageSize,
            AccepterSendIdleIntervalMilliseconds,
            lingerMilliseconds: 0,
            resumeSecret: new byte[ResumeSecretLength]);
        fakeAccepter.SendTo(fakeAccepterLink, dialSource, zeroLingerAccept);

        // Act
        dialler.PumpUntil(() => dialler.Reactor.Counters.UnknownSourceDatagrams > 0);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.DrainToList();

        // Assert — discarded as malformed, the same outcome a zero maxMessageSize or a zero send-idle
        // interval already produces, and no Session ever came to exist over it.
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        dialler.Reactor.Counters.UnknownSourceDatagrams.Should().Be(1);
    }

    /// <summary>
    /// Pumps <paramref name="dialler"/> and <paramref name="fakeAccepter"/> alternately until
    /// <paramref name="fakeAccepter"/> receives a control datagram of <paramref name="expectedType"/>,
    /// returning who sent it, its echoed client nonce (read from the position every Hello and
    /// Hello-with-cookie shares), and the full payload.
    /// </summary>
    private static (Source Source, ulong ClientNonce) PumpBothUntilReceived(
        DatagramSessionReactorHarness dialler, RawReactor fakeAccepter, byte expectedType, out byte[] payload)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            dialler.Reactor.Pump();
            fakeAccepter.Pump();
            foreach (RawEvent evt in fakeAccepter.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived
                    && evt.Payload.Length >= ControlBodyOffset + 4 + 8
                    && evt.Payload[4] == expectedType)
                {
                    ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(evt.Payload[(ControlBodyOffset + 4)..]);
                    payload = evt.Payload.ToArray();
                    return (evt.Source, clientNonce);
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException($"A control datagram of type {expectedType} did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    private static byte[] BuildCookie(ulong clientNonce, long epoch, ReadOnlySpan<byte> cookie)
    {
        byte[] body = new byte[8 + 8 + CookieLength];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8, 8), unchecked((ulong)epoch));
        cookie.CopyTo(body.AsSpan(16, CookieLength));
        return BuildControlDatagram(CookieType, body);
    }
}
