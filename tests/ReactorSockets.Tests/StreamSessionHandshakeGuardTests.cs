using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.3 (issue #19): the stream handshake's version and capacity guards, Linger negotiation, the
/// admission cap, and robustness against a hostile counterparty. Mirrors
/// <see cref="DatagramSessionRefusalTests"/>, the house pattern for this shape of test — a hand-built
/// raw TCP peer plays the role a well-behaved <see cref="StreamSessionReactor"/> could never take,
/// and <see cref="HostilePeer"/> plays the role a well-behaved consumer of the public Raw surface
/// could.
/// </summary>
public class StreamSessionHandshakeGuardTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;
    private const byte HelloType = StreamSessionTestRecords.HelloRecordType;
    private const byte RefusalType = 2;
    private const byte RefusalReasonVersion = 1;
    private const byte RefusalReasonCapacity = 2;
    private const uint ProtocolVersion = StreamSessionTestRecords.ProtocolVersion;

    // ---- wire helpers: parse ----

    private static (byte Reason, uint Value0, uint Value1) ParseRefusal(byte[] record)
    {
        byte reason = record[1];
        uint value0 = BinaryPrimitives.ReadUInt32BigEndian(record.AsSpan(2, 4));
        uint value1 = BinaryPrimitives.ReadUInt32BigEndian(record.AsSpan(6, 4));
        return (reason, value0, value1);
    }

    // =========================================================================================
    // Version refusal
    // =========================================================================================

    [Fact]
    public void Connect_OfferingAVersionTheReactorDoesNotSpeak_IsRefusedNamingBothVersions()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using RawTcpPeer peer = new(accepter.Endpoint);

        // Act — the accepter also sends its own unconditional Hello the instant it accepts, before it
        // has read enough of the peer's to know the version is wrong; the Refusal follows once it does.
        peer.Send(StreamSessionTestRecords.BuildHello(version: 2));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));
        byte[] reply = peer.ReceiveNextNonHelloRecord();

        // Assert
        reply[0].Should().Be(RefusalType);
        (byte reason, uint offered, uint required) = ParseRefusal(reply);
        reason.Should().Be(RefusalReasonVersion);
        offered.Should().Be(2);
        required.Should().Be(ProtocolVersion);
    }

    [Fact]
    public void Connect_OfferingAVersionTheReactorDoesNotSpeak_IsAFramedRefusalNotABareClose()
    {
        // Arrange — a bare close would be a zero-length read (EndOfStream); this must instead deliver
        // a full RecordSize-byte Refusal before the connection ends.
        using StreamSessionReactorHarness accepter = new();
        using RawTcpPeer peer = new(accepter.Endpoint);

        // Act
        peer.Send(StreamSessionTestRecords.BuildHello(version: 2));
        Action act = () => peer.ReceiveNextNonHelloRecord();
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Connect_OfferingAVersionTheReactorDoesNotSpeak_DrainsPeerClosedWithVersionMismatch()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using RawTcpPeer peer = new(accepter.Endpoint);

        // Act
        peer.Send(StreamSessionTestRecords.BuildHello(version: 2));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            accepter.PumpUntilDrained(e => e.Any(x => x.Kind == SessionEventKind.PeerClosed));

        // Assert
        events.Should().ContainSingle()
            .Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>(
                e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.VersionMismatch);
    }

    [Fact]
    public void Connect_OfferingAVersionTheReactorDoesNotSpeak_LogsPeerRefusedAtInfoWithTheSourceSubject()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using RawTcpPeer peer = new(accepter.Endpoint);

        // Act
        peer.Send(StreamSessionTestRecords.BuildHello(version: 7));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));

        // Assert — a version mismatch is a routine, expected outcome of running this profile, not a
        // condition a consumer needs to act on.
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.PeerRefused).Which;
        record.Level.Should().Be(DiagnosticLevel.Info);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
    }

    // =========================================================================================
    // A Refusal never overwrites or reorders with a still-draining Hello (#98)
    // =========================================================================================

    [Fact]
    public void Connect_ManySimultaneousVersionMismatchesWithShrunkReceiveBuffers_EachGetsExactlyOneWellFormedRefusalAndCleanClose()
    {
        // Arrange — every peer offers a version this Reactor does not speak, and every one shrinks its
        // own receive buffer before connecting (mirrors CreateCounterpartyListener's doc: this has to
        // happen before the handshake to actually clamp the negotiated window). This end's own Hello
        // is sent unconditionally the instant each connection is accepted, before anything of the
        // peer's has been read — a short send of that Hello racing the Refusal that follows once the
        // bad version is discovered (#98) is "improbable in practice, not impossible" per the issue,
        // so this admits every peer in a single Frame to give that race as many chances as one Pump
        // can offer, rather than pinning any one connection as the one that must reproduce it.
        const int peerCount = 30;
        using StreamSessionReactorHarness accepter = new(capacity: peerCount, admissionCap: peerCount);
        List<RawTcpPeer> peers = [.. Enumerable.Range(0, peerCount).Select(_ => new RawTcpPeer(accepter.Endpoint, receiveBufferSize: 1))];
        try
        {
            // Act
            foreach (RawTcpPeer peer in peers)
            {
                peer.Send(StreamSessionTestRecords.BuildHello(version: 2));
            }

            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
                accepter.PumpUntilDrained(e => e.Count(x => x.Kind == SessionEventKind.PeerClosed) >= peerCount);

            // Assert — every connection sees exactly one clean version-mismatch close, and the Refusal
            // each received off the wire is byte-correct: a short-sent Hello's remainder was never
            // silently overwritten or interleaved with the Refusal that followed it, and no connection
            // was double-processed into more than one close.
            events.Should().HaveCount(peerCount);
            events.Should().OnlyContain(e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.VersionMismatch);

            foreach (RawTcpPeer peer in peers)
            {
                byte[] reply = peer.ReceiveNextNonHelloRecord();
                reply[0].Should().Be(RefusalType);
                (byte reason, uint offered, uint required) = ParseRefusal(reply);
                reason.Should().Be(RefusalReasonVersion);
                offered.Should().Be(2);
                required.Should().Be(ProtocolVersion);
            }
        }
        finally
        {
            foreach (RawTcpPeer peer in peers)
            {
                peer.Dispose();
            }
        }
    }

    // =========================================================================================
    // Capacity refusal
    // =========================================================================================

    [Fact]
    public void Connect_EveryCapacitySlotOccupied_IsRefusedWithReasonCapacityAndClosedCleanlyRatherThanReset()
    {
        // Arrange — occupy every capacity Slot with genuine established Peers first (issue #138).
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — refused with a capacity reason naming this Reactor's capacity.
        outcome.Admitted.Should().BeFalse();
        outcome.RefusalReason.Should().Be(RefusalReasonCapacity);
        outcome.RefusalCapacity.Should().Be((uint)accepter.Capacity);

        // Assert — closed cleanly: a further read reports a graceful end-of-stream, never a reset.
        Action act = () => outcome.Counterparty.ExpectGracefulClose();
        act.Should().NotThrow<SocketException>();

        foreach (StreamSessionReactorHarness.HeldPeer heldPeer in held)
        {
            heldPeer.Counterparty.Dispose();
        }
    }

    [Fact]
    public void Connect_EveryCapacitySlotOccupied_TouchesNeitherAdmissionCounter()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using StreamSessionReactorHarness firstDialler = new(bindLoopback: false);
        firstDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(firstDialler, accepter, () => accepter.DrainToList().Count > 0);
        accepter.DrainToList();
        ulong deferralsBefore = accepter.Reactor.Counters.AdmissionDeferrals;
        ulong discardsBefore = accepter.Reactor.Counters.AdmissionDiscards;

        // Act — a refusal says something on the wire and ends the connection; it must not be counted
        // as a Deferral (silent, waits for a later Pump) or a Discard (this profile never produces one).
        using RawTcpPeer peer = new(accepter.Endpoint);
        peer.Send(StreamSessionTestRecords.BuildHello(ProtocolVersion));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));

        // Assert
        accepter.Reactor.Counters.AdmissionDeferrals.Should().Be(deferralsBefore);
        accepter.Reactor.Counters.AdmissionDiscards.Should().Be(discardsBefore);
    }

    [Fact]
    public void Connect_EveryCapacitySlotOccupied_ProducesNoSessionEventAndLeavesTheExistingSessionUnaffected()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using StreamSessionReactorHarness firstDialler = new(bindLoopback: false);
        firstDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(firstDialler, accepter, () => accepter.DrainToList().Count > 0);
        accepter.DrainToList();

        // Act
        using RawTcpPeer peer = new(accepter.Endpoint);
        peer.Send(StreamSessionTestRecords.BuildHello(ProtocolVersion));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        accepter.PumpAndDrainAll(3).Should().BeEmpty();
    }

    // =========================================================================================
    // Linger negotiation
    // =========================================================================================

    [Fact]
    public void Connect_WithDifferentLingerValues_BothEndsComputeTheSameEffectiveLingerAndEmitLingerNegotiated()
    {
        // Arrange — two Reactors with deliberately different Linger values, each with its own sink.
        RecordingDiagnosticsSink accepterSink = new();
        RecordingDiagnosticsSink diallerSink = new();
        using StreamSessionReactor accepter = new(new StreamSessionOptions
        {
            Reactor = new ReactorOptions { Capacity = 4, Sink = accepterSink, MinimumLevel = DiagnosticLevel.Trace },
            MaxMessageSize = 4096,
            RetentionBytes = 4100,
            SendIdleInterval = TimeSpan.FromSeconds(1),
            Linger = TimeSpan.FromSeconds(10),
            Endpoints = [new IPEndPoint(IPAddress.Loopback, 0)],
            Entropy = System.Security.Cryptography.RandomNumberGenerator.Create(),
        });
        using StreamSessionReactor dialler = new(new StreamSessionOptions
        {
            Reactor = new ReactorOptions { Capacity = 4, Sink = diallerSink, MinimumLevel = DiagnosticLevel.Trace },
            MaxMessageSize = 4096,
            RetentionBytes = 4100,
            SendIdleInterval = TimeSpan.FromSeconds(1),
            Linger = TimeSpan.FromSeconds(2),
            Endpoints = [],
            Entropy = System.Security.Cryptography.RandomNumberGenerator.Create(),
        });

        // Act
        dialler.Connect(accepter.BoundEndpoints[0]);
        for (int round = 0; round < 10; round++)
        {
            dialler.Pump();
            accepter.Pump();
        }

        // Assert — each end names its own Linger and the same negotiated minimum.
        DiagnosticRecord accepterRecord = accepterSink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.LingerNegotiated).Which;
        accepterRecord.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        accepterRecord.Value0.Should().Be(10_000); // the accepter's own Linger
        accepterRecord.Value1.Should().Be(2_000); // the minimum of the two

        DiagnosticRecord diallerRecord = diallerSink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.LingerNegotiated).Which;
        diallerRecord.Value0.Should().Be(2_000); // the dialler's own Linger
        diallerRecord.Value1.Should().Be(2_000); // the same negotiated minimum
    }

    // =========================================================================================
    // Admission cap
    // =========================================================================================

    [Fact]
    public void Pump_MoreConnectionsPendingThanTheAdmissionCap_ProcessesExactlyTheCapInOnePump()
    {
        // Arrange
        const int admissionCap = 3;
        using StreamSessionReactorHarness accepter = new(capacity: 10, admissionCap: admissionCap);
        List<RawTcpPeer> peers = [.. Enumerable.Range(0, 7).Select(_ => new RawTcpPeer(accepter.Endpoint))];
        try
        {
            // Act — every connection lands in the backlog, and every Hello with it, before the Reactor
            // is Pumped even once; a short sleep gives loopback time to complete every three-way
            // handshake and carry every Hello across before that one Pump call reads the listening
            // socket. The Hellos are what make the reply below possible at all: this end answers a
            // connection only once it has read the record saying whether it is a fresh dial or a Resume
            // claiming a Slot it already owns (issue #123), so a peer that connects and says nothing is
            // held, silently, until it does.
            foreach (RawTcpPeer peer in peers)
            {
                peer.Send(StreamSessionTestRecords.BuildHello());
            }

            Thread.Sleep(20);
            accepter.Reactor.Pump();

            // Assert — measured as work performed (a genuine Hello reply received), never as elapsed
            // time: exactly `admissionCap` peers were accepted and sent this end's own Hello.
            int repliesReceived = peers.Count(peer => peer.TryReceiveWithinTimeout(RecordSize, out _));
            repliesReceived.Should().Be(admissionCap);
        }
        finally
        {
            foreach (RawTcpPeer peer in peers)
            {
                peer.Dispose();
            }
        }
    }

    [Fact]
    public void Pump_AFrameThatDefersConnectionsPastTheCap_EmitsExactlyOneAdmissionCapReachedRecord()
    {
        // Arrange
        const int admissionCap = 2;
        using StreamSessionReactorHarness accepter = new(capacity: 10, admissionCap: admissionCap);
        List<RawTcpPeer> peers = [.. Enumerable.Range(0, 5).Select(_ => new RawTcpPeer(accepter.Endpoint))];
        try
        {
            // Act
            Thread.Sleep(20);
            accepter.Reactor.Pump();

            // Assert — reported once per Frame, not once per deferred connection.
            List<DiagnosticRecord> capRecords = [.. accepter.Sink.Received.Where(r => r.Id == DiagnosticId.AdmissionCapReached)];
            capRecords.Should().ContainSingle();
            capRecords[0].Value0.Should().Be(admissionCap);
        }
        finally
        {
            foreach (RawTcpPeer peer in peers)
            {
                peer.Dispose();
            }
        }
    }

    // =========================================================================================
    // Hostile peer robustness
    // =========================================================================================

    [Fact]
    public void Pump_AConnectionThatOpensAndSendsNothing_NeverThrowsAndFreesItsSlotOnceClosed()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();

        // Act
        Action act = () =>
        {
            LinkId link = hostile.ConnectAndSend(accepter.Endpoint, payload: []);
            accepter.PumpAndDrainAll(3);
            hostile.Close(link);
            accepter.PumpAndDrainAll(3);
        };

        // Assert — never throws, and the Slot the silent connection held is free afterwards.
        act.Should().NotThrow();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_ATruncatedHandshake_NeverThrowsAndFreesItsSlotOnceClosed()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();
        byte[] partial = StreamSessionTestRecords.BuildHello(ProtocolVersion).AsSpan(0, 10).ToArray();

        // Act
        Action act = () =>
        {
            LinkId link = hostile.ConnectAndSend(accepter.Endpoint, partial);
            accepter.PumpAndDrainAll(3);
            hostile.Close(link);
            accepter.PumpAndDrainAll(3);
        };

        // Assert
        act.Should().NotThrow();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Pump_AHandshakeWithAGarbageVersion_NeverThrowsAndFreesItsSlotAfterRefusal()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using HostilePeer hostile = new();
        byte[] garbage = StreamSessionTestRecords.BuildHello(version: 0xDEAD_BEEF);

        // Act
        Action act = () =>
        {
            hostile.ConnectAndSend(accepter.Endpoint, garbage);
            accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.PeerRefused));
        };

        // Assert
        act.Should().NotThrow();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// this Reactor has never seen — the instrument the parent slice spec's Further Notes call for.
    /// Unlike <see cref="HostilePeer"/>, this type can also read raw bytes back, which the version and
    /// capacity refusal tests need in order to parse the Refusal record off the wire.
    /// </summary>
    private sealed class RawTcpPeer : IDisposable
    {
        private readonly Socket _socket;

        /// <param name="destination">The Reactor's bound endpoint to connect to.</param>
        /// <param name="receiveBufferSize">
        /// Applied before <c>Connect</c> — mirrors <see cref="RawReactorHarness.CreateCounterpartyListener"/>'s
        /// receive-buffer parameter — so the negotiated window is small from the handshake onward, which is
        /// how a short-send is provoked without pinning a magic payload size. Omit for the OS default.
        /// </param>
        public RawTcpPeer(IPEndPoint destination, int? receiveBufferSize = null)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            if (receiveBufferSize is int size)
            {
                _socket.ReceiveBufferSize = size;
            }

            _socket.Connect(destination);
            _socket.ReceiveTimeout = 5000;
        }

        public void Send(byte[] bytes) => _socket.Send(bytes);

        public byte[] Receive(int count)
        {
            byte[] buffer = new byte[count];
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = _socket.Receive(buffer.AsSpan(totalRead));
                if (read == 0)
                {
                    throw new InvalidOperationException($"RawTcpPeer expected {count} bytes but the stream ended after {totalRead}.");
                }

                totalRead += read;
            }

            return buffer;
        }

        /// <summary>
        /// Reads fixed-size records until one is not a Hello, skipping any that are. The accepter sends
        /// its own unconditional Hello the instant it accepts, before it has read enough of this peer's
        /// to refuse it — so a Refusal always follows at least one Hello on this connection.
        /// </summary>
        public byte[] ReceiveNextNonHelloRecord()
        {
            while (true)
            {
                byte[] record = Receive(RecordSize);
                if (record[0] != HelloType)
                {
                    return record;
                }
            }
        }

        /// <summary>
        /// Reads <paramref name="count"/> bytes within a short timeout, answering whether it succeeded
        /// rather than throwing or blocking indefinitely — used to tell a connection this Reactor
        /// admitted from one it left waiting in the backlog.
        /// </summary>
        public bool TryReceiveWithinTimeout(int count, out byte[] record, int timeoutMs = 300)
        {
            _socket.ReceiveTimeout = timeoutMs;
            byte[] buffer = new byte[count];
            int totalRead = 0;
            try
            {
                while (totalRead < count)
                {
                    int read = _socket.Receive(buffer.AsSpan(totalRead));
                    if (read == 0)
                    {
                        record = [];
                        return false;
                    }

                    totalRead += read;
                }
            }
            catch (SocketException)
            {
                record = [];
                return false;
            }

            record = buffer;
            return true;
        }

        /// <summary>Expects the remote to have closed gracefully: a further read reports end-of-stream, never a reset.</summary>
        public void ExpectGracefulClose()
        {
            byte[] buffer = new byte[1];
            int read = _socket.Receive(buffer);
            read.Should().Be(0);
        }

        public void Dispose() => _socket.Dispose();
    }
}
