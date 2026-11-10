using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="StreamSessionOptions.MaxPeersPerAddress"/> (issue #452): one address may hold at most that
/// many accepted Peers, so it cannot occupy every capacity Slot. A Hello over the limit gets the capacity
/// Refusal a full Reactor sends, and a Resume is never refused by it, because the Peer it names already
/// owns its Slot (ADR 0011).
/// </summary>
/// <remarks>
/// A second address is <c>127.0.0.2</c>, which routes to loopback on Windows and Linux. Each
/// <see cref="StreamSessionReactorHarness.RawStreamPeer"/> connects from a fresh ephemeral port, so every
/// test here also shows that the count ignores the port.
/// </remarks>
public class StreamSessionPerAddressLimitTests
{
    private const byte RefusalReasonCapacity = 2;

    private static readonly IPAddress _otherAddress = IPAddress.Parse("127.0.0.2");

    [Fact]
    public void AttemptFreshConnection_LimitSetToCapacity_LetsOneAddressOccupyEverySlot()
    {
        // Arrange — the issue's failure scenario: with no limit, one address fills the Reactor.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 3);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult other =
            accepter.AttemptFreshConnection(localAddress: _otherAddress);

        // Assert
        other.Admitted.Should().BeFalse();
        other.RefusalReason.Should().Be(RefusalReasonCapacity);
        DisposeAll(held);
    }

    [Fact]
    public void AttemptFreshConnection_LimitLeftAtItsDefault_KeepsTheLastSlotForAnotherAddress()
    {
        // Arrange — the default is one below capacity, so one address cannot take every Slot.
        using StreamSessionReactorHarness accepter = new(capacity: 3, defaultMaxPeersPerAddress: true);
        List<StreamSessionReactorHarness.HeldPeer> held = [accepter.EstablishOnePeer(), accepter.EstablishOnePeer()];
        using StreamSessionReactorHarness.InboundAttemptResult refused = accepter.AttemptFreshConnection();

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult other =
            accepter.AttemptFreshConnection(localAddress: _otherAddress);

        // Assert
        refused.Admitted.Should().BeFalse();
        other.Admitted.Should().BeTrue();
        DisposeAll(held);
    }

    [Fact]
    public void AttemptFreshConnection_AddressAtItsPeerLimit_IsRefusedForCapacity()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = [accepter.EstablishOnePeer(), accepter.EstablishOnePeer()];

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult third = accepter.AttemptFreshConnection();

        // Assert
        third.Admitted.Should().BeFalse();
        third.RefusalReason.Should().Be(RefusalReasonCapacity);
        third.RefusalCapacity.Should().Be(3);
        DisposeAll(held);
    }

    [Fact]
    public void AttemptFreshConnection_AddressAtItsPeerLimit_LeavesTheRemainingSlotForAnotherAddress()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = [accepter.EstablishOnePeer(), accepter.EstablishOnePeer()];
        using StreamSessionReactorHarness.InboundAttemptResult refused = accepter.AttemptFreshConnection();

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult other =
            accepter.AttemptFreshConnection(localAddress: _otherAddress);

        // Assert
        refused.Admitted.Should().BeFalse();
        other.Admitted.Should().BeTrue();
        other.Peer.Should().NotBeNull();
        DisposeAll(held);
    }

    [Fact]
    public void AttemptFreshConnection_AddressAtItsPeerLimit_LogsAddressPeerLimitReachedRatherThanPeerRefused()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = [accepter.EstablishOnePeer()];

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult refused = accepter.AttemptFreshConnection();

        // Assert
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.Should().Be(Source.FromIPEndPoint(refused.Counterparty.LocalEndPoint));
        record.Value0.Should().Be(1);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.PeerRefused);
        DisposeAll(held);
    }

    [Fact]
    public void AttemptFreshConnection_EverySlotOccupied_LogsPeerRefusedRatherThanAddressPeerLimitReached()
    {
        // Arrange — a full Reactor is a capacity refusal even when the address is also at its limit.
        using StreamSessionReactorHarness accepter = new(capacity: 2, maxPeersPerAddress: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult refused = accepter.AttemptFreshConnection();

        // Assert
        refused.Admitted.Should().BeFalse();
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.PeerRefused);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.AddressPeerLimitReached);
        DisposeAll(held);
    }

    [Fact]
    public void AttemptFreshConnection_AddressWhosePeerIsInterrupted_StaysCharged()
    {
        // Arrange — an Interrupted Peer still holds its Slot, reserved for a Resume.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult refused = accepter.AttemptFreshConnection();

        // Assert
        refused.Admitted.Should().BeFalse();
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached);
        held.Counterparty.Dispose();
    }

    [Fact]
    public void Handshake_AResumeFromAnAddressAtItsPeerLimit_ResumesTheHeldPeer()
    {
        // Arrange — the address holds its whole limit, and the Peer it holds is the one coming back.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerResumed));

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed).Which.Peer.Should().Be(held.Peer);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.AddressPeerLimitReached);
        held.Counterparty.Dispose();
    }

    [Fact]
    public void AttemptFreshConnection_PeerResumedFromAnotherAddress_StaysChargedToTheAddressItWasAcceptedFrom()
    {
        // Arrange — were the charge to follow the Peer, one address could fill its limit, move those Peers
        // to a second address it controls, and fill its limit again.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint, _otherAddress);
        resumed.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerResumed));

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult sameAddressAsTheHandshake = accepter.AttemptFreshConnection();

        // Assert
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == held.Peer);
        sameAddressAsTheHandshake.Admitted.Should().BeFalse();
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached);
        held.Counterparty.Dispose();
    }

    [Fact]
    public void AttemptFreshConnection_AddressWhoseOnlyPeerClosed_IsAdmittedAgain()
    {
        // Arrange — the Interrupted Peer's Linger expires, which closes it and releases its Slot.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);
        accepter.TimeProvider.Advance(accepter.Linger);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == held.Peer));

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult again = accepter.AttemptFreshConnection();

        // Assert
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == held.Peer);
        again.Admitted.Should().BeTrue();
        held.Counterparty.Dispose();
    }

    [Fact]
    public void AttemptFreshConnection_AddressThisReactorDialled_IsNotChargedForTheDial()
    {
        // Arrange — a dial is this Reactor's own choice, so it spends none of the remote address's limit.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1);
        using StreamSessionReactorHarness target = new();
        accepter.Reactor.Connect(target.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> dialled, _) =
            StreamSessionReactorHarness.PumpBothUntilConnected(accepter, target);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult inbound = accepter.AttemptFreshConnection();

        // Assert
        dialled.Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);
        inbound.Admitted.Should().BeTrue();
    }

    // =========================================================================================
    // IPv6 prefix grouping (issue #453)
    // =========================================================================================

    [IPv6PrefixFact]
    public void AttemptFreshConnection_SecondAddressInAPrefixAtItsPeerLimit_IsRefusedForCapacity()
    {
        // Arrange — the default groups by /64, and both addresses are in fd00:453::/64.
        using StreamSessionReactorHarness accepter = IPv6Accepter(peerLimitIPv6PrefixLength: null);
        using StreamSessionReactorHarness.InboundAttemptResult first =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixFirst);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult second =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixSecond);

        // Assert
        first.Admitted.Should().BeTrue();
        second.Admitted.Should().BeFalse();
        second.RefusalReason.Should().Be(RefusalReasonCapacity);
    }

    [IPv6PrefixFact]
    public void AttemptFreshConnection_AddressOutsideAPrefixAtItsPeerLimit_KeepsItsOwnLimit()
    {
        // Arrange — fd00:453:0:1::1 shares a /48 with fd00:453::1, but not a /64.
        using StreamSessionReactorHarness accepter = IPv6Accepter(peerLimitIPv6PrefixLength: null);
        using StreamSessionReactorHarness.InboundAttemptResult first =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixFirst);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outside =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.OutsidePrefix);

        // Assert
        first.Admitted.Should().BeTrue();
        outside.Admitted.Should().BeTrue();
    }

    [IPv6PrefixFact]
    public void AttemptFreshConnection_PrefixLengthOf128_CountsEachAddressInAPrefixOnItsOwn()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = IPv6Accepter(peerLimitIPv6PrefixLength: 128);
        using StreamSessionReactorHarness.InboundAttemptResult first =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixFirst);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult second =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixSecond);

        // Assert
        first.Admitted.Should().BeTrue();
        second.Admitted.Should().BeTrue();
    }

    [IPv6PrefixFact]
    public void AttemptFreshConnection_SecondAddressInAPrefixAtItsPeerLimit_LogsThePrefixLengthItWasCountedBy()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = IPv6Accepter(peerLimitIPv6PrefixLength: 56);
        using StreamSessionReactorHarness.InboundAttemptResult first =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixFirst);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult second =
            accepter.AttemptFreshConnection(localAddress: IPv6PrefixFactAttribute.InPrefixSecond);

        // Assert
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached)
            .Which.Value1.Should().Be(56);
    }

    [Fact]
    public void AttemptFreshConnection_IPv4AddressAtItsPeerLimit_LogsThirtyTwoAsThePrefixLength()
    {
        // Arrange — IPv4 addresses are counted whole, whatever the IPv6 setting.
        using StreamSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 1, peerLimitIPv6PrefixLength: 56);
        using StreamSessionReactorHarness.InboundAttemptResult first = accepter.AttemptFreshConnection();

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult second = accepter.AttemptFreshConnection();

        // Assert
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached)
            .Which.Value1.Should().Be(32);
    }

    /// <summary>An accepter on <c>[::1]</c> whose per-address limit is 1, grouped by the given length.</summary>
    private static StreamSessionReactorHarness IPv6Accepter(int? peerLimitIPv6PrefixLength) =>
        new(capacity: 3,
            maxPeersPerAddress: 1,
            peerLimitIPv6PrefixLength: peerLimitIPv6PrefixLength,
            endpoints: [new IPEndPoint(IPAddress.IPv6Loopback, 0)]);

    private static void DisposeAll(List<StreamSessionReactorHarness.HeldPeer> held)
    {
        foreach (StreamSessionReactorHarness.HeldPeer peer in held)
        {
            peer.Counterparty.Dispose();
        }
    }
}
