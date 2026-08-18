using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.4.2 (issue #115): the Resume record on the wire — a third stream-handshake message type
/// carrying only the resume secret and the delivery counter, padded to exactly the size a Hello and a
/// Refusal occupy so a Peer waiting on a fixed-size handshake record never has to know which kind is
/// coming before it has read the whole thing. What a Resume <em>means</em> — matching it against a held
/// Interrupted Peer, ahead of the capacity check — is issue #123, so here a well-formed Resume is read
/// as one whole record and then refused, exactly as an unrecognised type is.
/// </summary>
/// <remarks>
/// The codec is internal and reached nowhere here (tests/CLAUDE.md, ADR 0015): every record is
/// hand-built by <see cref="StreamSessionTestRecords"/> over a raw counterparty socket, and every
/// assertion is on what the public L2 surface does with those bytes. Three of #115's acceptance
/// criteria are therefore not assertable here and are pinned by #123 instead, once a Resume has
/// somewhere to go: the round trip of secret and counter, the refusal of a wrong-length secret buffer,
/// and reading a Resume out of a Hello or Refusal record. Until then nothing in the profile reads a
/// Resume's payload, so no L2 behaviour depends on it — reaching past the seam to assert it anyway is
/// what ADR 0015 forbids.
/// </remarks>
public class StreamSessionResumeRecordTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    // =========================================================================================
    // A Resume is one whole record of exactly a Hello's size
    // =========================================================================================

    [Fact]
    public void Handshake_AResumeRecordOneByteShortOfAHellosSize_LeavesTheReactorWaitingForTheRest()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        byte[] resume = StreamSessionTestRecords.BuildResume();

        // Act — a short record is an incomplete read, never a malformed one.
        peer.Send(resume[..(RecordSize - 1)]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(10);

        // Assert
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Handshake_TheLastByteOfAResumeRecordArrivingSeparately_CompletesTheRecordAndClosesThePeer()
    {
        // Arrange — the same bytes as the test above, finished. That a Resume completes at exactly the
        // byte a Hello would is what pins the two records to one size on the wire.
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        byte[] resume = StreamSessionTestRecords.BuildResume();
        peer.Send(resume[..(RecordSize - 1)]);
        accepter.PumpAndDrainAll(5);

        // Act
        peer.Send(resume[(RecordSize - 1)..]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(10);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }

    [Fact]
    public void BuildResume_AgainstBuildHello_IsTheSameNumberOfBytes()
    {
        // Arrange
        byte[] hello = StreamSessionTestRecords.BuildHello();

        // Act
        byte[] resume = StreamSessionTestRecords.BuildResume();

        // Assert — the premise the two boundary tests above rest on: the record the Reactor is fed is
        // the size a Hello occupies, not merely 41 bytes by coincidence of this suite.
        resume.Should().HaveCount(hello.Length);
    }

    [Fact]
    public void Handshake_AHelloOfTheSameLengthAsAResumeRecord_StillEstablishes()
    {
        // Arrange — the control for the tests above: the size a Resume is read at is the size a Hello
        // establishes at.
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        byte[] hello = StreamSessionTestRecords.BuildHello();

        // Act
        peer.Send(hello);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(10);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected);
    }

    // =========================================================================================
    // The type byte tells all three kinds apart, and the discard arm refuses the rest
    // =========================================================================================

    [Fact]
    public void Handshake_AWholeResumeRecordInPlaceOfAHello_DoesNotEstablishTheSession()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        byte[] secret = Enumerable.Range(1, 16).Select(b => (byte)b).ToArray();

        // Act — a Resume is a claim on a Session, not a negotiation: it announces no version and no
        // parameters, so there is nothing here to establish a fresh Session from (matching is #123).
        peer.Send(StreamSessionTestRecords.BuildResume(secret, deliveryCount: 0x0102030405060708));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(10);

        // Assert
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Handshake_AWholeResumeRecordInPlaceOfAHello_ClosesThePeerRatherThanGuessing()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);

        // Act
        peer.Send(StreamSessionTestRecords.BuildResume());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(10);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }

    [Fact]
    public void Handshake_ARecordWhoseTypeByteNamesNoKnownKind_ClosesThePeerRatherThanGuessing()
    {
        // Arrange — one past the Resume type this ticket added: the discard arm refuses an unrecognised
        // type rather than treating it as the nearest kind it knows.
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        byte[] record = StreamSessionTestRecords.BuildResume();
        record[0] = StreamSessionTestRecords.ResumeRecordType + 1;

        // Act
        peer.Send(record);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(10);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }
}
