using System.Net;
using System.Net.Sockets;
using System.Reflection;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The formatter's wording is permanent public surface (issue #60, ADR 0005, ADR 0013): consumers grep
/// it, alert on it and paste it into issues. Each row of <see cref="_snapshots"/> pins the exact text
/// one member renders to, and the remaining tests pin the rules every rendering obeys.
/// </summary>
public class DiagnosticFormatterTests
{
    private static readonly PeerId _peer = new(Origin: 7, Index: 3, Generation: 1);
    private static readonly LinkId _link = new(Origin: 7, Index: 0, Generation: 2);
    private static readonly Source _source = Source.FromIPEndPoint(IPEndPoint.Parse("203.0.113.7:4242"));

    private const string PeerText = "peer slot 3 generation 1 of reactor 7";
    private const string LinkText = "link slot 0 generation 2 of reactor 7";
    private const string SourceText = "claimed source 203.0.113.7:4242 (unauthenticated)";

    /// <summary>
    /// One exact rendering per member, and per factory where a member has two shapes. A member added
    /// to the enum without a row here fails <see cref="Snapshots_PinnedTable_CoversEveryDiagnosticIdMember"/>.
    /// </summary>
    private static readonly Snapshot[] _snapshots =
    [
        new(
            DiagnosticRecord.ForAdmissionCapReached(cap: 64, declined: 3),
            "Warn AdmissionCapReached (1): the admission cap was reached with work still waiting (cap 64, admissions declined 3)"),
        new(
            DiagnosticRecord.ForArenaExhausted(_link, bytesRequested: 20_000, bytesFree: 100),
            $"Warn ArenaExhausted (2), {LinkText}: the frame arena had no room for a request (bytes requested 20000, bytes free 100)"),
        new(
            DiagnosticRecord.ForArenaExhausted(_peer, bytesRequested: 20_000, bytesFree: 100),
            $"Warn ArenaExhausted (2), {PeerText}: the frame arena had no room for a request (bytes requested 20000, bytes free 100)"),
        new(
            DiagnosticRecord.ForCapacityExhausted(capacity: 8, declined: 1),
            "Warn CapacityExhausted (3): every capacity slot was occupied with work still waiting (capacity 8, admissions declined 1)"),
        new(
            DiagnosticRecord.ForLinkFailed(_link, SocketError.ConnectionRefused),
            $"Warn LinkFailed (100), {LinkText}: the link failed and closed (socket error ConnectionRefused 10061)"),
        new(
            DiagnosticRecord.ForDestinationUnreachable(_link, SocketError.ConnectionReset),
            $"Warn DestinationUnreachable (101), {LinkText}: a send was reported unreachable (socket error ConnectionReset 10054)"),
        new(
            DiagnosticRecord.ForDatagramTruncated(_source, bufferSize: 1500),
            $"Warn DatagramTruncated (102), {SourceText}: a datagram was truncated to the scratch buffer (buffer bytes 1500)"),
        new(
            DiagnosticRecord.ForDatagramMalformed(_source, bytesReceived: 7),
            $"Warn DatagramMalformed (200), {SourceText}: a datagram naming no Peer was discarded (bytes received 7)"),
        new(
            DiagnosticRecord.ForCookieRejected(_source, epochOffered: 4, epochCurrent: 6),
            $"Warn CookieRejected (201), {SourceText}: a Hello's cookie was rejected (epoch offered 4, epoch current 6)"),
        new(
            DiagnosticRecord.ForVersionRefused(_source, versionOffered: 2, versionRequired: 1),
            $"Warn VersionRefused (202), {SourceText}: a Hello offering an unsupported protocol version was refused (version offered 2, version required 1)"),
        new(
            DiagnosticRecord.ForFragmentSetAbandoned(_peer, fragmentsHeld: 1, fragmentsExpected: 40),
            $"Warn FragmentSetAbandoned (203), {PeerText}: an incomplete fragment set was abandoned (fragments held 1, fragments expected 40)"),
        new(
            DiagnosticRecord.ForRetransmissionTimerBackedOff(_peer, newRtoMilliseconds: 400, consecutiveBackoffs: 2),
            $"Trace RetransmissionTimerBackedOff (204), {PeerText}: the retransmission timer backed off (RTO 400 ms, consecutive backoffs 2)"),
        new(
            DiagnosticRecord.ForOversizedFragmentSet(_peer, declaredFragmentCount: 300, maxLegalFragmentCount: 256),
            $"Warn OversizedFragmentSet (205), {PeerText}: a fragment set declared more fragments than the receive window can hold and the Session was closed (fragments declared 300, largest legal 256)"),
        new(
            DiagnosticRecord.ForUnreliableFragment(_source, modeBits: 1),
            $"Warn UnreliableFragment (206), {SourceText}: a datagram fragmenting in an unreliable mode was discarded (mode UnreliableSequenced, mode bits 1)"),
        new(
            DiagnosticRecord.ForOversizedMessage(_peer, messageLength: 5000, maxMessageSize: 4096),
            $"Warn OversizedMessage (207), {PeerText}: a Message larger than MaxMessageSize was discarded (bytes 5000, MaxMessageSize 4096)"),
        new(
            DiagnosticRecord.ForAddressPeerLimitReached(_source, limit: 2),
            $"Warn AddressPeerLimitReached (208), {SourceText}: a handshake was refused because its address holds as many Peers as it may (MaxPeersPerAddress 2)"),
        new(
            DiagnosticRecord.ForDatagramReadLimitReached(limit: 3, socketIndex: 1),
            "Warn DatagramReadLimitReached (209): a Pump made as many reads on bound socket 1 as it may (MaxDatagramsPerPump 3)"),
        new(
            DiagnosticRecord.ForPeerRefusedCapacity(_source, capacity: 8),
            $"Info PeerRefused (300), {SourceText}: a dialler was refused because every capacity slot was occupied (capacity 8)"),
        new(
            DiagnosticRecord.ForPeerRefusedVersion(_source, versionOffered: 2, versionRequired: 1),
            $"Info PeerRefused (300), {SourceText}: a dialler offering an unsupported protocol version was refused (version offered 2, version required 1)"),
        new(
            DiagnosticRecord.ForLingerNegotiated(_peer, oursMilliseconds: 5000, effectiveMilliseconds: 3000),
            $"Info LingerNegotiated (301), {PeerText}: the Session established and Linger was negotiated (own 5000 ms, effective 3000 ms)"),
        new(
            DiagnosticRecord.ForMalformedFraming(_peer, announcedLength: 4_294_967_295, maxMessageSize: 65_536),
            $"Warn MalformedFraming (302), {PeerText}: the Peer's framing broke this profile's rules and the Session was closed (length prefix 4294967295, max message size 65536)"),
        new(
            DiagnosticRecord.ForMalformedFraming(_peer, announcedLength: 0, maxMessageSize: 65_536),
            $"Warn MalformedFraming (302), {PeerText}: the Peer's framing broke this profile's rules and the Session was closed (length prefix 0 or none read, max message size 65536)"),
        new(
            DiagnosticRecord.ForDataAfterGoodbye(_peer),
            $"Warn DataAfterGoodbye (303), {PeerText}: the Peer sent Message bytes after its own Goodbye and the Session was closed"),
        new(
            DiagnosticRecord.ForResumeMatched(_peer, bytesRewound: 512),
            $"Info ResumeMatched (304), {PeerText}: a valid resume secret was matched and the Session resumed (bytes rewound 512)"),
        new(
            DiagnosticRecord.ForResumeUnmatched(_source, candidatesChecked: 2, recordType: 3),
            $"Warn ResumeUnmatched (305), {SourceText}: a claim to an existing Session was refused (candidate Peers checked 2, arrived as Resume, wire record type 3)"),
        new(
            DiagnosticRecord.ForRedialAttempted(_peer, attemptNumber: 2, delayMilliseconds: 250),
            $"Info RedialAttempted (306), {PeerText}: a re-dial attempt began (attempt 2, waited 250 ms)"),
        new(
            DiagnosticRecord.ForRedialFailed(_peer, SocketError.ConnectionRefused, attemptNumber: 2),
            $"Info RedialFailed (307), {PeerText}: a re-dial attempt could not connect (socket error ConnectionRefused 10061, attempt 2)"),
        new(
            DiagnosticRecord.ForDeliveryCounterRefused(_peer, claimedCount: 10, appendedMessageCount: 4),
            $"Warn DeliveryCounterRefused (308), {PeerText}: the Peer claimed delivery of more Messages than were sent and the Session was closed (count claimed 10, count appended 4)"),
    ];

    /// <summary>Every snapshot row, as xUnit theory data.</summary>
    public static TheoryData<int> SnapshotIndices()
    {
        TheoryData<int> data = [];
        for (int i = 0; i < _snapshots.Length; i++)
        {
            data.Add(i);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(SnapshotIndices))]
    public void Format_EachMember_RendersItsPinnedWording(int index)
    {
        // Arrange
        Snapshot snapshot = _snapshots[index];

        // Act
        string text = DiagnosticFormatter.Format(snapshot.Record);

        // Assert
        text.Should().Be(snapshot.Expected);
    }

    [Fact]
    public void Snapshots_PinnedTable_CoversEveryDiagnosticIdMember()
    {
        // Arrange
        DiagnosticId[] members = Enum.GetValues<DiagnosticId>();

        // Act
        DiagnosticId[] covered = _snapshots.Select(s => s.Record.Id).Distinct().ToArray();

        // Assert
        covered.Should().BeEquivalentTo(members);
    }

    [Fact]
    public void Format_EveryMember_SaysInWordsWhatHappenedAfterTheHeader()
    {
        // Arrange
        // Act
        string[] sentences = _snapshots
            .Select(s => DiagnosticFormatter.Format(s.Record))
            .Select(text => text[(text.IndexOf("): ", StringComparison.Ordinal) + 3)..])
            .ToArray();

        // Assert
        sentences.Should().AllSatisfy(sentence =>
            sentence.Count(char.IsLetter).Should().BeGreaterThan(10, "a record that renders as bare numbers is a member whose wording was never written"));
    }

    [Fact]
    public void Format_EverySourceSubject_MarksTheAddressAsAClaimRatherThanAFact()
    {
        // Arrange
        DiagnosticRecord[] sourceRecords = _snapshots
            .Select(s => s.Record)
            .Where(r => r.SubjectKind == DiagnosticSubjectKind.Source)
            .ToArray();

        // Act
        string[] texts = sourceRecords.Select(r => DiagnosticFormatter.Format(r)).ToArray();

        // Assert
        texts.Should().NotBeEmpty();
        texts.Should().AllSatisfy(text =>
        {
            text.Should().Contain("claimed source 203.0.113.7:4242 (unauthenticated)");
            text.Should().NotContain("from 203.0.113.7");
        });
    }

    [Fact]
    public void Format_IPv6SourceWithAScopeId_RendersTheScope()
    {
        // Arrange
        Source source = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Parse("fe80::1%3"), 4242));
        DiagnosticRecord record = DiagnosticRecord.ForDatagramMalformed(source, bytesReceived: 7);

        // Act
        string text = DiagnosticFormatter.Format(record);

        // Assert
        text.Should().Contain("claimed source [fe80::1%3]:4242 (unauthenticated)");
    }

    [Fact]
    public void Format_IPv6SourceWithoutAScopeId_RendersNoScope()
    {
        // Arrange
        Source source = Source.FromIPEndPoint(IPEndPoint.Parse("[2001:db8::7]:4242"));
        DiagnosticRecord record = DiagnosticRecord.ForDatagramMalformed(source, bytesReceived: 7);

        // Act
        string text = DiagnosticFormatter.Format(record);

        // Assert
        text.Should().Contain("claimed source [2001:db8::7]:4242 (unauthenticated)");
    }

    [Fact]
    public void Format_IPv4SourceCarryingAStrayScopeId_RendersNoPhantomScope()
    {
        // Arrange
        Source source = _source with { ScopeId = 3 };
        DiagnosticRecord record = DiagnosticRecord.ForDatagramMalformed(source, bytesReceived: 7);

        // Act
        string text = DiagnosticFormatter.Format(record);

        // Assert
        text.Should().Contain("claimed source 203.0.113.7:4242 (unauthenticated)");
        text.Should().NotContain("%");
    }

    [Fact]
    public void Format_ARecord_LeavesTheRecordUnchanged()
    {
        // Arrange
        DiagnosticRecord record = DiagnosticRecord.ForCookieRejected(_source, epochOffered: 4, epochCurrent: 6);
        DiagnosticRecord before = record;

        // Act
        _ = DiagnosticFormatter.Format(record);

        // Assert
        record.Should().Be(before);
    }

    [Fact]
    public void DiagnosticFormatter_BeforeAnyCall_HoldsNoStaticStateToAllocate()
    {
        // Arrange
        Type formatter = typeof(DiagnosticFormatter);

        // Act
        FieldInfo[] staticFields = formatter.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

        // Assert
        formatter.IsAbstract.Should().BeTrue("a static class has no instance to allocate");
        formatter.IsSealed.Should().BeTrue("a static class has no instance to allocate");
        staticFields.Should().BeEmpty("a static field would be initialised before anything is formatted");
        formatter.TypeInitializer.Should().BeNull();
    }

    [Fact]
    public void Format_TheDefaultRecord_ThrowsBecauseItHasNoCause()
    {
        // Arrange
        DiagnosticRecord record = default;

        // Act
        Action act = () => DiagnosticFormatter.Format(record);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    private sealed record Snapshot(DiagnosticRecord Record, string Expected);
}
