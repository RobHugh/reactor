using System.Net.Sockets;

namespace ReactorSockets;

/// <summary>
/// A single diagnostic emitted to an <see cref="IReactorDiagnostics"/> Sink: a closed
/// <see cref="DiagnosticId"/>, its fixed <see cref="DiagnosticLevel"/>, a tagged subject — none,
/// Link, Source or Peer (ADR 0013) — and two numeric slots whose meaning is documented per
/// <see cref="DiagnosticId"/> member. No strings, no <c>object[]</c>, no closures — everything here
/// lives on the stack and is built only after <see cref="IReactorDiagnostics.IsEnabled"/> passes.
/// </summary>
public readonly record struct DiagnosticRecord
{
    private DiagnosticRecord(
        DiagnosticId id,
        DiagnosticLevel level,
        DiagnosticSubjectKind subjectKind,
        LinkId link,
        Source source,
        PeerId peer,
        long value0,
        long value1)
    {
        Id = id;
        Level = level;
        SubjectKind = subjectKind;
        Link = link;
        Source = source;
        Peer = peer;
        Value0 = value0;
        Value1 = value1;
    }

    /// <summary>Which condition produced this record.</summary>
    public DiagnosticId Id { get; }

    /// <summary>The fixed severity for <see cref="Id"/>, per ADR 0013's roster.</summary>
    public DiagnosticLevel Level { get; }

    /// <summary>Which of <see cref="Link"/>, <see cref="Source"/> or <see cref="Peer"/>, if any, is populated.</summary>
    public DiagnosticSubjectKind SubjectKind { get; }

    /// <summary>
    /// The Link this record concerns, valid only when <see cref="SubjectKind"/> is
    /// <see cref="DiagnosticSubjectKind.Link"/>.
    /// </summary>
    public LinkId Link { get; }

    /// <summary>
    /// The claimed Source this record concerns, valid only when <see cref="SubjectKind"/> is
    /// <see cref="DiagnosticSubjectKind.Source"/>.
    /// </summary>
    public Source Source { get; }

    /// <summary>
    /// The Peer this record concerns, valid only when <see cref="SubjectKind"/> is
    /// <see cref="DiagnosticSubjectKind.Peer"/>.
    /// </summary>
    public PeerId Peer { get; }

    /// <summary>The first numeric slot; meaning documented on the <see cref="DiagnosticId"/> member.</summary>
    public long Value0 { get; }

    /// <summary>The second numeric slot; meaning documented on the <see cref="DiagnosticId"/> member.</summary>
    public long Value1 { get; }

    /// <summary>
    /// Builds a <see cref="DiagnosticId.AdmissionCapReached"/> record. No subject.
    /// <paramref name="declined"/> counts declinings where the work waits out of reach and pieces of
    /// work where the Reactor held it — see the remarks on
    /// <see cref="DiagnosticId.AdmissionCapReached"/>.
    /// </summary>
    public static DiagnosticRecord ForAdmissionCapReached(int cap, int declined) =>
        new(DiagnosticId.AdmissionCapReached, DiagnosticLevel.Warn, DiagnosticSubjectKind.None, default, default, default, cap, declined);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.CapacityExhausted"/> record. No subject.
    /// <paramref name="declined"/> is counted exactly as
    /// <see cref="ForAdmissionCapReached"/>'s is.
    /// </summary>
    public static DiagnosticRecord ForCapacityExhausted(int capacity, int declined) =>
        new(DiagnosticId.CapacityExhausted, DiagnosticLevel.Warn, DiagnosticSubjectKind.None, default, default, default, capacity, declined);

    /// <summary>
    /// Builds an <see cref="DiagnosticId.ArenaExhausted"/> record for the Link that made the request.
    /// </summary>
    public static DiagnosticRecord ForArenaExhausted(LinkId link, int bytesRequested, int bytesFree) =>
        new(DiagnosticId.ArenaExhausted, DiagnosticLevel.Warn, DiagnosticSubjectKind.Link, link, default, default, bytesRequested, bytesFree);

    /// <summary>
    /// Builds an <see cref="DiagnosticId.ArenaExhausted"/> record for the Peer whose Message could not
    /// be reserved — the Session profile's counterpart to the Link-subject overload above (ADR 0014:
    /// share-holder identity is per-Peer in a Session, not per-socket).
    /// </summary>
    public static DiagnosticRecord ForArenaExhausted(PeerId peer, int bytesRequested, int bytesFree) =>
        new(DiagnosticId.ArenaExhausted, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, bytesRequested, bytesFree);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.LinkFailed"/> record carrying the platform
    /// <see cref="SocketError"/> that caused the failure.
    /// </summary>
    public static DiagnosticRecord ForLinkFailed(LinkId link, SocketError error) =>
        new(DiagnosticId.LinkFailed, DiagnosticLevel.Warn, DiagnosticSubjectKind.Link, link, default, default, (long)error, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DestinationUnreachable"/> record carrying the platform
    /// <see cref="SocketError"/> reported for the send.
    /// </summary>
    public static DiagnosticRecord ForDestinationUnreachable(LinkId link, SocketError error) =>
        new(DiagnosticId.DestinationUnreachable, DiagnosticLevel.Warn, DiagnosticSubjectKind.Link, link, default, default, (long)error, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DatagramTruncated"/> record for the claimed Source, carrying
    /// the scratch buffer size the datagram was truncated to.
    /// </summary>
    public static DiagnosticRecord ForDatagramTruncated(Source source, int bufferSize) =>
        new(DiagnosticId.DatagramTruncated, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, default, bufferSize, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DatagramMalformed"/> record for the claimed Source, carrying
    /// the number of bytes that arrived.
    /// </summary>
    public static DiagnosticRecord ForDatagramMalformed(Source source, int bytesReceived) =>
        new(DiagnosticId.DatagramMalformed, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, default, bytesReceived, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.CookieRejected"/> record for the claimed Source, carrying the
    /// epoch the cookie offered and the epoch current when it was checked.
    /// </summary>
    public static DiagnosticRecord ForCookieRejected(Source source, long epochOffered, long epochCurrent) =>
        new(DiagnosticId.CookieRejected, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, default, epochOffered, epochCurrent);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.VersionRefused"/> record for the claimed Source, carrying the
    /// version offered and the version this Reactor requires.
    /// </summary>
    public static DiagnosticRecord ForVersionRefused(Source source, long versionOffered, long versionRequired) =>
        new(DiagnosticId.VersionRefused, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, default, versionOffered, versionRequired);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.FragmentSetAbandoned"/> record for the Peer whose Fragment set
    /// was abandoned before it ever completed, carrying the fragments actually held and the number the
    /// set's own metadata declared.
    /// </summary>
    public static DiagnosticRecord ForFragmentSetAbandoned(PeerId peer, long fragmentsHeld, long fragmentsExpected) =>
        new(DiagnosticId.FragmentSetAbandoned, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, fragmentsHeld, fragmentsExpected);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.OversizedFragmentSet"/> record for the Peer whose Fragment
    /// declared a <c>fragmentCount</c> larger than this Reactor's own receive window could ever hold,
    /// carrying the declared count and the largest legal count.
    /// </summary>
    public static DiagnosticRecord ForOversizedFragmentSet(PeerId peer, long declaredFragmentCount, long maxLegalFragmentCount) =>
        new(DiagnosticId.OversizedFragmentSet, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, declaredFragmentCount, maxLegalFragmentCount);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.UnreliableFragment"/> record for the claimed Source that put a
    /// Fragment on the wire in an unreliable mode (issue #273), carrying the mode it claimed as the two
    /// mode bits' wire value. <c>Value1</c> is unused: the rule has no threshold to report against.
    /// </summary>
    public static DiagnosticRecord ForUnreliableFragment(Source source, long modeBits) =>
        new(DiagnosticId.UnreliableFragment, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, default, modeBits, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.OversizedMessage"/> record for the Peer whose completed Message
    /// was larger than this Reactor's own <c>MaxMessageSize</c> (issue #425), carrying the Message's
    /// length and that limit.
    /// </summary>
    public static DiagnosticRecord ForOversizedMessage(PeerId peer, long messageLength, long maxMessageSize) =>
        new(DiagnosticId.OversizedMessage, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, messageLength, maxMessageSize);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> record for the Peer whose
    /// <see cref="RetransmissionTimer"/> just backed off (subtask 6.4.9, issue #199). <c>Value0</c> is
    /// the new RTO in milliseconds; <c>Value1</c> is the consecutive backoff count after this backoff.
    /// </summary>
    public static DiagnosticRecord ForRetransmissionTimerBackedOff(PeerId peer, long newRtoMilliseconds, long consecutiveBackoffs) =>
        new(DiagnosticId.RetransmissionTimerBackedOff, DiagnosticLevel.Trace, DiagnosticSubjectKind.Peer, default, default, peer, newRtoMilliseconds, consecutiveBackoffs);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.PeerRefused"/> record for the claimed Source of a stream
    /// dialler refused because every capacity Slot was occupied. <c>Value0</c> carries this Reactor's
    /// capacity; <c>Value1</c> is unused.
    /// </summary>
    public static DiagnosticRecord ForPeerRefusedCapacity(Source source, int capacity) =>
        new(DiagnosticId.PeerRefused, DiagnosticLevel.Info, DiagnosticSubjectKind.Source, default, source, default, capacity, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.PeerRefused"/> record for the claimed Source of a stream
    /// dialler refused because it announced a version this Reactor does not speak. <c>Value0</c> is
    /// the version offered; <c>Value1</c> is the version this Reactor requires.
    /// </summary>
    public static DiagnosticRecord ForPeerRefusedVersion(Source source, long versionOffered, long versionRequired) =>
        new(DiagnosticId.PeerRefused, DiagnosticLevel.Info, DiagnosticSubjectKind.Source, default, source, default, versionOffered, versionRequired);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.LingerNegotiated"/> record for the newly established
    /// <paramref name="peer"/>, on either profile — stream or datagram. <c>Value0</c> is this end's own
    /// Linger in milliseconds; <c>Value1</c> is the effective (minimum of the two ends') Linger in
    /// milliseconds.
    /// </summary>
    public static DiagnosticRecord ForLingerNegotiated(PeerId peer, long oursMilliseconds, long effectiveMilliseconds) =>
        new(DiagnosticId.LingerNegotiated, DiagnosticLevel.Info, DiagnosticSubjectKind.Peer, default, default, peer, oursMilliseconds, effectiveMilliseconds);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.MalformedFraming"/> record for the Peer whose framing broke
    /// this profile's rules. <c>Value0</c> is the offending length prefix — 0 for a truncation, where
    /// no full prefix was ever read; <c>Value1</c> is this Reactor's own <c>MaxMessageSize</c>.
    /// </summary>
    public static DiagnosticRecord ForMalformedFraming(PeerId peer, long announcedLength, long maxMessageSize) =>
        new(DiagnosticId.MalformedFraming, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, announcedLength, maxMessageSize);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DataAfterGoodbye"/> record for the Peer that kept sending
    /// after its own Goodbye. No claimed length: <c>Value0</c> and <c>Value1</c> are unused.
    /// </summary>
    public static DiagnosticRecord ForDataAfterGoodbye(PeerId peer) =>
        new(DiagnosticId.DataAfterGoodbye, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, 0, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.ResumeMatched"/> record for the Peer a valid resume secret was
    /// matched back onto. <c>Value0</c> is the number of retained outbound bytes the match rewound;
    /// <c>Value1</c> is unused.
    /// </summary>
    public static DiagnosticRecord ForResumeMatched(PeerId peer, long bytesRewound) =>
        new(DiagnosticId.ResumeMatched, DiagnosticLevel.Info, DiagnosticSubjectKind.Peer, default, default, peer, bytesRewound, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.ResumeUnmatched"/> record for the claimed Source of a
    /// connection that claimed an existing Session and was refused rather than resumed.
    /// <c>Value0</c> is the number of Interrupted Peers the claim was checked against;
    /// <c>Value1</c> is the wire record type the claim arrived as.
    /// </summary>
    public static DiagnosticRecord ForResumeUnmatched(Source source, long candidatesChecked, long recordType) =>
        new(DiagnosticId.ResumeUnmatched, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, default, candidatesChecked, recordType);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.RedialAttempted"/> record for the Peer beginning a fresh
    /// re-dial attempt. <c>Value0</c> is the attempt number; <c>Value1</c> is the delay in
    /// milliseconds this attempt actually waited before dialling — the jittered figure, not the
    /// computed delay before jitter.
    /// </summary>
    public static DiagnosticRecord ForRedialAttempted(PeerId peer, long attemptNumber, long delayMilliseconds) =>
        new(DiagnosticId.RedialAttempted, DiagnosticLevel.Info, DiagnosticSubjectKind.Peer, default, default, peer, attemptNumber, delayMilliseconds);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.RedialFailed"/> record for the Peer whose re-dial attempt
    /// could not connect. <c>Value0</c> carries the platform <see cref="SocketError"/> as its
    /// underlying integer value; <c>Value1</c> is the attempt number that failed.
    /// </summary>
    public static DiagnosticRecord ForRedialFailed(PeerId peer, SocketError error, long attemptNumber) =>
        new(DiagnosticId.RedialFailed, DiagnosticLevel.Info, DiagnosticSubjectKind.Peer, default, default, peer, (long)error, attemptNumber);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DeliveryCounterRefused"/> record for the Peer whose
    /// delivery-counter record claimed more Messages than this end ever sent it. <c>Value0</c> is the
    /// claimed cumulative count; <c>Value1</c> is the count of Messages this end has actually appended
    /// to this Peer's retention.
    /// </summary>
    public static DiagnosticRecord ForDeliveryCounterRefused(PeerId peer, long claimedCount, long appendedMessageCount) =>
        new(DiagnosticId.DeliveryCounterRefused, DiagnosticLevel.Warn, DiagnosticSubjectKind.Peer, default, default, peer, claimedCount, appendedMessageCount);
}
