using System.Net.Sockets;

namespace ReactorSockets;

/// <summary>
/// A single diagnostic emitted to an <see cref="IReactorDiagnostics"/> Sink: a closed
/// <see cref="DiagnosticId"/>, its fixed <see cref="DiagnosticLevel"/>, a tagged subject — none,
/// Link or Source (ADR 0013) — and two numeric slots whose meaning is documented per
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
        long value0,
        long value1)
    {
        Id = id;
        Level = level;
        SubjectKind = subjectKind;
        Link = link;
        Source = source;
        Value0 = value0;
        Value1 = value1;
    }

    /// <summary>Which condition produced this record.</summary>
    public DiagnosticId Id { get; }

    /// <summary>The fixed severity for <see cref="Id"/>, per ADR 0013's roster.</summary>
    public DiagnosticLevel Level { get; }

    /// <summary>Which of <see cref="Link"/> or <see cref="Source"/>, if either, is populated.</summary>
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
        new(DiagnosticId.AdmissionCapReached, DiagnosticLevel.Warn, DiagnosticSubjectKind.None, default, default, cap, declined);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.CapacityExhausted"/> record. No subject.
    /// <paramref name="declined"/> is counted exactly as
    /// <see cref="ForAdmissionCapReached"/>'s is.
    /// </summary>
    public static DiagnosticRecord ForCapacityExhausted(int capacity, int declined) =>
        new(DiagnosticId.CapacityExhausted, DiagnosticLevel.Warn, DiagnosticSubjectKind.None, default, default, capacity, declined);

    /// <summary>
    /// Builds an <see cref="DiagnosticId.ArenaExhausted"/> record for the Link that made the request.
    /// </summary>
    public static DiagnosticRecord ForArenaExhausted(LinkId link, int bytesRequested, int bytesFree) =>
        new(DiagnosticId.ArenaExhausted, DiagnosticLevel.Warn, DiagnosticSubjectKind.Link, link, default, bytesRequested, bytesFree);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.LinkFailed"/> record carrying the platform
    /// <see cref="SocketError"/> that caused the failure.
    /// </summary>
    public static DiagnosticRecord ForLinkFailed(LinkId link, SocketError error) =>
        new(DiagnosticId.LinkFailed, DiagnosticLevel.Warn, DiagnosticSubjectKind.Link, link, default, (long)error, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DestinationUnreachable"/> record carrying the platform
    /// <see cref="SocketError"/> reported for the send.
    /// </summary>
    public static DiagnosticRecord ForDestinationUnreachable(LinkId link, SocketError error) =>
        new(DiagnosticId.DestinationUnreachable, DiagnosticLevel.Warn, DiagnosticSubjectKind.Link, link, default, (long)error, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DatagramTruncated"/> record for the claimed Source, carrying
    /// the scratch buffer size the datagram was truncated to.
    /// </summary>
    public static DiagnosticRecord ForDatagramTruncated(Source source, int bufferSize) =>
        new(DiagnosticId.DatagramTruncated, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, bufferSize, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.DatagramMalformed"/> record for the claimed Source, carrying
    /// the number of bytes that arrived.
    /// </summary>
    public static DiagnosticRecord ForDatagramMalformed(Source source, int bytesReceived) =>
        new(DiagnosticId.DatagramMalformed, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, bytesReceived, 0);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.CookieRejected"/> record for the claimed Source, carrying the
    /// epoch the cookie offered and the epoch current when it was checked.
    /// </summary>
    public static DiagnosticRecord ForCookieRejected(Source source, long epochOffered, long epochCurrent) =>
        new(DiagnosticId.CookieRejected, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, epochOffered, epochCurrent);

    /// <summary>
    /// Builds a <see cref="DiagnosticId.VersionRefused"/> record for the claimed Source, carrying the
    /// version offered and the version this Reactor requires.
    /// </summary>
    public static DiagnosticRecord ForVersionRefused(Source source, long versionOffered, long versionRequired) =>
        new(DiagnosticId.VersionRefused, DiagnosticLevel.Warn, DiagnosticSubjectKind.Source, default, source, versionOffered, versionRequired);
}
