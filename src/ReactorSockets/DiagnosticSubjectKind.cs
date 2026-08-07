namespace ReactorSockets;

/// <summary>
/// Which field of a <see cref="DiagnosticRecord"/>'s tagged subject is populated (ADR 0013). A
/// record has exactly one subject, so a Sink or formatter switches on this rather than checking
/// several optional fields for which one is set.
/// </summary>
public enum DiagnosticSubjectKind
{
    /// <summary>The record concerns no particular Link or Source.</summary>
    None,

    /// <summary>The record concerns a registered Link, carried in <see cref="DiagnosticRecord.Link"/>.</summary>
    Link,

    /// <summary>
    /// The record concerns an unauthenticated claimed origin, carried in
    /// <see cref="DiagnosticRecord.Source"/>.
    /// </summary>
    Source,

    /// <summary>
    /// The record concerns an established Peer, carried in <see cref="DiagnosticRecord.Peer"/>.
    /// Distinct from <see cref="Source"/>: a Peer exists only once a handshake has completed, while a
    /// Source is merely a claimed origin — a refusal made before a Peer exists tags <see cref="Source"/>,
    /// never this.
    /// </summary>
    Peer,
}
