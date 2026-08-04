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
}
