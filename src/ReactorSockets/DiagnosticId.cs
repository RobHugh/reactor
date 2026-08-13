namespace ReactorSockets;

/// <summary>
/// A closed, permanent enumeration of diagnostic causes with explicit values in reserved blocks —
/// common 1-99, Raw 100-199, datagram session 200-299, stream session 300-399 (ADR 0013) — so a
/// future insertion cannot silently renumber an id a consumer has persisted. There is no
/// <c>None</c> or <c>Unknown</c> member and zero is never used: every <see cref="DiagnosticRecord"/>
/// has a cause by construction.
/// </summary>
public enum DiagnosticId
{
    /// <summary>
    /// The configured admission cap was hit this Pump with work still waiting to be admitted. No
    /// subject. <c>Value0</c> is the cap, <c>Value1</c> is the count of Admissions declined,
    /// counted as the Reactor's counters count them: once per declining where the work waits out of
    /// reach — a listening Link or bound endpoint with a pending connection it left in the OS
    /// backlog, never the number of connections left there, a depth unknowable without accepting —
    /// and once per piece of work where the Reactor was holding it, whether it then waits or is
    /// Discarded.
    /// </summary>
    AdmissionCapReached = 1,

    /// <summary>
    /// The frame arena had no room left for a request, regardless of the read-share escape.
    /// Subject is the Link that made the request. <c>Value0</c> is the bytes requested,
    /// <c>Value1</c> is the arena's bytes free at the moment of the request.
    /// </summary>
    ArenaExhausted = 2,

    /// <summary>
    /// Every capacity slot was occupied this Pump with work still waiting to be admitted — a
    /// different exhaustion from <see cref="AdmissionCapReached"/>, reachable because the admission
    /// cap can be configured above <see cref="ReactorOptions.Capacity"/>. No subject.
    /// <c>Value0</c> is the capacity, <c>Value1</c> is the count of Admissions declined, counted the
    /// same way and with the same limit as <see cref="AdmissionCapReached"/>'s.
    /// </summary>
    CapacityExhausted = 3,

    /// <summary>
    /// A registered Link failed and closed. Subject is the Link. <c>Value0</c> carries the
    /// platform <see cref="System.Net.Sockets.SocketError"/> as its underlying integer value;
    /// <c>Value1</c> is unused.
    /// </summary>
    LinkFailed = 100,

    /// <summary>
    /// An ICMP port-unreachable (or platform equivalent) was reported for a send. Subject is the
    /// Link. <c>Value0</c> carries the platform <see cref="System.Net.Sockets.SocketError"/> as its
    /// underlying integer value; <c>Value1</c> is unused.
    /// </summary>
    DestinationUnreachable = 101,

    /// <summary>
    /// A datagram exceeded the Reactor's scratch buffer and was truncated. Subject is the claimed
    /// Source. <c>Value0</c> is the scratch buffer size in bytes; <c>Value1</c> is unused.
    /// </summary>
    DatagramTruncated = 102,

    /// <summary>
    /// A datagram was discarded because nothing in it could be attributed to a Peer: wrong length,
    /// an unknown control type, a body the declared type cannot fill, padding that was not zero, a
    /// session id naming no Session, or a well-formed control reply — a Cookie, an Accept, a
    /// Refusal — matching no dial this Reactor is waiting on. That last case is not a parse failure,
    /// but it is the same thing to a consumer: bytes arrived, named nobody, and went nowhere.
    /// Subject is the claimed Source. <c>Value0</c> is the bytes received; <c>Value1</c> is unused.
    /// </summary>
    /// <remarks>
    /// A remote can produce this at will by sending rubbish, which is exactly why it is a Diagnostic
    /// and never an Event: the event stream is the consumer's protocol, and an unauthenticated
    /// stranger must not be able to write to it. <see cref="DiagnosticLevel.Warn"/> rather than
    /// <see cref="DiagnosticLevel.Error"/> for the same reason — nothing is wrong with this Reactor.
    /// </remarks>
    DatagramMalformed = 200,

    /// <summary>
    /// A Hello carried a cookie this Reactor did not mint, or minted too long ago: a forged or
    /// corrupted authenticator, a cookie replayed from a different source address, or one from an
    /// epoch older than the previous. Subject is the claimed Source. <c>Value0</c> is the epoch the
    /// cookie offered; <c>Value1</c> is the epoch current at the moment it was checked.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Warn"/>, never <see cref="DiagnosticLevel.Error"/>: a cookie
    /// arriving one epoch after it was minted is the mechanism working, and a Reactor visible to the
    /// internet will see forged ones continuously. Raising this to Error would train a consumer to
    /// ignore Error.
    /// </remarks>
    CookieRejected = 201,

    /// <summary>
    /// A cookie-validated Hello offered a protocol version this Reactor does not speak, and was
    /// refused. Subject is the claimed Source. <c>Value0</c> is the version offered; <c>Value1</c> is
    /// the version this Reactor requires.
    /// </summary>
    /// <remarks>
    /// Emitted only for a Hello that passed the cookie check, because that is the only Hello a
    /// Reactor answers at all — a bare Hello is answered with a Cookie and nothing else, whatever
    /// version it claimed.
    /// </remarks>
    VersionRefused = 202,

    /// <summary>
    /// A dialler was refused in-protocol during the stream handshake, either because it announced a
    /// different protocol version or because every capacity Slot was occupied. Subject is the claimed
    /// Source: at refusal time there is no Peer, only a claimed origin. For a capacity refusal,
    /// <c>Value0</c> is this Reactor's capacity and <c>Value1</c> is unused. For a version refusal,
    /// <c>Value0</c> is the version offered and <c>Value1</c> is the version this Reactor requires.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Info"/>, not <see cref="DiagnosticLevel.Warn"/>: a version mismatch
    /// on an upgrade and an ordinary burst at capacity are both routine, expected outcomes of running
    /// this profile, not conditions a consumer needs to act on.
    /// </remarks>
    PeerRefused = 300,

    /// <summary>
    /// A stream handshake completed and the two ends' announced Linger values were negotiated to their
    /// minimum (ADR 0011). Subject is the newly established Peer. <c>Value0</c> is this end's own
    /// Linger in milliseconds; <c>Value1</c> is the effective (minimum) Linger in milliseconds.
    /// </summary>
    LingerNegotiated = 301,

    /// <summary>
    /// An established Peer's Message framing broke this profile's rules and the Session was closed
    /// with <see cref="DisconnectReason.ProtocolViolation"/>: an oversized or zero-length prefix, or a
    /// stream that ended before a prefix or the Message body it named had fully arrived. Subject is
    /// the Peer. <c>Value0</c> is the offending length prefix — 0 for a truncation, where no full
    /// prefix was ever read; <c>Value1</c> is this Reactor's own <c>MaxMessageSize</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Warn"/>: a remote can produce this at will by sending malformed
    /// bytes, which is exactly why the Session ends rather than throws, and why this is a Diagnostic
    /// rather than an Event on the consumer's own stream.
    /// </remarks>
    MalformedFraming = 302,

    /// <summary>
    /// An established Peer sent Message bytes after its own Goodbye had already been received, and
    /// the Session was closed with <see cref="DisconnectReason.ProtocolViolation"/> (issue #93).
    /// Subject is the Peer. Carries no claimed length — <see cref="MessageFramingLegality"/> reaches
    /// this outcome before ever reading a prefix, so there is nothing to report but the contradiction
    /// itself. <c>Value0</c> and <c>Value1</c> are unused.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Warn"/>, for the same reason as <see cref="MalformedFraming"/>: a
    /// remote can produce this at will by ignoring the Goodbye it just sent.
    /// </remarks>
    DataAfterGoodbye = 303,

    /// <summary>
    /// An established Peer's delivery-counter record (issue #109) claimed to have received more
    /// Messages than this end has ever sent it, and the Session was closed with
    /// <see cref="DisconnectReason.ProtocolViolation"/> (issue #113) — the peer is confirming Messages
    /// this end never queued, rather than merely repeating a count it already reported. Subject is the
    /// Peer. <c>Value0</c> is the claimed cumulative count; <c>Value1</c> is the count of Messages (and,
    /// if this end's own Goodbye has already been queued for this Peer, that too) this end has actually
    /// appended to this Peer's retention — the largest count a legitimate confirmation could ever sit
    /// at or below.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="DiagnosticLevel.Warn"/>, for the same reason as <see cref="MalformedFraming"/>: a
    /// remote can produce this at will by inflating its own counter.
    /// </para>
    /// <para>
    /// Numbered 308 rather than the next free 304: 304-307 are already spoken for by the rest of slice 4
    /// (ResumeMatched, ResumeUnmatched, RedialAttempted, RedialFailed), and a value published in a
    /// closed permanent enum cannot be moved later to make room.
    /// </para>
    /// </remarks>
    DeliveryCounterRefused = 308,
}
