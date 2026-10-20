namespace ReactorSockets;

/// <summary>
/// A closed, permanent enumeration of diagnostic causes with explicit values in reserved blocks —
/// common 1-99, Raw 100-199, datagram session 200-299, stream session 300-399 (ADR 0013) — so a
/// future insertion cannot silently renumber an id a consumer has persisted. There is no
/// <c>None</c> or <c>Unknown</c> member and zero is never used: every <see cref="DiagnosticRecord"/>
/// has a cause by construction.
/// </summary>
/// <remarks>
/// <para>
/// <b>Supported Sink style: range-check the blocks.</b> The enum grows inside its blocks, so a Sink
/// that classifies an id by the block it sits in, or that switches on members it cares about with a
/// default arm for the rest, keeps working when a member is added. A Sink whose <c>switch</c> assumes
/// it lists every member is not supported: the next additive release breaks it.
/// </para>
/// <para>
/// <b>A block names the profile that defined a condition, not the profile that emitted a record</b>
/// (ADR 0020). <see cref="LingerNegotiated"/>, <see cref="ResumeMatched"/> and
/// <see cref="ResumeUnmatched"/> sit in the stream block and are emitted by the datagram session
/// profile too, because each is one condition and a member names a condition. A Raw-only Sink's
/// range check over 100-199 is exact, since nothing outside Raw shares that block. A Sink that needs
/// to know which profile emitted a record knows which Reactor it was given to.
/// </para>
/// <para>
/// Each member's documentation is the authority on its subject and value slots. ADR 0013's roster table
/// is the one place to check them side by side and is kept in step with these docs.
/// </para>
/// </remarks>
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
    /// Subject is the Link that made the request on Raw, and the Peer whose Message could not be
    /// reserved on either session profile (ADR 0014). <c>Value0</c> is the bytes requested,
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
    /// A Fragment set for an established Peer was abandoned before it ever completed. Subject is the
    /// Peer, not the claimed Source: an incomplete set belongs to the Session that was receiving it,
    /// not to a bare source address. <c>Value0</c> is the number of fragments actually held for the
    /// set at the moment it was abandoned; <c>Value1</c> is the number the set's own metadata
    /// declared it would need. Held against expected is the pair a consumer can act on — one of forty
    /// held is a path dropping almost everything of that size, thirty-nine of forty is ordinary loss
    /// on a set that was nearly there, and the two want different responses.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Warn"/>, not <see cref="DiagnosticLevel.Error"/>: nothing local is
    /// broken, an abandoned set is loss on a lossy transport working as designed, and the level's job
    /// is to answer whether a consumer wants to see this, not whether this Reactor is at fault.
    /// </remarks>
    FragmentSetAbandoned = 203,

    /// <summary>
    /// This Peer's <see cref="RetransmissionTimer"/> backed off: a Pump's expiry sweep
    /// (<see cref="Session.OutstandingReliableSequences.CollectExpired"/>, subtask 6.4.8, issue #198)
    /// found at least one overdue sequence on either reliable Space, so <see cref="RetransmissionTimer.BackOff"/>
    /// was called once for the Peer and every sequence the sweep reported re-armed at the single
    /// resulting RTO. Subject is the Peer. <c>Value0</c> is the new RTO in milliseconds —
    /// <see cref="RetransmissionTimer.EffectiveTimeout"/> at the moment of the backoff; <c>Value1</c> is
    /// <see cref="RetransmissionTimer.ConsecutiveBackoffs"/> after this backoff.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Trace"/>, not <see cref="DiagnosticLevel.Warn"/>: this is protocol
    /// machinery working correctly, not a fault. A lossy path emits it continuously — every Pump that
    /// finds an overdue sequence backs off again — and raising it above <c>Trace</c> would train a
    /// consumer to ignore the level it sits at (subtask 6.4.9, issue #199).
    /// </remarks>
    RetransmissionTimerBackedOff = 204,

    /// <summary>
    /// An established Peer's Fragment declared a <c>fragmentCount</c> larger than this Reactor's own
    /// receive window could ever hold, and the Session was closed with
    /// <see cref="DisconnectReason.ProtocolViolation"/> on that Fragment's first appearance, before any
    /// receive-window slot or Fragment-group bookkeeping was created for its set (issue #282, reworked
    /// under #264). Subject is the Peer. <c>Value0</c> is the declared <c>fragmentCount</c>; <c>Value1</c>
    /// is the largest legal count this Reactor's <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>
    /// allows.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Warn"/>, for the same reason as <see cref="DatagramMalformed"/>: a
    /// remote can produce this at will by lying about how many Fragments a Message needs.
    /// </remarks>
    OversizedFragmentSet = 205,

    /// <summary>
    /// A packet record carried the fragment shape in one of the two unreliable modes, which ADR 0004
    /// forbids — you may only fragment what you can retransmit — and the read failed outright, taking
    /// the whole Datagram with it (issue #273). Subject is the claimed Source, not the Peer, matching
    /// the <see cref="DatagramMalformed"/> that is raised alongside it: a discarded Datagram is never
    /// attributed to the Session it named, and no byte of it is counted against that Peer.
    /// <c>Value0</c> is the mode the record claimed, as the two mode bits' wire value — 0 for
    /// <c>Unreliable</c>, 1 for <c>UnreliableSequenced</c>; <c>Value1</c> is unused.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="DiagnosticLevel.Warn"/>, for the same reason as <see cref="DatagramMalformed"/>: a
    /// remote can produce this at will, and nothing is wrong with this Reactor.
    /// </para>
    /// <para>
    /// Distinct from <see cref="DatagramMalformed"/> rather than folded into it because the two say
    /// different things to a consumer. A malformed Datagram is rubbish from anywhere; this one is
    /// well-formed at every level but the rule, which means the sender is running a build that
    /// fragments in modes this profile does not — a misconfiguration a consumer can act on, and the
    /// send path already refuses to produce.
    /// </para>
    /// </remarks>
    UnreliableFragment = 206,

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
    /// A Session established — on either profile, stream or datagram — and the two ends' announced
    /// Linger values were negotiated to their minimum (ADR 0011). Subject is the newly established
    /// Peer. <c>Value0</c> is this end's own Linger in milliseconds; <c>Value1</c> is the effective
    /// (minimum) Linger in milliseconds.
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
    /// Subject is the Peer. Carries no claimed length — <see cref="Session.MessageFramingLegality"/> reaches
    /// this outcome before ever reading a prefix, so there is nothing to report but the contradiction
    /// itself. <c>Value0</c> and <c>Value1</c> are unused.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Warn"/>, for the same reason as <see cref="MalformedFraming"/>: a
    /// remote can produce this at will by ignoring the Goodbye it just sent.
    /// </remarks>
    DataAfterGoodbye = 303,

    /// <summary>
    /// A connection presenting a valid resume secret was matched onto the Interrupted Peer whose Slot
    /// was reserving the Session it named, and that Session continued over the replacement transport
    /// rather than a new one being established (issue #123, ADR 0011). Subject is the resumed Peer —
    /// the same <see cref="PeerId"/>, generation included, the Session had before the interruption,
    /// since nothing about a Peer's identity is renewed by a Resume. <c>Value0</c> is the number of
    /// retained outbound bytes the match rewound to the counter the Resume reported; <c>Value1</c> is
    /// unused. On the datagram profile it is logged when a Resume control datagram matches the Peer on
    /// the accepting end (session id and secret), and when an Interrupted dialling end that has sent a
    /// Resume hears its session again. Nothing is rewound, because retransmission already re-sends
    /// anything unacknowledged, so <c>Value0</c> is 0 and <c>Value1</c> is unused.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="DiagnosticLevel.Info"/>: a reconnection is the outcome reserving a Slot exists to
    /// produce, not a condition a consumer needs to act on.
    /// </para>
    /// <para>
    /// Shared with the datagram session profile rather than duplicated in the datagram block, as are
    /// <see cref="LingerNegotiated"/> and <see cref="ResumeUnmatched"/> (ADR 0020).
    /// </para>
    /// <para>
    /// Numbered 304 rather than the 302 the slice-4 spec named: 302 and 303 were taken by
    /// <see cref="MalformedFraming"/> and <see cref="DataAfterGoodbye"/> (issues #92 and #106) after
    /// that spec was written, and a value published in a closed permanent enum is never renumbered.
    /// </para>
    /// </remarks>
    ResumeMatched = 304,

    /// <summary>
    /// A connection claiming an existing Session was refused rather than resumed. Subject is the
    /// claimed Source: a claim that is not matched comes from a party that is by construction not a
    /// Peer, so there is no Peer to name, only a claimed origin. <c>Value0</c> is the number of
    /// Interrupted Peers whose reservations the claim was checked against — 0 means nothing was being
    /// held at all, which is the reconfiguration path where a once-valid secret arrives after its
    /// Linger expired. <c>Value1</c> is the wire record type the claim arrived as: 3 for a Resume
    /// whose secret matched no held Peer, and 1 for a Hello presenting a secret that did match, which
    /// re-announces the parameters a Resume has no field for and is refused as a protocol violation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="DiagnosticLevel.Warn"/> rather than the <see cref="DiagnosticLevel.Info"/> a version
    /// mismatch or a capacity refusal carries: those are routine outcomes of running this profile and
    /// a wrong secret is not.
    /// </para>
    /// <para>
    /// "Unmatched" names the outcome, not the comparison: the claim was not matched onto a Session.
    /// The Hello case reaches here having found the secret it announced, and is refused anyway because
    /// the record it arrived in re-announces parameters — <c>Value1</c> is what tells the two apart.
    /// </para>
    /// <para>
    /// <c>Value0</c> is reported to this end's own Sink and never to the wire: a refused party is
    /// closed on without being told anything, so it learns neither the count nor which of the three
    /// refusals it met.
    /// </para>
    /// <para>
    /// The datagram profile reports a Resume control datagram the same way, with <c>Value1</c> 6 (the
    /// Resume control type, <c>DatagramControlType.Resume</c>). <c>Value0</c> is 0 when the target
    /// session id names no Peer the Reactor holds, or names a dialled Peer (never a Resume target, since only
    /// the dialling end sends Resume), and 1 when it names a Peer but the presented secret
    /// does not equal the one that Peer issued.
    /// </para>
    /// </remarks>
    ResumeUnmatched = 305,

    /// <summary>
    /// A dialling-side Peer began a fresh re-dial attempt after its transport was lost (issue #125,
    /// ADR 0011). Subject is the Peer. <c>Value0</c> is the attempt number, starting at 1 and rising by
    /// one per attempt; <c>Value1</c> is the delay in milliseconds this attempt actually waited before
    /// dialling — the jittered figure <see cref="Session.RedialSchedule.NextDelay"/> drew, so
    /// what actually elapsed is observable rather than only the computed figure before jitter.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Info"/>: a re-dial is the ordinary behaviour of a dialling Peer that
    /// lost its transport, not a condition a consumer needs to act on.
    /// </remarks>
    RedialAttempted = 306,

    /// <summary>
    /// A re-dial attempt could not connect (issue #125, ADR 0011). Subject is the Peer, which this
    /// never closes — it stays Interrupted and waits out the schedule's next delay, leaving Linger,
    /// not an attempt count, the only thing that ever closes it. <c>Value0</c> carries the platform
    /// <see cref="System.Net.Sockets.SocketError"/> as its underlying integer value; <c>Value1</c> is
    /// the attempt number that failed.
    /// </summary>
    /// <remarks>
    /// <see cref="DiagnosticLevel.Info"/> rather than <see cref="DiagnosticLevel.Warn"/>: under this
    /// profile's premise a failed reconnection attempt is an ordinary, expected outcome, and raising it
    /// to Warn would train a consumer to ignore Warn.
    /// </remarks>
    RedialFailed = 307,

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
    /// Numbered 308 rather than the next free 304: ADR 0013's roster, as amended for this slice, reserves
    /// 304-307 for the rest of slice 4 (<see cref="ResumeMatched"/> and <see cref="ResumeUnmatched"/>,
    /// which issue #124 has since taken, then RedialAttempted and RedialFailed), and a value published
    /// in a closed permanent enum cannot be moved later to make room.
    /// </para>
    /// </remarks>
    DeliveryCounterRefused = 308,
}
