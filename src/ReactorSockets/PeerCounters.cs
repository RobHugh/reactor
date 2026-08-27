namespace ReactorSockets;

/// <summary>
/// A point-in-time snapshot of one Peer's counters (ADR 0013): monotonic figures running from that
/// Peer's establishment, with no reset, fetched by value and keyed by <see cref="PeerId"/> — mirrors
/// <see cref="LinkCounters"/>'s Raw-profile shape and the same "no accessor enumerates every Peer's
/// counters in one call" rule.
/// </summary>
/// <param name="MessagesReceived">
/// Whole Messages handed to the frame arena for delivery — incremented at that point only, never
/// when a Message's bytes finish arriving or while it sits buffered waiting for a read share or
/// arena room (subtask 3.4.4, issue #82). This is what makes a share exhaustion visible: a Frame in
/// which bytes plainly arrived but this figure did not move is a Message still buffered, not one
/// dropped.
/// </param>
/// <param name="RetentionHighWaterBytes">
/// The most bytes this Peer's retention ring has ever held at once (written minus released) since
/// the Peer was established — <see cref="Core.RetentionArena.HighWaterBytes(int)"/> read fresh at
/// snapshot time, never pushed. Monotonic for the Peer's lifetime: draining the ring after a burst
/// lowers what is currently retained but never lowers this figure (subtask 3.5.5, issue #87).
/// </param>
/// <param name="Resumes">
/// How many times this Peer's transport has been successfully resumed after an Interruption (issue
/// #128, ADR 0013). Incremented once per <see cref="SessionEventKind.PeerResumed"/> raised for this
/// Peer, on both the listening and dialling sides, and never on a refused resume. Earns its own
/// member rather than being derived by the consumer counting <see cref="SessionEventKind.PeerResumed"/>
/// Events: a Session profile drains Events into whatever cadence the consumer chooses, so counting
/// them back out is not generally possible once drained, while this figure is a Counter — read fresh
/// by value at any time, the same as every other figure here. Monotonic and never reset: a resume
/// carries no counter's history away with it, so a retransmission count spanning several resumes is
/// how a flapping path shows itself instead of being hidden by a zeroing on every reconnect.
/// </param>
/// <param name="OutOfWindowPackets">
/// How many packet records this Peer's per-Space receive window has classified as out of window —
/// a sequence number too far ahead of, or behind, that Space's receive window base
/// to be tracked against it (subtask 6.2.1, issue #168; ADR 0013). Recurs every Pump under sustained
/// loss or reordering, which is exactly why it is a Counter and never a Diagnostic. Incremented by
/// the parse loop's classification of each arriving record (issue #172); a freshly connected Peer
/// reads zero. Note that only the two reliable Spaces raise this on a far-ahead arrival: the
/// unreliable Spaces slide their window forward instead and count the retirement as loss (issue
/// #173), so for those Spaces out-of-window means a sequence below the base alone.
/// </param>
/// <param name="DuplicatePackets">
/// How many packet records this Peer's per-Space receive window has classified as duplicates — a
/// sequence number already accounted for, most often a retransmission the far end sent because it
/// never saw this end's acknowledgement rather than a protocol violation (subtask 6.2.1, issue #168;
/// ADR 0013). Recurs every Pump under sustained loss exactly like <see cref="OutOfWindowPackets"/>,
/// and for the same reason is a Counter. Incremented by the parse loop's classification of each
/// arriving record (issue #172); a freshly connected Peer reads zero.
/// The plain <c>Unreliable</c> Space contributes from a second place as well (issue #222, ADR 0017):
/// that Space suppresses repeats within its own bounded 64-sequence window, a protocol constant rather
/// than the receive window, and a repeat that window catches after the receive window <em>admitted</em>
/// it raises this figure. A repeat the receive window itself refused is counted there instead — most
/// often on <see cref="OutOfWindowPackets"/>, since a repeat usually arrives below a base that has
/// already passed it — so one record raises one figure and never both. A repeat delayed past those 64
/// sequences is delivered and raises neither: suppression there is bounded by design, and these
/// figures count what was refused rather than what was repeated.
/// </param>
/// <param name="UnreliableLossPackets">
/// How many sequences an unreliable Space's receive window has lost — retired by sliding its base
/// forward before that sequence ever arrived (issues #173, #176; ADR 0013). A gap is provisional until
/// the window passes it by: a sequence still inside the window may yet arrive late and be admitted
/// without ever touching this figure, so it reflects only sequences a window has moved past, never
/// every sequence not yet seen. This is one figure covering both unreliable Spaces —
/// <c>Unreliable</c> and <c>UnreliableSequenced</c> — summed, exactly as
/// <see cref="OutOfWindowPackets"/> and <see cref="DuplicatePackets"/> already cover all four Spaces
/// summed, so per-Space loss is not separable from it. The two reliable Spaces never contribute,
/// because their windows do not slide: retransmission is what fills their holes, and what they refuse
/// lands on <see cref="OutOfWindowPackets"/> instead. No Diagnostic accompanies a gap in either Space;
/// a lossy path must not fill the Sink with volume. Monotonic and never reset, exactly like
/// <see cref="OutOfWindowPackets"/>; a freshly connected Peer reads zero.
/// </param>
/// <param name="HorizonDeferrals">
/// How many sequences arrived beyond a reliable Space's ack horizon and so had their retransmit timer
/// withheld rather than started (subtask 6.3, issue #41; ADR 0013). It measures a deferral, not a
/// loss: the sequence is not missing and nothing was discarded, only judged too far ahead of
/// <c>[base, base + 32]</c> for this Pump's ack blocks to describe yet, so the timer that would otherwise
/// start ticking toward a spurious retransmission is withheld until the horizon slides over it. This
/// is why it is a Counter and not a Diagnostic (ADR 0013): sustained load with many sequences in
/// flight defers on every Pump, which fails the record test, and it keeps its own identity — rather
/// than folding into <see cref="OutOfWindowPackets"/> — because counters have a per-Peer scope. Only
/// the two reliable Spaces contribute; the unreliable Spaces have no ack horizon to sit beyond. Rises
/// only under the horizon scenario (issue #185) and stays zero in ordinary operation, where every
/// sequence lands inside the horizon as it arrives. Monotonic and never reset, exactly like
/// <see cref="OutOfWindowPackets"/>: a Resume carries no counter's history away with it, and this one's
/// lifetime is the Peer's, not the transport's. A freshly connected Peer reads zero.
/// </param>
/// <param name="PacketsRetransmitted">
/// How many packet records this Peer has resent (subtask 6.4.6, issue #196; ADR 0013). Counts packet
/// RECORDS resent, not datagrams sent and not Messages: retransmission is byte-identical at packet-
/// record granularity, because a datagram is repacked on every send and is not itself reproducible —
/// several records can ride one datagram, and the same record can ride a different datagram on retry.
/// This is why it is a Counter and not a Diagnostic (ADR 0013): a retransmit recurs continuously on a
/// lossy path, which fails the record test, and it keeps its own identity — rather than folding into
/// <see cref="OutOfWindowPackets"/> or <see cref="HorizonDeferrals"/> — because counters have a
/// per-Peer scope. Monotonic and never reset, exactly like <see cref="HorizonDeferrals"/>: its lifetime
/// is the Peer's, not the transport's, so a Resume does not zero it. Nothing increments this yet — the
/// expiry path that does is issue #198 — so a freshly connected Peer reads zero.
/// </param>
/// <param name="BytesSent">
/// The datagram bytes a flush has handed the transport for this Peer, header included (subtask 6.7.4,
/// issue #231; ADR 0013). Incremented once per Datagram, at the size the coalescing packer actually
/// wrote for it — the 20-byte session header plus every packet record's own header and payload —
/// rather than per record or per payload byte, since header bytes amortising across a Pump's coalesced
/// records is exactly what this figure exists to let a consumer see; counting payload alone would hide
/// that amortisation. Raised regardless of whether the underlying send call reports success, since the
/// bytes were handed to the transport at that point either way — distinct from the Reactor-scope send
/// figure, which counts only a confirmed success. Only a data Datagram to an Established Peer moves
/// this: a handshake's control datagrams carry no session id yet to attribute them to, so they are
/// never counted here, on either the sent or the received side (see <see cref="BytesReceived"/>).
/// Monotonic and never reset, exactly like <see cref="HorizonDeferrals"/>: its lifetime is the Peer's,
/// not the transport's, so a Resume does not zero it. A freshly connected Peer reads zero.
/// </param>
/// <param name="BytesReceived">
/// The datagram bytes attributed to this Peer on arrival (subtask 6.7.4, issue #231; ADR 0013).
/// Incremented once per data Datagram that both names an established Peer's session id and proves
/// well-formed — the same proof that gates ack application and record classification — at the whole
/// Datagram's length, header included, mirroring <see cref="BytesSent"/>. A Datagram naming no
/// Session, or one so malformed it cannot be attributed to the Peer it claims, charges no Peer: it
/// lands on the Reactor-scope unknown-source figure instead, the same bucket a genuinely unknown
/// source falls into. A handshake's control datagrams are never counted here either, for the same
/// reason as <see cref="BytesSent"/>: at the point one arrives, no Peer's session id has been assigned
/// yet for it to name. Monotonic and never reset, exactly like <see cref="HorizonDeferrals"/>: its
/// lifetime is the Peer's, not the transport's, so a Resume does not zero it. A freshly connected Peer
/// reads zero.
/// </param>
public readonly record struct PeerCounters(
    ulong MessagesReceived,
    ulong RetentionHighWaterBytes,
    ulong Resumes,
    ulong OutOfWindowPackets,
    ulong DuplicatePackets,
    ulong UnreliableLossPackets,
    ulong HorizonDeferrals,
    ulong PacketsRetransmitted,
    ulong BytesSent,
    ulong BytesReceived);
