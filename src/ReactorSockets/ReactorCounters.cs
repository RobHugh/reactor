namespace ReactorSockets;

/// <summary>
/// A point-in-time snapshot of the Reactor-scope counters (ADR 0013): monotonic figures running
/// from construction, with no reset. Fetched by value so nothing lets a consumer hold a pointer
/// into live state across a Pump; diffing two snapshots gives everything a reset would, from state
/// the consumer already owns.
/// </summary>
/// <remarks>
/// The shape is shared by every profile; which members move is not. <c>DatagramsSent</c> and
/// <c>DatagramsReceived</c> stay at zero on a Reactor carrying no datagram transport, and the two
/// admission counters divide the same way. A member that never moves on a profile is reporting a
/// true zero, not a gap.
/// </remarks>
/// <param name="SinkFaulted">Times the installed Sink threw and was disabled for the Reactor's life.</param>
/// <param name="DatagramsSent">Datagrams handed to the transport for sending.</param>
/// <param name="DatagramsReceived">Datagrams taken from the transport.</param>
/// <param name="ArenaHighWaterBytes">The most Arena bytes any single Frame has used.</param>
/// <param name="AdmissionDeferrals">
/// Admissions declined that left the work where it was, for a later Pump to take. Moves on the
/// transports where declined work waits — Raw and the stream Session profile. Counted once per
/// declining rather than once per piece of work turned away, which makes it coarse where the work
/// sits in an OS backlog: one listening Link or bound endpoint declines its whole backlog with one
/// declining, because that depth is unknowable without accepting. Where the Reactor already holds
/// the work — a stream Session handshake accepted but not completed — one declining is one piece of
/// work and the figure is exact. Read the magnitude as "how much of the listening fleet is turning
/// work away", never as backlog depth.
/// </param>
/// <param name="AdmissionDiscards">
/// Admissions declined that let the work go, because the transport gave it nowhere to wait. Moves
/// only on the datagram Session profile, which Discards the Hellos it cannot admit. Always an exact
/// count of work turned away, which <paramref name="AdmissionDeferrals"/> is not, because a Reactor
/// can only Discard what it had in hand. Kept apart from Deferrals because the two differ in the
/// one way a consumer would act on: deferred work arrives on a later Pump, Discarded work never
/// does.
/// </param>
/// <param name="UnknownSourceDiscards">
/// Datagrams taken from the transport and thrown away because nothing could be attributed to a Peer:
/// a malformed control datagram, or a session id naming no Session. Reactor-scope rather than
/// per-Peer precisely because there is no Peer to charge them to — the whole point of the member is
/// the traffic that failed to name one. Moves only on the datagram Session profile. A rising figure
/// on a Reactor that is otherwise healthy is a scan or a flood, not a fault, which is why the
/// matching Diagnostics are <see cref="DiagnosticLevel.Warn"/>.
/// </param>
public readonly record struct ReactorCounters(
    ulong SinkFaulted,
    ulong DatagramsSent,
    ulong DatagramsReceived,
    ulong ArenaHighWaterBytes,
    ulong AdmissionDeferrals,
    ulong AdmissionDiscards,
    ulong UnknownSourceDiscards);
