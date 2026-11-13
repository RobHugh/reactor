using System.Globalization;
using System.Net.Sockets;

namespace ReactorSockets;

/// <summary>
/// The one authoritative rendering of a <see cref="DiagnosticRecord"/> to text (ADR 0005, ADR 0013),
/// so a structured Sink consumes the typed fields and never formats, and a text Sink gets this
/// library's wording rather than inventing its own. Holds no state: nothing is allocated until
/// <see cref="Format"/> is called, and a consumer who never formats pays nothing.
/// </summary>
/// <remarks>
/// <para>
/// The output is permanent public surface — consumers grep it, alert on it and paste it into issues —
/// and its shape is <c>{Level} {Id} ({number}), {subject}: {what happened} ({value slots})</c>, the
/// subject segment omitted when the record has none. Numbers are rendered in the invariant culture.
/// </para>
/// <para>
/// A Source is rendered as a claim — <c>claimed source 203.0.113.7:4242 (unauthenticated)</c> — never
/// as a fact like "from 203.0.113.7:4242". A Source is a hint about origin that an attacker chooses,
/// and the wording must not invite a consumer to firewall on it (<c>CONTEXT.md</c>, ADR 0013).
/// </para>
/// </remarks>
public static class DiagnosticFormatter
{
    /// <summary>
    /// Renders <paramref name="record"/> to text, leaving the record untouched. Throws
    /// <see cref="ArgumentOutOfRangeException"/> for an id or subject kind outside the roster, which
    /// only a <see langword="default"/> or hand-forged record can carry — every record the library
    /// builds has a cause by construction.
    /// </summary>
    public static string Format(in DiagnosticRecord record)
    {
        string subject = FormatSubject(record);
        string body = FormatBody(record);
        return Invariant(
            $"{record.Level} {record.Id} ({(int)record.Id}){subject}: {body}");
    }

    /// <summary>
    /// The subject segment, including its leading separator, or empty for a record with no subject.
    /// One switch over the tagged subject (ADR 0013).
    /// </summary>
    private static string FormatSubject(in DiagnosticRecord record) => record.SubjectKind switch
    {
        DiagnosticSubjectKind.None => string.Empty,
        DiagnosticSubjectKind.Link => Invariant(
            $", link slot {record.Link.Index} generation {record.Link.Generation} of reactor {record.Link.Origin}"),
        DiagnosticSubjectKind.Peer => Invariant(
            $", peer slot {record.Peer.Index} generation {record.Peer.Generation} of reactor {record.Peer.Origin}"),
        DiagnosticSubjectKind.Source => Invariant($", claimed source {record.Source.ToIPEndPoint()} (unauthenticated)"),
        _ => throw new ArgumentOutOfRangeException(
            nameof(record), record.SubjectKind, $"{record.SubjectKind} is not a recognised DiagnosticSubjectKind."),
    };

    /// <summary>
    /// What happened and the value slots, worded per member with the meanings documented on each
    /// <see cref="DiagnosticId"/> member.
    /// </summary>
    private static string FormatBody(in DiagnosticRecord record)
    {
        long v0 = record.Value0;
        long v1 = record.Value1;
        return record.Id switch
        {
            DiagnosticId.AdmissionCapReached => Invariant(
                $"the admission cap was reached with work still waiting (cap {v0}, admissions declined {v1})"),
            DiagnosticId.ArenaExhausted => Invariant(
                $"the frame arena had no room for a request (bytes requested {v0}, bytes free {v1})"),
            DiagnosticId.CapacityExhausted => Invariant(
                $"every capacity slot was occupied with work still waiting (capacity {v0}, admissions declined {v1})"),
            DiagnosticId.LinkFailed => Invariant(
                $"the link failed and closed (socket error {SocketErrorText(v0)})"),
            DiagnosticId.DestinationUnreachable => Invariant(
                $"a send was reported unreachable (socket error {SocketErrorText(v0)})"),
            DiagnosticId.DatagramTruncated => Invariant(
                $"a datagram was truncated to the scratch buffer (buffer bytes {v0})"),
            DiagnosticId.DatagramMalformed => Invariant(
                $"a datagram naming no Peer was discarded (bytes received {v0})"),
            DiagnosticId.CookieRejected => Invariant(
                $"a Hello's cookie was rejected (epoch offered {v0}, epoch current {v1})"),
            DiagnosticId.VersionRefused => Invariant(
                $"a Hello offering an unsupported protocol version was refused (version offered {v0}, version required {v1})"),
            DiagnosticId.FragmentSetAbandoned => Invariant(
                $"an incomplete fragment set was abandoned (fragments held {v0}, fragments expected {v1})"),
            DiagnosticId.RetransmissionTimerBackedOff => Invariant(
                $"the retransmission timer backed off (RTO {v0} ms, consecutive backoffs {v1})"),
            DiagnosticId.OversizedFragmentSet => Invariant(
                $"a fragment set declared more fragments than the receive window can hold and the Session was closed (fragments declared {v0}, largest legal {v1})"),
            DiagnosticId.OversizedFragment => Invariant(
                $"a fragment wider than FragmentSize arrived and the Session was closed (fragment bytes {v0}, FragmentSize {v1})"),
            DiagnosticId.UnreliableFragment => Invariant(
                $"a datagram fragmenting in an unreliable mode was discarded (mode {ModeName(v0)}, mode bits {v0})"),
            DiagnosticId.OversizedMessage => Invariant(
                $"a Message larger than MaxMessageSize was discarded (bytes {v0}, MaxMessageSize {v1})"),
            DiagnosticId.AddressPeerLimitReached => Invariant(
                $"a handshake was refused because its address holds as many Peers as it may (MaxPeersPerAddress {v0}, counted by its /{v1})"),
            DiagnosticId.DatagramReadLimitReached => Invariant(
                $"a Pump made as many reads on bound socket {v1} as it may (MaxDatagramsPerPump {v0})"),

            // A version refusal always carries the required version, which is never 0, so a zero
            // Value1 is the capacity shape (see DiagnosticRecord.ForPeerRefusedCapacity).
            DiagnosticId.PeerRefused when v1 == 0 => Invariant(
                $"a dialler was refused because every capacity slot was occupied (capacity {v0})"),
            DiagnosticId.PeerRefused => Invariant(
                $"a dialler offering an unsupported protocol version was refused (version offered {v0}, version required {v1})"),
            DiagnosticId.LingerNegotiated => Invariant(
                $"the Session established and Linger was negotiated (own {v0} ms, effective {v1} ms)"),
            // Value0 is 0 both for a zero-length prefix and for a stream truncated before a full
            // prefix was read, and the record cannot tell the two apart.
            DiagnosticId.MalformedFraming when v0 == 0 => Invariant(
                $"the Peer's framing broke this profile's rules and the Session was closed (length prefix 0 or none read, max message size {v1})"),
            DiagnosticId.MalformedFraming => Invariant(
                $"the Peer's framing broke this profile's rules and the Session was closed (length prefix {v0}, max message size {v1})"),
            DiagnosticId.DataAfterGoodbye =>
                "the Peer sent Message bytes after its own Goodbye and the Session was closed",
            DiagnosticId.ResumeMatched => Invariant(
                $"a valid resume secret was matched and the Session resumed (bytes rewound {v0})"),
            DiagnosticId.ResumeUnmatched => Invariant(
                $"a claim to an existing Session was refused (candidate Peers checked {v0}, arrived as {ClaimRecordName(v1)}, wire record type {v1})"),
            DiagnosticId.RedialAttempted => Invariant(
                $"a re-dial attempt began (attempt {v0}, waited {v1} ms)"),
            DiagnosticId.RedialFailed => Invariant(
                $"a re-dial attempt failed (socket error {SocketErrorText(v0)}, attempt {v1})"),
            DiagnosticId.DeliveryCounterRefused => Invariant(
                $"the Peer claimed delivery of more Messages than were sent and the Session was closed (count claimed {v0}, count appended {v1})"),
            DiagnosticId.PendingAdmissionExpired => Invariant(
                $"an accepted connection sent no whole handshake record within its deadline and was closed (bytes received {v0}, deadline {v1} ms)"),
            DiagnosticId.PendingAdmissionLimitReached => Invariant(
                $"the Reactor held as many accepted connections waiting for a handshake record as it may and left the rest in the backlog (limit {v0}, admissions declined {v1})"),
            _ => throw new ArgumentOutOfRangeException(
                nameof(record), record.Id, $"{record.Id} is not a recognised DiagnosticId."),
        };
    }

    /// <summary>A <see cref="SocketError"/> carried as its integer value, rendered as name and number.</summary>
    private static string SocketErrorText(long value)
    {
        SocketError error = (SocketError)value;
        return Enum.IsDefined(error) ? Invariant($"{error} {value}") : Invariant($"{value}");
    }

    /// <summary>The delivery mode named by <see cref="DiagnosticId.UnreliableFragment"/>'s mode bits.</summary>
    private static string ModeName(long modeBits) => modeBits switch
    {
        0 => "Unreliable",
        1 => "UnreliableSequenced",
        _ => "unknown",
    };

    /// <summary>
    /// The record a <see cref="DiagnosticId.ResumeUnmatched"/> claim arrived as. The values the two
    /// profiles emit do not collide: 1 is the stream Hello, 3 the stream Resume, 6 the datagram Resume.
    /// </summary>
    private static string ClaimRecordName(long recordType) => recordType switch
    {
        1 => "Hello",
        3 or 6 => "Resume",
        _ => "unknown",
    };

    /// <summary>Renders <paramref name="text"/> in the invariant culture, so numbers never vary by locale.</summary>
    private static string Invariant(FormattableString text) => text.ToString(CultureInfo.InvariantCulture);
}
