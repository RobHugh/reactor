namespace ReactorSockets.Session;

/// <summary>
/// The datagram Session protocol's constants (ADR 0009, ADR 0012): values fixed by the wire format
/// itself, which both ends must agree on for a byte to mean anything. None of these is a knob. A knob
/// trades the consumer's own resources and the consumer is the only one who can weigh the trade;
/// changing any value here changes what a datagram means, which is a protocol version, not a
/// setting — so exposing one on <see cref="DatagramSessionOptions"/> would offer a consumer a
/// configuration that can only break interoperation.
/// </summary>
internal static class DatagramSessionProtocol
{
    /// <summary>
    /// The protocol version both ends must offer, matched exactly. There is no negotiation, no
    /// lowest-common-version rule and no compatibility range: a mismatch is a Refusal naming both
    /// versions. Per-profile, so the stream Session profile's version moves independently of this one.
    /// </summary>
    public const uint Version = 1;

    /// <summary>
    /// The session id reserved to mean "this datagram is control, not data" (ADR 0009). Never
    /// assigned to a Session, which is why the assignment loop rejects it rather than masking it.
    /// </summary>
    public const uint ControlSession = 0;

    /// <summary>
    /// The exact size in bytes of every pre-Session control datagram, whatever its type, zero-padded
    /// to fill.
    /// </summary>
    /// <remarks>
    /// Uniform size is what makes the amplification bound structural rather than checked. An
    /// unverified sender is answered with exactly one control datagram, and every control datagram is
    /// this many bytes, so bytes sent equals bytes received for every such exchange — there is no
    /// arithmetic to get wrong and no combination of types that could invert the ratio. It also costs
    /// nothing: 256 bytes is far below the 1200-byte fragment floor a data-carrying datagram already
    /// assumes will pass, so the padding never introduces a path-MTU problem the data plane does not
    /// already have.
    /// </remarks>
    public const int ControlDatagramSize = 256;

    /// <summary>
    /// The number of seconds a cookie-minting secret stays current before the next one takes over.
    /// The verifier accepts the current epoch and the previous one, so a client always has at least
    /// this long to complete a round trip and never less.
    /// </summary>
    /// <remarks>
    /// Thirty seconds is the protocol's own choice, not a guess at a network: it is long enough that
    /// no plausible round trip expires inside it, and short enough that a captured cookie is useless
    /// almost immediately. It is measured against each Reactor's own <see cref="TimeProvider"/> from
    /// its own construction — the two ends never compare epoch numbers, so no shared clock is
    /// required and none is assumed.
    /// </remarks>
    public const int CookieEpochSeconds = 30;

    /// <summary>The length in bytes of a minted cookie: a full HMAC-SHA256 tag, untruncated.</summary>
    public const int CookieLength = 32;

    /// <summary>The length in bytes of the secret established for a later Resume, exchanged during the handshake.</summary>
    public const int ResumeSecretLength = 16;

    /// <summary>The length in bytes of the per-Reactor secret the cookie HMAC is keyed with.</summary>
    public const int EpochSecretLength = 32;

    /// <summary>
    /// The offset of a control datagram's type byte: immediately after the four-byte session id that
    /// every datagram, control or data, opens with.
    /// </summary>
    public const int ControlTypeOffset = sizeof(uint);

    /// <summary>The offset at which a control datagram's body begins.</summary>
    public const int ControlBodyOffset = ControlTypeOffset + sizeof(byte);

    /// <summary>
    /// The exact size in bytes of a live-Session datagram header (ADR 0009): the four-byte session id
    /// followed by the two reliable spaces' ack blocks, always present and fixed-size regardless of
    /// how much the sender has to acknowledge.
    /// </summary>
    public const int DatagramHeaderSize = sizeof(uint) + (2 * ReliableAckBlockSize);

    /// <summary>
    /// The offset of the <c>ReliableOrdered</c> space's ack block: a cumulative base sequence followed
    /// by the 32-bit bitfield of the sequences above it.
    /// </summary>
    public const int ReliableOrderedAckBlockOffset = sizeof(uint);

    /// <summary>
    /// The offset of the <c>ReliableUnordered</c> space's ack block, immediately after the
    /// <c>ReliableOrdered</c> one. Only the two reliable spaces are acked; <c>Unreliable</c> and
    /// <c>UnreliableSequenced</c> are observed, never acknowledged, which is why there are four
    /// sequence spaces but only two ack blocks.
    /// </summary>
    public const int ReliableUnorderedAckBlockOffset = ReliableOrderedAckBlockOffset + ReliableAckBlockSize;

    /// <summary>The size in bytes of one ack block: a four-byte base sequence plus a four-byte bitfield.</summary>
    private const int ReliableAckBlockSize = sizeof(uint) + sizeof(uint);

    /// <summary>
    /// The exact size in bytes of a packet record carrying the unfragmented shape (ADR 0009): one flags
    /// byte, a four-byte sequence, and a two-byte length. A complete Message pays only this — no
    /// fragment index, no fragment count — since those two fields exist only to locate one piece
    /// within a larger Message, which an unfragmented record never is.
    /// </summary>
    public const int UnfragmentedRecordSize = sizeof(byte) + sizeof(uint) + sizeof(ushort);

    /// <summary>
    /// The exact size in bytes of a packet record carrying the fragment shape (ADR 0009): the
    /// unfragmented record's bytes followed by a two-byte fragment index and a two-byte fragment count.
    /// The two extra fields are what let a receiver place this piece within its Message and know when
    /// every piece has arrived, in place of a message id no fragment shape carries — see
    /// <see cref="FragmentBit"/>.
    /// </summary>
    public const int FragmentRecordSize = UnfragmentedRecordSize + sizeof(ushort) + sizeof(ushort);

    /// <summary>
    /// The offset of a Fragment record's <c>u16</c> fragment index, immediately after the unfragmented
    /// shape's bytes. Meaningless — and unread — on an unfragmented record, which does not carry it.
    /// </summary>
    public const int RecordFragmentIndexOffset = UnfragmentedRecordSize;

    /// <summary>The offset of a Fragment record's <c>u16</c> fragment count, immediately after its fragment index.</summary>
    public const int RecordFragmentCountOffset = RecordFragmentIndexOffset + sizeof(ushort);

    /// <summary>
    /// The offset of a packet record's <c>u32</c> sequence, immediately after the flags byte. Named
    /// here rather than spelled as a literal at each end, because the reader and the writer sit in
    /// separate files and a record whose two ends disagree by a byte is the one defect the wire format
    /// cannot detect.
    /// </summary>
    public const int RecordSequenceOffset = sizeof(byte);

    /// <summary>The offset of a packet record's <c>u16</c> payload length, immediately after its sequence.</summary>
    public const int RecordLengthOffset = RecordSequenceOffset + sizeof(uint);

    /// <summary>
    /// The mask isolating the two mode bits in a packet record's flags byte. Applied before comparing
    /// against <see cref="ModeUnreliable"/>, <see cref="ModeUnreliableSequenced"/>,
    /// <see cref="ModeReliableUnordered"/> or <see cref="ModeReliableOrdered"/>, since the fragment bit
    /// and the spare bits share the same byte.
    /// </summary>
    public const byte ModeMask = 0b0000_0011;

    /// <summary>
    /// The wire value of the <c>Unreliable</c> mode's two mode bits (ADR 0009). Not the public
    /// <see cref="ReactorSockets.DeliveryMode"/> enum (issue #207) — this is what the wire carries for
    /// that mode, named here so the eight tickets after this one never restate the number.
    /// </summary>
    public const byte ModeUnreliable = 0b0000_0000;

    /// <summary>The wire value of the <c>UnreliableSequenced</c> mode's two mode bits (ADR 0009).</summary>
    public const byte ModeUnreliableSequenced = 0b0000_0001;

    /// <summary>The wire value of the <c>ReliableUnordered</c> mode's two mode bits (ADR 0009).</summary>
    public const byte ModeReliableUnordered = 0b0000_0010;

    /// <summary>The wire value of the <c>ReliableOrdered</c> mode's two mode bits (ADR 0009).</summary>
    public const byte ModeReliableOrdered = 0b0000_0011;

    /// <summary>
    /// The bit marking a packet record as a fragment of a larger Message rather than a complete one
    /// (ADR 0004, ADR 0009). Only the two reliable modes ever set it, and both ends now enforce that
    /// rather than merely observe it: <see cref="DatagramSessionReactor.Send"/> answers
    /// <see cref="SendOutcome.MessageTooLarge"/> rather than fragment an unreliable Message (issue
    /// #246), and <see cref="PacketRecordLegality.ShapeOf"/> refuses the shape on the way in, failing
    /// the read and discarding the whole Datagram (issue #273). Setting it grows the record by
    /// the fragment index and fragment count that follow the length field — an unfragmented Message
    /// pays one bit rather than the four bytes those fields would cost every record if they were
    /// always present.
    /// </summary>
    public const byte FragmentBit = 0b0000_0100;

    /// <summary>
    /// The mask isolating the five spare bits in a packet record's flags byte. These are genuinely
    /// spare, not reserved-and-ignored: with the protocol version matched exactly (ADR 0009), a record
    /// with any spare bit set is a protocol violation and is rejected, not tolerated. Only a version
    /// bump unlocks them.
    /// </summary>
    public const byte SpareBitsMask = 0b1111_1000;
}
