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
}
