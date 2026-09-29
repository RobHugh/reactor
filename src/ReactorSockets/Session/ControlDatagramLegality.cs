namespace ReactorSockets.Session;

/// <summary>
/// What a control datagram of a given type must look like, and whether this profile acts on it —
/// stated once, as a table, so that the parser, the sender and the reader all consult the same
/// statement rather than three copies that can drift apart.
/// </summary>
/// <param name="IsProcessed">
/// Whether this profile does anything with a datagram of this type. A type on the roster that this
/// slice does not implement is <see langword="false"/> here, which discards it exactly as an
/// off-roster type is discarded — the wire is not the place to advertise an unfinished feature.
/// </param>
/// <param name="BodyLength">
/// The exact number of meaningful bytes after the type byte. Every remaining byte up to
/// <see cref="DatagramSessionProtocol.ControlDatagramSize"/> is padding and must be zero.
/// </param>
internal readonly record struct ControlDatagramShape(bool IsProcessed, int BodyLength);

/// <summary>
/// The control datagram roster as a single table (the house pattern <c>RawLinkLegality</c> sets).
/// Every arm is spelled out and the fall-through refuses, so a type added to
/// <see cref="DatagramControlType"/> without a decision here is discarded rather than silently
/// parsed against whatever body length the last arm happened to name.
/// </summary>
internal static class ControlDatagramLegality
{
    /// <summary>
    /// The shape of a control datagram of <paramref name="type"/>, or a shape that is neither
    /// processed nor parseable when this profile has no arm for it.
    /// </summary>
    public static ControlDatagramShape ShapeOf(DatagramControlType type) => type switch
    {
        // version(4) + clientNonce(8). Nothing else: a Hello must be answerable without allocating,
        // so it carries nothing the answer would have to remember.
        DatagramControlType.Hello => new ControlDatagramShape(true, 12),

        // clientNonce(8) + epoch(8) + mac(32). The epoch travels so the verifier knows which secret
        // to recompute under without having to try every one it still holds.
        DatagramControlType.Cookie => new ControlDatagramShape(true, 48),

        // version(4) + clientNonce(8) + epoch(8) + mac(32) + sessionIdForMe(4) + maxMessageSize(4)
        // + sendIdleIntervalMilliseconds(8) + lingerMilliseconds(8) + resumeSecret(16). Everything a
        // Session needs, arriving only once the round trip is paid.
        DatagramControlType.HelloWithCookie => new ControlDatagramShape(true, 92),

        // clientNonce(8) + sessionIdForMe(4) + maxMessageSize(4) + sendIdleIntervalMilliseconds(8)
        // + lingerMilliseconds(8) + resumeSecret(16).
        DatagramControlType.Accept => new ControlDatagramShape(true, 48),

        // clientNonce(8) + reason(1) + versionOffered(4) + versionRequired(4). The two version fields
        // are populated only for a version refusal and are zero otherwise, rather than the refusal
        // splitting into two datagram types over four bytes.
        DatagramControlType.Refusal => new ControlDatagramShape(true, 17),

        // targetSessionId(4) + resumeSecret(16). The codec reads and writes it (issue #373). The
        // dialling end sends it while Interrupted and the accepting end routes it by targetSessionId
        // and authorises it by resumeSecret.
        DatagramControlType.Resume => new ControlDatagramShape(true, 20),

        // The discard arm refuses. An unknown control type is a protocol violation, not an extension
        // point: this profile has no forward-compatibility rule that would let it skip a body of
        // unknown length, so there is nothing it could do but throw the datagram away.
        _ => new ControlDatagramShape(false, 0),
    };
}
