namespace ReactorSockets.Session;

/// <summary>
/// The stream Session protocol's constants (ADR 0010): values fixed by the wire format itself, which
/// both ends must agree on for a byte to mean anything. None of these is a knob — changing one changes
/// what a handshake record means, which is a version, not a setting, so none is exposed on
/// <see cref="StreamSessionOptions"/>.
/// </summary>
internal static class StreamSessionProtocol
{
    /// <summary>
    /// The protocol version both ends must offer, matched exactly. There is no negotiation, no
    /// lowest-common-version rule and no compatibility range. Per-profile: this moves independently of
    /// <see cref="DatagramSessionProtocol.Version"/>, since the two Session protocols are separate
    /// formats on separate transports with separate handshakes.
    /// </summary>
    public const uint Version = 1;

    /// <summary>The length in bytes of the secret established for a later Resume, exchanged during the handshake.</summary>
    public const int ResumeSecretLength = 16;
}
