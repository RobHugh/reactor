namespace ReactorSockets.Session;

/// <summary>
/// What counts as a legal value in a handshake, stated once for both Session profiles and for the
/// options that produce this end's own announcements. <see cref="ControlDatagramCodec"/> and
/// <see cref="StreamHandshakeCodec"/> each read their own wire layout and then ask this class whether
/// the values they read are legal, so both transports apply the rules stated here. Every value judged
/// here comes from a sender nothing has authenticated.
/// </summary>
internal static class HandshakeLegality
{
    /// <summary>
    /// The largest legal announced send-idle interval or Linger in milliseconds: the largest count 32
    /// bits can hold, about 49.7 days. It is far below where a <see cref="TimeSpan"/> conversion can
    /// overflow.
    /// </summary>
    public const ulong MaximumIntervalMilliseconds = uint.MaxValue;

    /// <summary>
    /// <see cref="MaximumIntervalMilliseconds"/> as a <see cref="TimeSpan"/>. An option at or below
    /// this announces at or below <see cref="MaximumIntervalMilliseconds"/>, since rounding up to whole
    /// milliseconds cannot pass a value that is already whole.
    /// </summary>
    public static readonly TimeSpan MaximumInterval = TimeSpan.FromMilliseconds(MaximumIntervalMilliseconds);

    /// <summary>
    /// Whether <paramref name="milliseconds"/> is a legal announced interval. Zero is illegal because
    /// an interval of zero is as meaningless off the wire as it is in the local options.
    /// </summary>
    public static bool IsLegalInterval(ulong milliseconds) => milliseconds is > 0 and <= MaximumIntervalMilliseconds;

    /// <summary>
    /// Whether the values an end announces about itself in a handshake are legal. A
    /// <paramref name="maxMessageSize"/> of zero is illegal whatever the version, as it is in the local
    /// options. The intervals are judged by <see cref="IsLegalInterval"/> only when
    /// <paramref name="speaksVersion"/> is <see langword="true"/>: a handshake of a version this end
    /// does not speak is left for the version Refusal, because this end cannot judge the fields of
    /// that version.
    /// </summary>
    public static bool IsLegalAnnouncement(
        bool speaksVersion,
        uint maxMessageSize,
        ulong sendIdleIntervalMilliseconds,
        ulong lingerMilliseconds) =>
        maxMessageSize != 0
        && (!speaksVersion
            || (IsLegalInterval(sendIdleIntervalMilliseconds) && IsLegalInterval(lingerMilliseconds)));

    /// <summary>
    /// Whether <paramref name="reason"/> is a legal Refusal reason: one
    /// <see cref="RefusalReasonMap.DisconnectReasonFor"/> maps. An unreadable reason is
    /// indistinguishable from a forged Refusal, so a record carrying one is discarded whole.
    /// </summary>
    public static bool IsLegalRefusalReason(RefusalReason reason) => RefusalReasonMap.DisconnectReasonFor(reason) is not null;
}
