namespace ReactorSockets.Session;

/// <summary>
/// What counts as a legal announced interval or Refusal reason in a handshake, stated once for both
/// Session profiles and for the options that produce this end's own announcements.
/// <see cref="ControlDatagramCodec"/> and <see cref="StreamHandshakeCodec"/> both ask this class, and
/// <see cref="AnnouncedLimits.IsLegal"/> judges its intervals here. Every value judged here comes from a
/// sender nothing has authenticated.
/// </summary>
/// <remarks>
/// The rules here make both handshake parsers total: a read that succeeds returns only values every
/// downstream conversion accepts without throwing, and a read that fails discards the record. Each
/// Session profile has a handshake fuzz test that holds this through the public surface.
/// </remarks>
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
    /// Whether <paramref name="reason"/> is a legal Refusal reason: one
    /// <see cref="RefusalReasonMap.DisconnectReasonFor"/> maps. An unreadable reason is
    /// indistinguishable from a forged Refusal, so a record carrying one is discarded whole.
    /// </summary>
    public static bool IsLegalRefusalReason(RefusalReason reason) => RefusalReasonMap.DisconnectReasonFor(reason) is not null;
}
