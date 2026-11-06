namespace ReactorSockets.Session;

/// <summary>
/// The range a send-idle interval or Linger announced in a handshake must lie in, stated once for both
/// Session profiles and for the options that produce this end's own announcements. Both fields are a
/// <c>ulong</c> on the wire and come from a sender nothing has authenticated.
/// </summary>
internal static class AnnouncedIntervalLegality
{
    /// <summary>
    /// The largest legal announcement in milliseconds: the largest count 32 bits can hold, about 49.7
    /// days. It is far below where a <see cref="TimeSpan"/> conversion can overflow.
    /// </summary>
    public const ulong MaximumMilliseconds = uint.MaxValue;

    /// <summary>
    /// <see cref="MaximumMilliseconds"/> as a <see cref="TimeSpan"/>. An option at or below this
    /// announces at or below <see cref="MaximumMilliseconds"/>, since rounding up to whole milliseconds
    /// cannot pass a value that is already whole.
    /// </summary>
    public static readonly TimeSpan Maximum = TimeSpan.FromMilliseconds(MaximumMilliseconds);

    /// <summary>
    /// Whether <paramref name="milliseconds"/> is a legal announcement. Zero is illegal because an
    /// interval of zero is as meaningless off the wire as it is in the local options.
    /// </summary>
    public static bool IsLegal(ulong milliseconds) => milliseconds is > 0 and <= MaximumMilliseconds;
}
