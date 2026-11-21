namespace ReactorSockets.Session;

/// <summary>
/// What each end announces about itself during the datagram handshake, carried in the same order
/// by both a <see cref="DatagramControlType.HelloWithCookie"/> (the dialler's) and an
/// <see cref="DatagramControlType.Accept"/> (the accepting end's). <see cref="ControlDatagramCodec"/>
/// writes and reads them as one value so the two datagrams cannot drift apart on these fields.
/// </summary>
/// <param name="SessionIdForMe">The session id the announcing end wants stamped on datagrams sent to it.</param>
/// <param name="Limits">
/// The announcing end's limits, the same block a stream Hello carries. Once read, they are
/// <see cref="AnnouncedLimits.IsLegal">legal</see> for the version they arrived with.
/// </param>
/// <param name="FragmentSize">
/// The announcing end's own Fragment size in bytes, written after <paramref name="Limits"/>. Datagram
/// profile only, which is why it is not part of the shared <see cref="AnnouncedLimits"/> block.
/// </param>
/// <param name="ReceiveWindowFragments">The announcing end's receive window, in Fragments, written after <paramref name="FragmentSize"/>.</param>
internal readonly record struct AnnouncedSessionTerms(
    uint SessionIdForMe,
    AnnouncedLimits Limits,
    ushort FragmentSize,
    uint ReceiveWindowFragments)
{
    /// <summary>The smallest legal announced <see cref="FragmentSize"/>: the reassembly buffer RFC 791 guarantees every host.</summary>
    public const int MinimumFragmentSize = 576;

    /// <summary>The largest legal announced <see cref="FragmentSize"/>: the largest payload a UDP datagram has room for.</summary>
    public const int MaximumFragmentSize = 65_507;

    /// <summary>
    /// Whether these terms are a legal announcement. The <see cref="Limits"/> are judged by
    /// <see cref="AnnouncedLimits.IsLegal"/>. <see cref="FragmentSize"/> must lie between
    /// <see cref="MinimumFragmentSize"/> and <see cref="MaximumFragmentSize"/> and
    /// <see cref="ReceiveWindowFragments"/> must not be zero, both only when
    /// <paramref name="speaksVersion"/> is <see langword="true"/>: a handshake of a version this end
    /// does not speak is left for the version Refusal, because this end cannot judge the fields of
    /// that version.
    /// </summary>
    public bool IsLegal(bool speaksVersion) =>
        Limits.IsLegal(speaksVersion)
        && (!speaksVersion
            || (FragmentSize is >= MinimumFragmentSize and <= MaximumFragmentSize
                && ReceiveWindowFragments != 0));
}
