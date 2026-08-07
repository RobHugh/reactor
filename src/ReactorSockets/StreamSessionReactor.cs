namespace ReactorSockets;

/// <summary>
/// The stream Session profile (ADR 0010): Sessions established over TCP, addressed as Peers,
/// delivering <c>u32</c> length-prefixed Messages rather than the byte runs the transport gives.
/// This slice (3.1) validates <see cref="StreamSessionOptions"/> and its allocation bill at
/// construction; binding (3.2), the handshake (3.3), framing, retention and the Goodbye exchange are
/// not yet built and land in the sub-issues that follow.
/// </summary>
/// <remarks>
/// There is no cookie, no ack, no sequence space, no fragment and no RTO in this profile: TCP already
/// provides what those were built for. The datagram Session protocol is deliberately not carried over
/// TCP, because that would put retransmission on top of retransmission.
/// </remarks>
public sealed class StreamSessionReactor
{
    /// <summary>
    /// Validates <paramref name="options"/> — every knob and cross-check, then the frame arena's
    /// floor — before anything is allocated or bound. A combination that cannot work throws here and
    /// nowhere else, so there is no half-built Reactor to hand back.
    /// </summary>
    /// <param name="options">
    /// Composed rather than inherited (ADR 0011): the shared L0 budget plus this profile's own
    /// required and defaulted knobs, with no shared base between profiles.
    /// </param>
    public StreamSessionReactor(StreamSessionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        options.ResolveArenaSize();
    }
}
