namespace ReactorSockets.Core;

/// <summary>
/// The half of <c>ISessionEventSource.GetPendingEvent</c>'s translation both Session profiles share
/// (issue #103): <see cref="SessionEventKind.PeerConnected"/>, <see cref="SessionEventKind.PeerClosed"/>,
/// <see cref="SessionEventKind.PeerInterrupted"/> (issue #350) and <see cref="SessionEventKind.PeerResumed"/>
/// (issue #383, so the datagram profile can raise it too) need nothing beyond a
/// <see cref="PeerId"/> and, for <see cref="SessionEventKind.PeerClosed"/>, a
/// <see cref="DisconnectReason"/> to become a <see cref="SessionEvent"/>. A profile with kinds of its
/// own — the stream profile's <see cref="SessionEventKind.MessageReceived"/> and
/// <see cref="SessionEventKind.PeerClosing"/> — falls through to its own switch when this returns
/// <see langword="false"/>.
/// </summary>
internal static class SessionEventKindExtensions
{
    public static bool TryCreateCommonSessionEvent(
        this SessionEventKind kind, PeerId peer, DisconnectReason reason, out SessionEvent sessionEvent)
    {
        switch (kind)
        {
            case SessionEventKind.PeerConnected:
                sessionEvent = SessionEvent.ForPeerConnected(peer);
                return true;

            case SessionEventKind.PeerClosed:
                sessionEvent = SessionEvent.ForPeerClosed(peer, reason);
                return true;

            case SessionEventKind.PeerInterrupted:
                sessionEvent = SessionEvent.ForPeerInterrupted(peer);
                return true;

            case SessionEventKind.PeerResumed:
                sessionEvent = SessionEvent.ForPeerResumed(peer);
                return true;

            default:
                sessionEvent = default;
                return false;
        }
    }
}
