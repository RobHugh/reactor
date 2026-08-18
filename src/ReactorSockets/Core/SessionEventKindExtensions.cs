namespace ReactorSockets.Core;

/// <summary>
/// The half of <c>ISessionEventSource.GetPendingEvent</c>'s translation both Session profiles share
/// (issue #103): <see cref="SessionEventKind.PeerConnected"/> and <see cref="SessionEventKind.PeerClosed"/>
/// need nothing beyond a <see cref="PeerId"/> and a <see cref="DisconnectReason"/> to become a
/// <see cref="SessionEvent"/>. A profile with kinds of its own — the stream profile's
/// <see cref="SessionEventKind.MessageReceived"/> and <see cref="SessionEventKind.PeerClosing"/> —
/// falls through to its own switch when this returns <see langword="false"/>.
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

            default:
                sessionEvent = default;
                return false;
        }
    }
}
