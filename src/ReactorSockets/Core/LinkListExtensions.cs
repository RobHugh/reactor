using System.Net.Sockets;

namespace ReactorSockets.Core;

/// <summary>
/// The one shape both Session profiles' teardown of their bound links shares (issue #103): unregister
/// every socket from the dispatcher, dispose the link, then clear the list — identical whether the
/// links are <c>TcpListenLink</c> or <c>UdpLink</c>, which is why this stays a generic extension rather
/// than a member either profile's link type would otherwise need to grow.
/// </summary>
internal static class LinkListExtensions
{
    public static void DisposeAndClear<TLink>(
        this List<TLink> links, ReadinessDispatcher dispatcher, Func<TLink, Socket> socketSelector)
        where TLink : IDisposable
    {
        foreach (TLink link in links)
        {
            dispatcher.Unregister(socketSelector(link));
            link.Dispose();
        }

        links.Clear();
    }
}
