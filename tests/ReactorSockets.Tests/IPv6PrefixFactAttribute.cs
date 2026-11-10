using System.Net;
using System.Net.Sockets;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A <see cref="FactAttribute"/> for a test that needs several local IPv6 addresses in one prefix
/// (issue #453). Loopback holds only <c>::1</c>, so these addresses exist only where something added
/// them: CI adds them to the loopback interface before the test step. Where any one cannot be bound,
/// the test is skipped with the reason, rather than failing on a machine nobody configured.
/// </summary>
public sealed class IPv6PrefixFactAttribute : FactAttribute
{
    /// <summary>The first address in the shared prefix <c>fd00:453::/64</c>.</summary>
    public static readonly IPAddress InPrefixFirst = IPAddress.Parse("fd00:453::1");

    /// <summary>A second address in <c>fd00:453::/64</c>, sharing its /64 with <see cref="InPrefixFirst"/>.</summary>
    public static readonly IPAddress InPrefixSecond = IPAddress.Parse("fd00:453::2");

    /// <summary>An address in <c>fd00:453:0:1::/64</c>, which shares a /48 but not a /64 with the others.</summary>
    public static readonly IPAddress OutsidePrefix = IPAddress.Parse("fd00:453:0:1::1");

    private static readonly string? _skipReason = ProbeSkipReason();

    /// <summary>Skips the test unless all three addresses can be bound on this machine.</summary>
    public IPv6PrefixFactAttribute()
    {
        if (_skipReason is not null)
        {
            Skip = _skipReason;
        }
    }

    private static string? ProbeSkipReason()
    {
        foreach (IPAddress address in (IPAddress[])[InPrefixFirst, InPrefixSecond, OutsidePrefix])
        {
            try
            {
                using Socket probe = new(AddressFamily.InterNetworkV6, SocketType.Dgram, ProtocolType.Udp);
                probe.Bind(new IPEndPoint(address, 0));
            }
            catch (SocketException ex)
            {
                return $"{address} is not a local address ({ex.SocketErrorCode}). Add fd00:453::1, fd00:453::2 and fd00:453:0:1::1 to the loopback interface, as CI does, to run this test.";
            }
        }

        return null;
    }
}
