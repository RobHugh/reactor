using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace ReactorSockets;

/// <summary>
/// Who a datagram claims to be from — an unauthenticated hint about origin, never an identity
/// (ADR 0014). Carried on the record path as a value type: <c>{ family, 16 address bytes, scope id,
/// port }</c>, roughly 56 bytes, so a Diagnostic or an Event never allocates to name a Source.
/// </summary>
/// <remarks>
/// Equality is total and exact over every field, including <see cref="ScopeId"/>: an IPv6
/// link-local address is only meaningful with its zone index, and two hosts reachable at the same
/// <c>fe80::/10</c> address on different interfaces must not compare equal — the datagram profile keys
/// a cookie HMAC over this value. <see cref="Family"/> is an explicit tag rather than inferred from
/// <see cref="AddressBytes"/>, because with <c>DualMode</c> never set there is nothing in a 16-byte
/// field distinguishing a zero-padded IPv4 address from an IPv6 one.
/// </remarks>
public readonly record struct Source(SourceFamily Family, UInt128 AddressBytes, long ScopeId, int Port)
{
    /// <summary>
    /// Converts an <see cref="IPEndPoint"/> — the type the consumer already holds on the cold
    /// bind/dial surface — into the value type carried on the record path (ADR 0014). Throws
    /// <see cref="ArgumentException"/> for anything other than IPv4 or IPv6, since <c>DualMode</c>
    /// is never set and no mapped form can appear here.
    /// </summary>
    public static Source FromIPEndPoint(IPEndPoint endpoint)
    {
        IPAddress address = endpoint.Address;
        Span<byte> buffer = stackalloc byte[16];
        SourceFamily family;
        long scopeId;
        switch (address.AddressFamily)
        {
            case AddressFamily.InterNetwork:
                family = SourceFamily.IPv4;
                scopeId = 0;
                address.TryWriteBytes(buffer[..4], out _);
                break;
            case AddressFamily.InterNetworkV6:
                family = SourceFamily.IPv6;
                scopeId = address.ScopeId;
                address.TryWriteBytes(buffer, out _);
                break;
            default:
                throw new ArgumentException(
                    $"address family {address.AddressFamily} is neither IPv4 nor IPv6 — a Source has no mapped form.",
                    nameof(endpoint));
        }

        UInt128 addressBytes = BinaryPrimitives.ReadUInt128BigEndian(buffer);
        return new Source(family, addressBytes, scopeId, endpoint.Port);
    }

    /// <summary>
    /// Converts this value back into an <see cref="IPEndPoint"/> for the cold surfaces that already
    /// hold that type (ADR 0014).
    /// </summary>
    public IPEndPoint ToIPEndPoint()
    {
        Span<byte> buffer = stackalloc byte[16];
        BinaryPrimitives.WriteUInt128BigEndian(buffer, AddressBytes);
        IPAddress address = Family switch
        {
            SourceFamily.IPv4 => new IPAddress(buffer[..4]),
            SourceFamily.IPv6 => new IPAddress(buffer, ScopeId),
            _ => throw new ArgumentOutOfRangeException(nameof(Family), Family, $"{Family} is not a recognised SourceFamily."),
        };

        return new IPEndPoint(address, Port);
    }

    /// <summary>
    /// Whether <paramref name="other"/> names the same host address, whatever its port. Family and scope
    /// count, for the reasons on this type's remarks.
    /// </summary>
    internal bool HasSameAddressAs(Source other) =>
        Family == other.Family && AddressBytes == other.AddressBytes && ScopeId == other.ScopeId;

    /// <summary>
    /// Whether <paramref name="other"/> falls under the same per-address Peer limit as this Source
    /// (ADR 0014). An IPv4 address is compared whole. An IPv6 address is compared on its leading
    /// <paramref name="ipv6PrefixLength"/> bits. Family and scope always count, and the port never does.
    /// </summary>
    internal bool SharesPeerLimitWith(Source other, int ipv6PrefixLength)
    {
        if (Family != other.Family || ScopeId != other.ScopeId)
        {
            return false;
        }

        UInt128 mask = UInt128.MaxValue << (128 - PeerLimitPrefixLength(ipv6PrefixLength));
        return (AddressBytes & mask) == (other.AddressBytes & mask);
    }

    /// <summary>
    /// How many leading bits of this Source's address form its per-address Peer limit key: 32 for IPv4,
    /// which is never grouped, and <paramref name="ipv6PrefixLength"/> for IPv6.
    /// </summary>
    internal int PeerLimitPrefixLength(int ipv6PrefixLength) =>
        Family == SourceFamily.IPv4 ? 32 : ipv6PrefixLength;

    /// <summary>
    /// The default IPv6 grouping length for the per-address Peer limit. RFC 4291 §2.5.1 fixes the
    /// interface identifier of a unicast address at 64 bits, so a /64 is one link (ADR 0014).
    /// </summary>
    internal const int DefaultPeerLimitIPv6PrefixLength = 64;

    /// <summary>
    /// Throws when <paramref name="prefixLength"/> is set and outside 1 to 128, naming the knob that
    /// carried it. Shared by both Session profiles' options, which take the same knob.
    /// </summary>
    internal static void ValidatePeerLimitIPv6PrefixLength(int? prefixLength, string paramName)
    {
        if (prefixLength is int length && length is < 1 or > 128)
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                length,
                $"an IPv6 prefix length of {length} names no prefix of a 128-bit address — it must be from 1 to 128, and 128 counts each address on its own.");
        }
    }
}
