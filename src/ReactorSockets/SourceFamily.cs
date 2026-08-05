namespace ReactorSockets;

/// <summary>
/// The address family of a <see cref="Source"/>, carried as an explicit tag rather than inferred
/// from the address bytes (ADR 0014). With <c>DualMode</c> never set, nothing in a 16-byte field
/// distinguishes a zero-padded IPv4 address from an IPv6 one.
/// </summary>
public enum SourceFamily
{
    /// <summary>An IPv4 address, stored in the low-order 4 bytes of <see cref="Source.AddressBytes"/>.</summary>
    IPv4,

    /// <summary>An IPv6 address, occupying all 16 bytes of <see cref="Source.AddressBytes"/>.</summary>
    IPv6,
}
