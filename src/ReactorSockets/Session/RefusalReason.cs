namespace ReactorSockets.Session;

/// <summary>
/// Why a cookie-validated Hello was refused. A wire value, so the members carry explicit numbers.
/// </summary>
/// <remarks>
/// There are exactly two, and both are answers a Reactor gives only after the round trip is paid. A
/// bare Hello is never refused — it is answered with a Cookie whatever it claimed — because refusing
/// one would mean an unverified sender could make this Reactor emit a considered reply, and worse,
/// could learn from a spoofed source address whether some third party is full or speaks a different
/// version.
/// </remarks>
internal enum RefusalReason : byte
{
    /// <summary>The offered version is not the one this Reactor speaks. The Refusal names both.</summary>
    Version = 1,

    /// <summary>
    /// No capacity Slot was available to this handshake: every Slot was occupied, or its address held
    /// <see cref="DatagramSessionOptions.MaxPeersPerAddress"/> or
    /// <see cref="StreamSessionOptions.MaxPeersPerAddress"/> Peers. Says nothing about versions.
    /// </summary>
    Capacity = 2,
}

/// <summary>
/// The single statement of what each <see cref="RefusalReason"/> means to a consumer, so the sending
/// end and the receiving end cannot disagree about it.
/// </summary>
internal static class RefusalReasonMap
{
    /// <summary>
    /// The <see cref="DisconnectReason"/> a received <paramref name="reason"/> resolves to, or
    /// <see langword="null"/> when it names nothing this profile understands — in which case the
    /// Refusal is treated as malformed rather than as some default disconnection, because a Refusal
    /// whose reason cannot be read is indistinguishable from a forged one.
    /// </summary>
    public static DisconnectReason? DisconnectReasonFor(RefusalReason reason) => reason switch
    {
        RefusalReason.Version => DisconnectReason.VersionMismatch,
        RefusalReason.Capacity => DisconnectReason.RemoteAtCapacity,

        // The discard arm refuses.
        _ => null,
    };
}
