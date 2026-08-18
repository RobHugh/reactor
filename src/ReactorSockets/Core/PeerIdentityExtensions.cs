namespace ReactorSockets.Core;

/// <summary>
/// The conversion between the public <see cref="PeerId"/> and the internal <see cref="SlotHandle"/> it
/// wraps — identical in both Session profiles (ADR 0007, issue #103), since a <see cref="PeerId"/>
/// carries the same three fields a <see cref="SlotHandle"/> does under its own public names.
/// </summary>
internal static class PeerIdentityExtensions
{
    public static PeerId ToPeerId(this SlotHandle handle) => new(handle.Origin, handle.Index, handle.Generation);

    public static SlotHandle ToHandle(this PeerId peer) => new(peer.Origin, peer.Index, peer.Generation);
}
