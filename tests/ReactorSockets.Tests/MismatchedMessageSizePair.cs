namespace ReactorSockets.Tests;

/// <summary>
/// A dialler and an accepter, each built with a deliberately different <c>MaxMessageSize</c>, connected
/// and handshaken. Issue #284 moved <see cref="DatagramSessionReactor.Send"/>'s size boundary from a
/// Reactor's own announced limit to the peer's: a shared limit could never prove that, so both #290 and
/// #291 need this same mismatched pair. There is no public accessor for the peer's announced limit, so
/// both <see cref="DiallerPeer"/> and <see cref="AccepterPeer"/> are exposed — one <see cref="PeerId"/>
/// per direction — so a test can drive <c>Send</c> from either end and read the limit it proves back off
/// the <see cref="SendOutcome"/> and what the other end drains.
/// </summary>
internal sealed class MismatchedMessageSizePair : IDisposable
{
    /// <summary>The Fragment size both Reactors are built with.</summary>
    public const int FragmentSize = 576;

    /// <summary>The accepter's announced limit — the smaller of the two, and the one the dialler's Sends are bound by.</summary>
    public const int AccepterMaxMessageSize = 1200;

    /// <summary>The dialler's announced limit — the larger of the two, and the one the accepter's Sends are bound by.</summary>
    public const int DiallerMaxMessageSize = 4096;

    /// <summary>Retention room comfortably above <see cref="DiallerMaxMessageSize"/>, the larger of the two limits.</summary>
    public const int RetentionBytes = DiallerMaxMessageSize * 4;

    private MismatchedMessageSizePair(
        DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, PeerId diallerPeer, PeerId accepterPeer)
    {
        Dialler = dialler;
        Accepter = accepter;
        DiallerPeer = diallerPeer;
        AccepterPeer = accepterPeer;
    }

    /// <summary>The Reactor built with <see cref="DiallerMaxMessageSize"/>, which dialled the connection.</summary>
    public DatagramSessionReactorHarness Dialler { get; }

    /// <summary>The Reactor built with <see cref="AccepterMaxMessageSize"/>, which accepted the connection.</summary>
    public DatagramSessionReactorHarness Accepter { get; }

    /// <summary>
    /// The <see cref="PeerId"/> the dialler holds for the accepter — pass this to
    /// <c>Dialler.Reactor.Send</c> to send toward the accepter's announced <see cref="AccepterMaxMessageSize"/> limit.
    /// </summary>
    public PeerId DiallerPeer { get; }

    /// <summary>
    /// The <see cref="PeerId"/> the accepter holds for the dialler — pass this to
    /// <c>Accepter.Reactor.Send</c> to send toward the dialler's announced <see cref="DiallerMaxMessageSize"/> limit.
    /// </summary>
    public PeerId AccepterPeer { get; }

    /// <summary>Builds a connected, handshaken pair with the mismatched limits above.</summary>
    public static MismatchedMessageSizePair Build()
    {
        DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: DiallerMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        DatagramSessionReactorHarness accepter = new(
            maxMessageSize: AccepterMaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);

        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> _, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        return new MismatchedMessageSizePair(dialler, accepter, diallerPeer, accepterPeer);
    }

    public void Dispose()
    {
        Dialler.Dispose();
        Accepter.Dispose();
    }
}
