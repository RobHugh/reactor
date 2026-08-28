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

    /// <summary>
    /// Retention room for four Messages at <see cref="DiallerMaxMessageSize"/>, the larger of the two
    /// limits. A reliable Send retains every Fragment it cuts until the far end acknowledges it, so the
    /// floor is one whole Message at the larger limit; the multiplier is headroom, so that a test driving
    /// several Messages across one pair cannot trip a retention refusal and report a budget exhaustion as
    /// though it were the size-limit refusal under test.
    /// </summary>
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

    /// <summary>
    /// Pumps both ends until <paramref name="receiver"/> has drained at least one Message payload, or
    /// until the <see cref="PumpDeadline"/> budget expires, and returns what it drained. Reassembling a
    /// Fragment set spans more than one Frame, so a test waiting on a delivered Message needs a
    /// clock-bounded wait rather than a round count, and
    /// <see cref="DatagramSessionReactorHarness.PumpBothUntilDrained"/> accumulates only Event kinds and
    /// so cannot hand a caller the delivered bytes. The other end is pumped and drained to nothing: it
    /// is the sender here, and both ends of this pair send in one test or another, so which end receives
    /// is the only thing that varies between callers.
    /// </summary>
    public List<byte[]> PumpUntilDeliveredTo(DatagramSessionReactorHarness receiver)
    {
        DatagramSessionReactorHarness sender = ReferenceEquals(receiver, Dialler) ? Accepter : Dialler;
        List<byte[]> delivered = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            receiver.Reactor.Pump();
            delivered.AddRange(receiver.DrainPayloads());
            sender.Reactor.Pump();
            sender.DrainAndDiscard();
            if (delivered.Count > 0 || deadline.Expired)
            {
                return delivered;
            }

            PumpDeadline.Yield();
        }
    }

    public void Dispose()
    {
        Dialler.Dispose();
        Accepter.Dispose();
    }
}
