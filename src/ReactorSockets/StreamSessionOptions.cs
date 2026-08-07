namespace ReactorSockets;

/// <summary>
/// The construction surface of a <see cref="StreamSessionReactor"/> (ADR 0011): the shared L0 budget
/// composed in as <see cref="Reactor"/>, plus the knobs this profile alone has. Composed rather than
/// inherited, so that nothing protocol-specific here can ever leak into <see cref="ReactorOptions"/>
/// and turn it into the base class ADR 0011 refused.
/// </summary>
/// <remarks>
/// <para>
/// A knob carries a default only when that default is <em>derived</em> — from machinery this library
/// owns, from another knob, or from a citable standard. A value that trades the consumer's own
/// resources against their own traffic, and that nothing in this library can derive, is required with
/// no default: <see cref="MaxMessageSize"/>, <see cref="RetentionBytes"/>,
/// <see cref="SendIdleInterval"/> and <see cref="Linger"/> are all of that kind, and so is
/// <see cref="ReactorOptions.Capacity"/>. Guessing any of them would be guessing at the consumer's
/// application.
/// </para>
/// <para>
/// There is no cookie, no ack, no sequence space, no fragment and no RTO here: TCP already provides
/// what those were built for, so a knob to configure one would have nothing under it.
/// </para>
/// <para>
/// <b>The memory bill.</b> A Reactor built from these options preallocates
/// <c>Capacity × (RetentionBytes + receive buffer)</c> bytes, all of it per Peer and none of it
/// shared, so a <c>Send</c> on a healthy Peer never fails because of somebody else's traffic. This is
/// buffered <em>twice</em> — once in our retention, once again in the kernel's own send buffer for the
/// socket — so a stream Peer's real footprint is this bound plus whatever the OS is holding, and the
/// OS's half is not fully ours to control.
/// </para>
/// <para>
/// <see cref="MaxMessageSize"/> appears in three places at once: announced to the remote as a receive
/// limit, flooring the receive buffer, and flooring <see cref="RetentionBytes"/>. It is therefore the
/// one knob whose careless enlargement multiplies memory in two directions rather than one.
/// </para>
/// <para>
/// Every check below is deterministic and entirely the caller's, so failures throw at construction —
/// <see cref="ArgumentOutOfRangeException"/> for a single bad value and <see cref="ArgumentException"/>
/// for a combination that cannot work — rather than being reported through a <c>Result</c>.
/// </para>
/// </remarks>
public sealed class StreamSessionOptions
{
    /// <summary>
    /// The default number of consecutive missed heartbeats before a Peer is declared gone: RFC 4271's
    /// hold time, three times the keepalive interval. Two would make a single lost heartbeat fatal;
    /// three is the smallest count that survives one loss, and is the count a standard already
    /// defends.
    /// </summary>
    private const int DefaultMissedHeartbeats = 3;

    /// <summary>The width of the <c>u32</c> length prefix every framed Message carries.</summary>
    private const int LengthPrefixSize = 4;

    /// <summary>
    /// The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
    /// Required — a Reactor cannot be built without one, and its <see cref="ReactorOptions.Capacity"/>
    /// has no default of its own.
    /// </summary>
    public required ReactorOptions Reactor { get; init; }

    /// <summary>
    /// The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
    /// statement about the consumer's own protocol that nothing here can derive. It is also
    /// announced to the remote as a receive limit, and floors both the receive buffer and
    /// <see cref="RetentionBytes"/> — an arena that cannot hold one maximum Message could never
    /// deliver one.
    /// </summary>
    public required int MaxMessageSize { get; init; }

    /// <summary>
    /// The bytes retained per Peer for the send path — retention <em>is</em> the send path (ADR 0011):
    /// a <c>Send</c> frames the Message once and appends it to this Peer's ring, and what reaches the
    /// socket is a copy from it. Required: this is the consumer trading their memory against their
    /// tolerance for a stalled Peer, and the right answer depends on their traffic, not on anything
    /// here. Must be at least <see cref="MaxMessageSize"/> plus the 4-byte length prefix, or a single
    /// maximum Message could never be retained.
    /// </summary>
    public required int RetentionBytes { get; init; }

    /// <summary>
    /// How long a Session may go without sending before it sends a heartbeat instead. Required:
    /// shorter costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the
    /// consumer knows which they would rather spend.
    /// </summary>
    public required TimeSpan SendIdleInterval { get; init; }

    /// <summary>
    /// How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
    /// flight. Required, for the same reason as <see cref="SendIdleInterval"/>: it trades a prompt
    /// teardown against a clean one.
    /// </summary>
    public required TimeSpan Linger { get; init; }

    /// <summary>
    /// The bytes held per Peer for an incoming, not-yet-complete Message. Defaults to
    /// <see cref="MaxMessageSize"/> plus the 4-byte length prefix — its own floor, since that is
    /// exactly one incomplete maximum Message and nothing narrower could ever hold one.
    /// </summary>
    public int? ReceiveBufferSize { get; init; }

    /// <summary>
    /// How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
    /// Defaults to 3, RFC 4271's hold-time multiple. Must be at least 2, because a count of 1 makes a
    /// single lost heartbeat fatal, and in .NET the stall that produces it is as ordinary as a
    /// collection pause.
    /// </summary>
    public int? MissedHeartbeats { get; init; }

    /// <summary>The floor both <see cref="RetentionBytes"/> and the receive buffer are held to: one maximum Message plus its length prefix.</summary>
    public int ReceiveBufferFloor => MaxMessageSize + LengthPrefixSize;

    /// <summary>The resolved receive buffer: <see cref="ReceiveBufferSize"/> if set, otherwise <see cref="ReceiveBufferFloor"/>.</summary>
    public int ResolvedReceiveBufferSize => ReceiveBufferSize ?? ReceiveBufferFloor;

    /// <summary>The resolved missed-heartbeat count: <see cref="MissedHeartbeats"/> if set, otherwise 3.</summary>
    public int ResolvedMissedHeartbeats => MissedHeartbeats ?? DefaultMissedHeartbeats;

    /// <summary>
    /// The receive line of the memory bill in bytes, as a <see langword="long"/> so that a
    /// combination too large to allocate is a value this can report rather than an overflow it
    /// silently wraps through.
    /// </summary>
    public long ReceiveBudgetBytes => (long)Reactor.Capacity * ResolvedReceiveBufferSize;

    /// <summary>The send line of the memory bill in bytes, reported the same way.</summary>
    public long SendBudgetBytes => (long)Reactor.Capacity * RetentionBytes;

    /// <summary>
    /// Validates every knob and every cross-check, throwing before a Reactor allocates anything.
    /// Internal because a consumer never validates an options value themselves — the constructor
    /// calls this, so a combination that cannot work throws at construction and nowhere else.
    /// </summary>
    internal void Validate()
    {
        Reactor.Validate();

        if (MaxMessageSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxMessageSize),
                MaxMessageSize,
                $"maximum message size of {MaxMessageSize} bytes carries nothing — it must be at least 1.");
        }

        int receiveBufferFloor = ReceiveBufferFloor;

        if (RetentionBytes < receiveBufferFloor)
        {
            throw new ArgumentException(
                $"retention of {RetentionBytes} bytes cannot hold one Message of {MaxMessageSize} bytes plus its " +
                $"4-byte length prefix — raise RetentionBytes to at least {receiveBufferFloor}, or lower MaxMessageSize.",
                nameof(RetentionBytes));
        }

        int receiveBuffer = ResolvedReceiveBufferSize;
        if (receiveBuffer < receiveBufferFloor)
        {
            throw new ArgumentException(
                $"a receive buffer of {receiveBuffer} bytes cannot hold one incomplete Message of {MaxMessageSize} bytes " +
                $"plus its 4-byte length prefix — raise ReceiveBufferSize to at least {receiveBufferFloor}, or lower MaxMessageSize.",
                nameof(ReceiveBufferSize));
        }

        int missedHeartbeats = ResolvedMissedHeartbeats;
        if (missedHeartbeats < 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MissedHeartbeats),
                missedHeartbeats,
                $"a missed-heartbeat count of {missedHeartbeats} declares a Peer gone on a single lost heartbeat — it must be at least 2.");
        }

        if (SendIdleInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SendIdleInterval),
                SendIdleInterval,
                $"a send-idle interval of {SendIdleInterval} never elapses, so no heartbeat would ever be sent — it must be greater than zero.");
        }

        if (Linger <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Linger),
                Linger,
                $"a Linger of {Linger} closes a Session before it can retransmit its Goodbye — it must be greater than zero.");
        }

        ValidateMemoryBill();
    }

    /// <summary>
    /// Resolves the frame arena against this profile's floor — one maximum Message, since an arena
    /// too small to hold one could never deliver one.
    /// </summary>
    internal int ResolveArenaSize() =>
        Reactor.ResolveArenaSize(MaxMessageSize, $"one maximum Message of {MaxMessageSize} bytes");

    /// <summary>
    /// Rejects a combination whose preallocation could not be made, naming both lines of the memory
    /// bill and their total. The check is arithmetic rather than an attempted allocation because the
    /// point is to tell the caller which knob to change, and a caught allocation failure knows only
    /// that something was too big.
    /// </summary>
    private void ValidateMemoryBill()
    {
        long receive = ReceiveBudgetBytes;
        long send = SendBudgetBytes;
        long total = receive + send;

        if (total > Array.MaxLength)
        {
            throw new ArgumentException(
                $"these options ask for {total} bytes of preallocation, which cannot be allocated. " +
                $"Receive is Capacity {Reactor.Capacity} × {ResolvedReceiveBufferSize} bytes = {receive}; " +
                $"send is Capacity {Reactor.Capacity} × {RetentionBytes} bytes = {send}. " +
                "Lower Capacity, MaxMessageSize or RetentionBytes.",
                nameof(Reactor));
        }
    }
}
