using System.Net.Sockets;

namespace ReactorSockets.Core;

/// <summary>
/// The readiness dispatch loop L0 runs inside every Pump (ADR 0001, ADR 0002): one
/// <see cref="Socket.Select(System.Collections.IList?, System.Collections.IList?, System.Collections.IList?, int)"/>
/// scan of every registered Handle, followed by a non-preemptive, synchronous callback for each
/// Handle found ready. This is Schmidt's Reactor pattern with ownership inverted â€” there is no
/// <c>handle_events()</c> loop that owns the process and no <c>Initiation_Dispatcher::instance()</c>
/// singleton; a consumer's own loop calls <see cref="Pump"/> exactly when it decides to, and every
/// dispatcher instance is independent, so several can run on several threads at once.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Pump"/> never blocks: the underlying <c>Select</c> call always passes a zero
/// microsecond timeout, so a Pump reports only Handles ready at the instant it is called and never
/// waits for one to become ready. Nothing this dispatcher touches ever advances unless
/// <see cref="Pump"/> is called â€” there is no background thread, no timer, and no queue
/// accumulating work between Pumps.
/// </para>
/// <para>
/// The registered set is scanned starting from an offset that rotates by one Handle every Pump, so
/// unfairness does not accumulate on the same Handle across many Pumps. This only spreads
/// unfairness <i>across</i> Pumps; bounding how much of a single Pump one Handle can consume is the
/// frame arena's fair-share job (<see cref="FrameArena"/>), not this dispatcher's.
/// </para>
/// <para>
/// The buffer passed to <c>Select</c> is a single <see cref="List{T}"/> field cleared and refilled
/// every Pump rather than reallocated, because <c>Select</c> takes non-generic
/// <see cref="System.Collections.IList"/>s and mutates the one it is given in place â€” passing a
/// fresh list every Frame would allocate every Frame.
/// </para>
/// <para>
/// A long Frame â€” one in which the consumer does not call <see cref="Pump"/> again for a while â€”
/// does not drain the OS's own receive buffer. <c>SO_RCVBUF</c> sizing is a tuning concern for the
/// consumer; a caller that stalls between Pumps drops UDP datagrams exactly as it would with any
/// other socket API, and this dispatcher does not defend against that.
/// </para>
/// </remarks>
internal sealed class ReadinessDispatcher
{
    private readonly TimeProvider _timeProvider;
    private readonly int _constructingThreadId;
    private readonly List<Socket> _registered = [];
    private readonly Dictionary<Socket, Action> _onReadable = [];
    private readonly List<Socket> _scanBuffer = [];
    private int _rotationOffset;
    private long? _lastPumpTimestamp;

    /// <summary>
    /// Constructs a dispatcher bound to the calling thread (ADR 0002) â€” every future
    /// <see cref="Pump"/> call must come from this same thread or it throws.
    /// </summary>
    /// <param name="timeProvider">
    /// The source of monotonic time for <see cref="ObservedPumpInterval"/>. Defaults to
    /// <see cref="System.TimeProvider.System"/>; only <c>GetTimestamp</c>/<c>GetElapsedTime</c> are
    /// ever consulted, never <c>GetUtcNow</c>.
    /// </param>
    public ReadinessDispatcher(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _constructingThreadId = Environment.CurrentManagedThreadId;
    }

    /// <summary>
    /// The elapsed monotonic time between the two most recent <see cref="Pump"/> calls, or
    /// <see langword="null"/> until a second Pump has happened. Later slices derive the RTO floor
    /// and the re-dial schedule from this observed interval.
    /// </summary>
    public TimeSpan? ObservedPumpInterval { get; private set; }

    /// <summary>
    /// Registers <paramref name="socket"/> as a Handle: <paramref name="onReadable"/> runs
    /// synchronously, on the <see cref="Pump"/> caller's thread, the first Pump after the socket has
    /// bytes to read. Handles are non-preemptive â€” one Handle's callback always runs to completion
    /// before the next one starts. Registering the same socket twice is a programmer error and
    /// throws <see cref="ArgumentException"/>.
    /// </summary>
    public void Register(Socket socket, Action onReadable)
    {
        ArgumentNullException.ThrowIfNull(socket);
        ArgumentNullException.ThrowIfNull(onReadable);

        _onReadable.Add(socket, onReadable);
        _registered.Add(socket);
    }

    /// <summary>
    /// Removes a Handle previously registered with <see cref="Register"/>; a no-op if it was never
    /// registered or was already removed.
    /// </summary>
    public void Unregister(Socket socket)
    {
        _onReadable.Remove(socket);
        _registered.Remove(socket);
    }

    /// <summary>
    /// One synchronous advance: checks which registered Handles are ready right now and invokes
    /// each ready Handle's callback, scanning in an order that starts from a rotating offset so the
    /// same Handle is not always served first across many Pumps. Throws
    /// <see cref="InvalidOperationException"/> if called from any thread other than the one that
    /// constructed this dispatcher.
    /// </summary>
    public void Pump()
    {
        EnsureCallingThreadIsConstructingThread();
        RecordObservedInterval();

        int count = _registered.Count;
        if (count == 0)
        {
            return;
        }

        _scanBuffer.Clear();
        int offset = _rotationOffset % count;
        for (int i = 0; i < count; i++)
        {
            _scanBuffer.Add(_registered[(offset + i) % count]);
        }

        _rotationOffset = (_rotationOffset + 1) % count;

        Socket.Select(_scanBuffer, null, null, 0);

        foreach (Socket socket in _scanBuffer)
        {
            if (_onReadable.TryGetValue(socket, out Action? onReadable))
            {
                onReadable();
            }
        }
    }

    private void EnsureCallingThreadIsConstructingThread()
    {
        int callingThreadId = Environment.CurrentManagedThreadId;
        if (callingThreadId != _constructingThreadId)
        {
            throw new InvalidOperationException(
                $"Pump was called from thread {callingThreadId}, but this dispatcher was constructed on thread {_constructingThreadId} â€” a Reactor may only be pumped from the thread that constructed it.");
        }
    }

    private void RecordObservedInterval()
    {
        long now = _timeProvider.GetTimestamp();
        if (_lastPumpTimestamp is long previous)
        {
            ObservedPumpInterval = _timeProvider.GetElapsedTime(previous, now);
        }

        _lastPumpTimestamp = now;
    }
}
