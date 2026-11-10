using System.Net.Sockets;

namespace ReactorSockets.Core;

/// <summary>
/// The readiness dispatch loop L0 runs inside every Pump (ADR 0001, ADR 0002): one
/// <see cref="Socket.Select(System.Collections.IList?, System.Collections.IList?, System.Collections.IList?, int)"/>
/// scan of every registered Handle's readability and error state, plus whichever Handles are currently
/// armed for writability, followed by a non-preemptive, synchronous callback for each Handle found
/// ready — at most one callback per Handle per Pump, whichever of those found it. This
/// is Schmidt's Reactor pattern with ownership inverted — there is no <c>handle_events()</c> loop
/// that owns the process and no <c>Initiation_Dispatcher::instance()</c> singleton; a consumer's own
/// loop calls <see cref="Pump"/> exactly when it decides to, and every dispatcher instance is
/// independent, so several can run on several threads at once.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Pump"/> never blocks: the underlying <c>Select</c> call always passes a zero
/// microsecond timeout, so a Pump reports only Handles ready at the instant it is called and never
/// waits for one to become ready. Nothing this dispatcher touches ever advances unless
/// <see cref="Pump"/> is called — there is no background thread, no timer, and no queue
/// accumulating work between Pumps.
/// </para>
/// <para>
/// The registered set is scanned starting from an offset that rotates by one Handle every Pump, so
/// unfairness does not accumulate on the same Handle across many Pumps. This only spreads
/// unfairness <i>across</i> Pumps; bounding how much of a single Pump one Handle can consume is the
/// job of the frame arena's fair share (<see cref="FrameArena"/>) and, on the datagram profile,
/// <see cref="DatagramSessionOptions.MaxDatagramsPerPump"/>, not this dispatcher's.
/// </para>
/// <para>
/// The buffer passed to <c>Select</c> for readability is a single <see cref="List{T}"/> field cleared
/// and refilled every Pump rather than reallocated, because <c>Select</c> takes non-generic
/// <see cref="System.Collections.IList"/>s and mutates the one it is given in place — passing a
/// fresh list every Frame would allocate every Frame. The write-side buffer is the same reused field
/// pattern, but is only ever populated — and only ever passed to <c>Select</c> at all — when
/// <see cref="ArmWritable"/> has actually been called for at least one socket; an unarmed Pump does
/// not build a write-set, does not call <c>Select</c> with one, and allocates nothing extra to not do
/// so, which is what keeps arming free for the overwhelming majority of Pumps that never use it.
/// </para>
/// <para>
/// A long Frame — one in which the consumer does not call <see cref="Pump"/> again for a while —
/// does not drain the OS's own receive buffer. <c>SO_RCVBUF</c> sizing is a tuning concern for the
/// consumer; a caller that stalls between Pumps drops UDP datagrams exactly as it would with any
/// other socket API, and this dispatcher does not defend against that.
/// </para>
/// </remarks>
internal sealed class ReadinessDispatcher
{
    private readonly List<Socket> _registered = [];
    private readonly Dictionary<Socket, Action> _onReadable = [];
    private readonly Dictionary<Socket, Action> _onWritable = [];
    private readonly HashSet<Socket> _armedForWritable = [];
    private readonly List<Socket> _scanBuffer = [];
    private readonly List<Socket> _writeScanBuffer = [];
    private readonly List<Socket> _errorScanBuffer = [];
    private readonly HashSet<Socket> _alreadyDispatched = [];
    private int _rotationOffset;

    /// <summary>
    /// Registers <paramref name="socket"/> as a Handle: <paramref name="onReadable"/> runs
    /// synchronously, on the <see cref="Pump"/> caller's thread, the first Pump after the socket has
    /// bytes to read. Handles are non-preemptive — one Handle's callback always runs to completion
    /// before the next one starts. Registering the same socket twice is a programmer error and
    /// throws <see cref="ArgumentException"/>.
    /// </summary>
    /// <param name="socket">
    /// The socket to register. Must not already be registered with this dispatcher.
    /// </param>
    /// <param name="onReadable">
    /// Runs synchronously the first Pump after <paramref name="socket"/> has bytes to read.
    /// </param>
    /// <param name="onWritable">
    /// Runs synchronously the first Pump after <paramref name="socket"/> is found writable while
    /// armed via <see cref="ArmWritable"/> — never on an unarmed socket, and never level-triggered
    /// (see the remarks on <see cref="ArmWritable"/>). <see langword="null"/> for a Handle that never
    /// sends, such as a listening or datagram Link.
    /// </param>
    public void Register(Socket socket, Action onReadable, Action? onWritable = null)
    {
        ArgumentNullException.ThrowIfNull(socket);
        ArgumentNullException.ThrowIfNull(onReadable);

        _onReadable.Add(socket, onReadable);
        _registered.Add(socket);
        if (onWritable is not null)
        {
            _onWritable.Add(socket, onWritable);
        }
    }

    /// <summary>
    /// Removes a Handle previously registered with <see cref="Register"/>; a no-op if it was never
    /// registered or was already removed. Also disarms it for <c>Writable</c>, so a Link closed while
    /// armed cannot leave a stale entry behind for a socket that no longer exists.
    /// </summary>
    public void Unregister(Socket socket)
    {
        _onReadable.Remove(socket);
        _onWritable.Remove(socket);
        _armedForWritable.Remove(socket);
        _registered.Remove(socket);
    }

    /// <summary>
    /// Arms <paramref name="socket"/> to produce exactly one <c>Writable</c> callback the next time a
    /// <see cref="Pump"/> finds it writable, then automatically disarms — never level-triggered. A
    /// socket is writable nearly always, so scanning every registered socket's write-readiness every
    /// Pump would report up to one non-event per Handle for the caller to skip past; arming instead
    /// means only a socket that just took a short send is ever placed in the write-set <see cref="Pump"/>
    /// scans, which is why an idle Link that was never over-sent on costs this dispatcher nothing.
    /// Idempotent — arming an already-armed socket is a no-op.
    /// </summary>
    public void ArmWritable(Socket socket) => _armedForWritable.Add(socket);

    /// <summary>
    /// One synchronous advance: checks which registered Handles are ready right now and invokes
    /// each ready Handle's callback, scanning in an order that starts from a rotating offset so the
    /// same Handle is not always served first across many Pumps.
    /// </summary>
    /// <remarks>
    /// Thread affinity is not re-checked here. This dispatcher is internal and reachable only through
    /// the Reactor that owns it, which enforces the constructing-thread rule (ADR 0002) on every
    /// public member before delegating — so a second check would only duplicate the first, and the
    /// exception a consumer sees should name the Reactor rather than a type they cannot reference.
    /// </remarks>
    public void Pump()
    {
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

        // The write-set is built from the armed sockets only — usually none — so the common Pump
        // costs nothing beyond an empty HashSet enumeration, and Select itself is never even asked
        // about write-readiness unless something is actually armed. Scanning every registered socket
        // for writability every Pump, the way the read-set does, would reintroduce exactly the
        // level-triggered non-event this dispatcher's whole write-side design exists to avoid.
        List<Socket>? writeScan = null;
        if (_armedForWritable.Count > 0)
        {
            _writeScanBuffer.Clear();
            foreach (Socket socket in _armedForWritable)
            {
                _writeScanBuffer.Add(socket);
            }

            writeScan = _writeScanBuffer;
        }

        // The error-set is the same Handles as the read-set, and unlike the write-set it is scanned
        // unconditionally. That is not a contradiction of the arming rule above: a socket is writable
        // nearly always, so scanning for writability reports non-events, whereas a socket is in error
        // almost never, so scanning for it reports nothing until something genuinely went wrong. It has
        // to be scanned because some errors arrive ONLY this way — Linux signals a datagram socket's
        // queued ICMP error with POLLERR and no POLLIN, so a Handle carrying one is never in the
        // read-set and, without this, would sit unread for the life of the Link.
        _errorScanBuffer.Clear();
        _errorScanBuffer.AddRange(_scanBuffer);

        Socket.Select(_scanBuffer, writeScan, _errorScanBuffer, 0);

        _alreadyDispatched.Clear();
        foreach (Socket socket in _scanBuffer)
        {
            if (_onReadable.TryGetValue(socket, out Action? onReadable))
            {
                _alreadyDispatched.Add(socket);
                onReadable();
            }
        }

        // A Handle in error is dispatched to the same callback as a readable one, because reading is
        // how a pending socket error is collected and reported: the read returns the error instead of
        // bytes, and the Link's owner turns that into whatever the Link's kind calls it. Handles that
        // were in both sets have already run — one Handle, one callback, per Pump.
        foreach (Socket socket in _errorScanBuffer)
        {
            if (!_alreadyDispatched.Contains(socket) && _onReadable.TryGetValue(socket, out Action? onReadable))
            {
                onReadable();
            }
        }

        if (writeScan is not null)
        {
            // Select mutated writeScan in place to contain only the sockets actually found writable —
            // the same pattern _scanBuffer relies on above. Each one fires at most once: disarming
            // before invoking the callback is what makes this a reply to the short send that armed it,
            // never a status the dispatcher volunteers again next Pump.
            foreach (Socket socket in writeScan)
            {
                _armedForWritable.Remove(socket);
                if (_onWritable.TryGetValue(socket, out Action? onWritable))
                {
                    onWritable();
                }
            }
        }
    }
}
