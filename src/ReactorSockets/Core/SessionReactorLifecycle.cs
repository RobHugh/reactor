namespace ReactorSockets.Core;

/// <summary>
/// The single-threaded lifecycle guard both Session profiles share (ADR 0002, ADR 0005): which thread
/// constructed the owning Reactor, whether a Pump is currently running, and whether it has been
/// disposed. A composed field rather than a shared base type: both Reactors are public sealed types,
/// and a base type here would have to be at least as accessible as they are, which would leak this
/// internal L0 concept onto their public surface for nothing. Each Reactor keeps its own thin,
/// differently-worded forwarding members — the checks and their messages live here once.
/// </summary>
internal sealed class SessionReactorLifecycle
{
    public SessionReactorLifecycle()
    {
        ConstructingThreadId = Environment.CurrentManagedThreadId;
    }

    /// <summary>The thread that constructed the owning Reactor — the only thread ever allowed to drive it.</summary>
    public int ConstructingThreadId { get; }

    /// <summary>Set for the duration of a Pump call.</summary>
    public bool InPump { get; set; }

    /// <summary>Set once the owning Reactor's <c>Dispose</c> has run. Every other member throws afterwards.</summary>
    public bool IsDisposed { get; set; }

    /// <summary>Throws if <paramref name="owner"/> is disposed or a Pump is already in progress.</summary>
    public void EnsureUsable(object owner)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, owner);
        EnsureNotInsideAPump();
    }

    /// <summary>
    /// Rejects re-entry (ADR 0005). The only caller this library hands control to mid-Pump is a
    /// Diagnostic Sink, so this is what stops a Sink acting on the Reactor it is reporting on.
    /// </summary>
    public void EnsureNotInsideAPump()
    {
        if (InPump)
        {
            throw new InvalidOperationException(
                "This Reactor is already inside Pump — a Diagnostic Sink may not call back into the Reactor it is reporting on. Copy what the record carries and act on it after Pump returns.");
        }
    }

    public void EnsureCallingThreadIsConstructingThread()
    {
        int callingThreadId = Environment.CurrentManagedThreadId;
        if (callingThreadId != ConstructingThreadId)
        {
            throw new InvalidOperationException(
                $"Called from thread {callingThreadId}, but this Reactor was constructed on thread {ConstructingThreadId} — a Reactor may only be driven from the thread that constructed it.");
        }
    }
}
