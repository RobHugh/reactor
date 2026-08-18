namespace ReactorSockets;

/// <summary>
/// The zero-allocation enumerator every Session Reactor's <c>Drain</c> returns (ADR 0010), shared by
/// both profiles via <see cref="ISessionEventSource"/> rather than duplicated per profile. A
/// <see langword="ref struct"/> so that a <c>foreach</c> over it can yield <see cref="SessionEvent"/> —
/// itself a <see langword="ref struct"/> — without a heap-allocated <c>IEnumerator&lt;T&gt;</c>,
/// following the same pattern as <see cref="Span{T}"/>'s own enumerator.
/// </summary>
public ref struct SessionEventEnumerator
{
    private readonly ISessionEventSource _source;
    private int _index;

    internal SessionEventEnumerator(ISessionEventSource source)
    {
        _source = source;
        _index = -1;
    }

    /// <summary>Returns this enumerator, so <c>Drain()</c> itself is usable directly in a <c>foreach</c>.</summary>
    public readonly SessionEventEnumerator GetEnumerator() => this;

    /// <summary>Advances to the next pending event, if one remains this Frame.</summary>
    public bool MoveNext() => ++_index < _source.PendingEventCount;

    /// <summary>The event at the enumerator's current position.</summary>
    public readonly SessionEvent Current => _source.GetPendingEvent(_index);
}
