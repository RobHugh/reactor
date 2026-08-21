namespace ReactorSockets;

/// <summary>
/// Measures the interval between consecutive Pump calls — <see cref="StreamSessionReactor.Pump"/> or
/// <see cref="DatagramSessionReactor.Pump"/>, each owning its own instance — from monotonic
/// <see cref="TimeProvider"/> timestamps. On the stream profile this is what makes the re-dial
/// schedule's initial delay (<see cref="Session.RedialSchedule"/>) derived rather than invented: a
/// retry sooner than the next Pump cannot happen, so the next Pump is the floor. On the datagram
/// profile the same reading floors the adaptive RTO (issue #191, slice #42) for the same reason: a
/// timeout shorter than the gap between Pumps cannot be acted on. Either way, the only way to know
/// that floor is to watch how often Pump is actually called.
/// </summary>
/// <remarks>
/// <para>
/// Public for the same reason <see cref="Session.RedialSchedule"/> is (ADR 0015): a test pins this behaviour
/// by constructing one directly against a substituted <see cref="TimeProvider"/> and calling
/// <see cref="Observe"/> the same way a Reactor's own Pump would, with no <c>InternalsVisibleTo</c>
/// and no interface invented for the purpose.
/// </para>
/// <para>
/// Only <see cref="TimeProvider.GetTimestamp"/> and <see cref="TimeProvider.GetElapsedTime(long,long)"/>
/// are ever consulted, never <c>GetUtcNow</c> — the monotonic-only discipline ADR 0002 states for
/// every timer this library owns, applied here for the first time. A consumer's own loop is
/// irregular by construction (the library owns no threads, ADR 0002), so every <see cref="Observe"/>
/// call replaces <see cref="Interval"/> outright rather than folding it into a running average: the
/// most recent cadence is what a schedule should track, not a stale first reading a later, faster or
/// slower loop has already left behind.
/// </para>
/// </remarks>
public sealed class PumpIntervalObserver
{
    private readonly TimeProvider _timeProvider;
    private long? _lastTimestamp;

    /// <summary>Builds an observer that reads timestamps from <paramref name="timeProvider"/>.</summary>
    /// <param name="timeProvider">
    /// The same <see cref="TimeProvider"/> the owning Reactor was built with — never a wall clock.
    /// </param>
    public PumpIntervalObserver(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
    }

    /// <summary>
    /// The interval between the two most recent <see cref="Observe"/> calls.
    /// </summary>
    /// <remarks>
    /// Before a second call this reads <see cref="TimeSpan.MaxValue"/> — deliberately, and documented
    /// rather than left to fall out of a default. There is no cadence to report yet, and the one thing
    /// this value is for is flooring a caller's own first computed bound against its own ceiling
    /// instead of an invented cadence: on the stream profile that is
    /// <see cref="Session.RedialSchedule"/>'s first delay (<c>Math.Min(pumpInterval, linger)</c>), and
    /// on the datagram profile it is the adaptive RTO's clamp against Linger (issue #191). Reporting
    /// the largest possible interval defers entirely to that ceiling rather than inventing a cadence
    /// that has never actually been observed. Zero would have done the opposite — floor every bound at
    /// immediately, the one value <see cref="Session.RedialSchedule"/>'s own constructor already
    /// refuses to accept.
    /// </remarks>
    public TimeSpan Interval { get; private set; } = TimeSpan.MaxValue;

    /// <summary>
    /// Records one Pump's timestamp and, once a previous one exists, updates <see cref="Interval"/>
    /// to the elapsed time since it. Intended to be called exactly once per Pump.
    /// </summary>
    public void Observe()
    {
        long now = _timeProvider.GetTimestamp();
        if (_lastTimestamp is long last)
        {
            Interval = _timeProvider.GetElapsedTime(last, now);
        }

        _lastTimestamp = now;
    }
}
