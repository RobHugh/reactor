using System.Security.Cryptography;

namespace ReactorSockets.Session;

/// <summary>
/// The delays a dialling side waits between re-dial attempts after its transport is lost: it starts
/// at the observed Pump interval, doubles, is ceilinged at the effective Linger, and every delay is
/// drawn with full jitter across <c>[0, computed]</c>.
/// </summary>
/// <remarks>
/// <para>
/// The schedule is derived, not configured (ADR 0011). Its two inputs are things the Reactor already
/// knows — the cadence the consumer pumps at, and the Linger they declared — so the conventional
/// initial, maximum and attempt-count knobs would have been three numbers a consumer had to invent
/// before they could reconnect a socket. Linger bounds the activity on its own: a retry after Linger
/// has nothing left to reconnect to, so there is no attempt limit here and there must not be one.
/// </para>
/// <para>
/// Jitter is stated rather than buried. Many instances talking to one shared remote will re-dial from
/// the same event, and an undithered doubling schedule synchronises them into bursts arriving exactly
/// when the far end is least able to absorb them. Full jitter — the whole range from zero, not a
/// fixed fraction of it — is what spreads them.
/// </para>
/// <para>
/// The type is internal, so the consumer's surface stays as small as ADR 0003 and ADR 0011 leave it —
/// the schedule is derived and cannot be set, and a public constructor would have been a permanent
/// commitment to a value no consumer ever builds. The jitter source is a constructor parameter rather
/// than a source this type creates, so a test substitutes a deterministic one through the options
/// seam a Reactor draws it from, and the schedule is observed through the Reactor that re-dials on it.
/// Nothing here is reached past the seam and no <c>InternalsVisibleTo</c> exists (ADR 0015).
/// </para>
/// </remarks>
internal sealed class RedialSchedule
{
    /// <summary>
    /// The jitter source, drawn from once per delay. Never disposed: the generator belongs to the
    /// consumer who supplied it, who may hand the same one to several Reactors — the rule
    /// <see cref="StreamSessionOptions.Entropy"/> already states, and this schedule is handed that same
    /// generator rather than creating one of its own.
    /// </summary>
    private readonly RandomNumberGenerator _jitter;

    /// <summary>The ceiling every computed delay is held to: the effective Linger, in ticks.</summary>
    private readonly long _ceilingTicks;

    /// <summary>The computed delay for the next attempt, in ticks, already held to <see cref="_ceilingTicks"/>.</summary>
    private long _computedTicks;

    /// <summary>
    /// Builds a schedule from the observed Pump interval and the effective Linger.
    /// </summary>
    /// <param name="pumpInterval">
    /// The interval the consumer's loop is observed to Pump at, which is the first computed delay: a
    /// retry sooner than the next Pump cannot happen, so the next Pump is the floor.
    /// </param>
    /// <param name="linger">
    /// The effective Linger, which is the ceiling: past it there is nothing left to reconnect to.
    /// A Linger shorter than <paramref name="pumpInterval"/> simply means every computed delay is the
    /// Linger.
    /// </param>
    /// <param name="jitter">
    /// The source each delay is drawn from. Required and never defaulted here: the caller is a
    /// Reactor, which already holds the consumer's own generator, and a second default would be a
    /// second answer to a question <see cref="StreamSessionOptions.Entropy"/> has already settled.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Either interval is zero or negative, neither of which describes a wait.
    /// </exception>
    internal RedialSchedule(TimeSpan pumpInterval, TimeSpan linger, RandomNumberGenerator jitter)
    {
        if (pumpInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pumpInterval),
                pumpInterval,
                $"an observed Pump interval of {pumpInterval} is not a cadence a loop could have — it must be greater than zero.");
        }

        if (linger <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(linger),
                linger,
                $"a Linger of {linger} leaves no window to re-dial in — it must be greater than zero.");
        }

        _jitter = jitter;
        _ceilingTicks = linger.Ticks;
        _computedTicks = Math.Min(pumpInterval.Ticks, _ceilingTicks);
    }

    /// <summary>
    /// Draws the next delay and advances the schedule. The returned delay lies anywhere in
    /// <c>[0, computed]</c>, where the computed delay doubles on each call until it reaches the
    /// effective Linger and stays there.
    /// </summary>
    /// <returns>How long to wait before the next re-dial attempt.</returns>
    internal TimeSpan NextDelay()
    {
        long drawnTicks = Draw(_computedTicks);

        // Doubling is done on the ceilinged value and re-ceilinged, so the multiplication can never
        // run away however many attempts are asked for. The test is written as a subtraction rather
        // than as `_computedTicks >= _ceilingTicks / 2`, which truncates and so clamps one doubling
        // early on an odd ceiling, and rather than `(_ceilingTicks + 1) / 2`, which overflows at
        // TimeSpan.MaxValue.
        _computedTicks = _ceilingTicks - _computedTicks <= _computedTicks
            ? _ceilingTicks
            : _computedTicks * 2;

        return TimeSpan.FromTicks(drawnTicks);
    }

    /// <summary>
    /// Draws uniformly from <c>[0, ticks]</c>, scaling one 64-bit draw into the range rather than
    /// rejecting and redrawing: a substituted jitter source that returns a constant would never
    /// escape a rejection loop, and a schedule that could spin on its own test double is worse than
    /// the negligible bias scaling leaves behind.
    /// </summary>
    private long Draw(long ticks)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        _jitter.GetBytes(bytes);
        ulong drawn = BitConverter.ToUInt64(bytes);

        return (long)((UInt128)drawn * (UInt128)((ulong)ticks + 1) >> 64);
    }
}
