using System.Security.Cryptography;

namespace ReactorSockets;

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
/// This type is public so the schedule can be pinned by a test without an <c>InternalsVisibleTo</c>
/// (ADR 0015): the jitter source is a constructor parameter, and a test supplies a deterministic one
/// by deriving from <see cref="RandomNumberGenerator"/>. It is a value the consumer never has to
/// build — a Reactor constructs its own — and it holds no resource.
/// </para>
/// </remarks>
public sealed class RedialSchedule
{
    /// <summary>
    /// The jitter source, drawn from once per delay. Never disposed: a supplied generator belongs to
    /// the caller, who may hand the same one to several schedules, and the default instance
    /// <see cref="RandomNumberGenerator.Create()"/> returns holds no handle — it draws from the OS on
    /// each call, so its <c>Dispose</c> has nothing to release. This is the argument
    /// <see cref="StreamSessionOptions.Entropy"/> already makes.
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
    /// The source each delay is drawn from, or <see langword="null"/> for the platform CSPRNG. The
    /// default is derived rather than guessed — the platform generator is the citable provenance, the
    /// same argument <see cref="StreamSessionOptions.Entropy"/> makes.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Either interval is zero or negative, neither of which describes a wait.
    /// </exception>
    public RedialSchedule(TimeSpan pumpInterval, TimeSpan linger, RandomNumberGenerator? jitter = null)
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

        _jitter = jitter ?? RandomNumberGenerator.Create();
        _ceilingTicks = linger.Ticks;
        _computedTicks = Math.Min(pumpInterval.Ticks, _ceilingTicks);
    }

    /// <summary>
    /// Draws the next delay and advances the schedule. The returned delay lies anywhere in
    /// <c>[0, computed]</c>, where the computed delay doubles on each call until it reaches the
    /// effective Linger and stays there.
    /// </summary>
    /// <returns>How long to wait before the next re-dial attempt.</returns>
    public TimeSpan NextDelay()
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
