using System.Diagnostics;

namespace ReactorSockets.Tests;

/// <summary>
/// The real-time bound a harness pump loop runs against. A Pump is a zero-timeout
/// <see cref="System.Net.Sockets.Socket.Select(System.Collections.IList?, System.Collections.IList?, System.Collections.IList?, int)"/>,
/// so counting Pump calls budgets syscalls rather than time: five of them span roughly fifteen
/// microseconds. Windows loopback occasionally takes about a millisecond to make a fresh connection
/// acceptable or its first record readable — measured at 5 in 3000 fresh connections on the resume
/// path, and 0.2% of 200,000 bare loopback connections — which a syscall budget cannot cover at any
/// count (issue #153). Linux has no such gap: the same 3000 resumes each settled on the first Pump.
/// </summary>
/// <remarks>
/// Spending real time inside a pump loop is safe precisely because a Reactor reads no real clock:
/// every timer it owns is on the harness's <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>,
/// so nothing time-dependent fires while a loop waits here. Only a record that was late arrives.
/// </remarks>
internal readonly struct PumpDeadline
{
    /// <summary>
    /// How long a pump loop waits before giving up. Far longer than the stall it exists for, because
    /// overshooting costs nothing — a loop returns the moment its condition holds — while undershooting
    /// is the whole defect. A test asserting an event that genuinely never arrives pays this once.
    /// </summary>
    public static readonly TimeSpan Default = TimeSpan.FromSeconds(2);

    private readonly long _expiresAt;

    private PumpDeadline(long expiresAt) => _expiresAt = expiresAt;

    /// <summary>Starts a deadline <paramref name="budget"/> from now, or <see cref="Default"/> from now.</summary>
    public static PumpDeadline Start(TimeSpan? budget = null) =>
        new(Stopwatch.GetTimestamp()
            + (long)((budget ?? Default).TotalSeconds * Stopwatch.Frequency));

    /// <summary>Whether the budget is spent, which is how a pump loop stops rather than hanging.</summary>
    public bool Expired => Stopwatch.GetTimestamp() >= _expiresAt;

    /// <summary>
    /// Waits between Pumps once one has come up empty. Yielding rather than spinning: what this loop
    /// is waiting on is a deferral inside the platform's stack, which no amount of spinning shortens,
    /// and a spin would burn a core for the whole budget on the one path that actually times out.
    /// </summary>
    public static void Yield() => Thread.Sleep(1);
}
