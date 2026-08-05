namespace ReactorSockets;

/// <summary>
/// The budget L0 spends inside a Pump, shared by every profile. Composed as a member into each
/// profile's options type rather than inherited from a base (ADR 0012) — nothing protocol-specific
/// may ever enter this type, since a knob two of three profiles need is exactly what would turn it
/// into the base class ADR 0011 refused.
/// </summary>
/// <remarks>
/// A knob carries a default only when that default is derived — from machinery the library owns,
/// from another knob, or from a citable standard (ADR 0011, ADR 0012). <see cref="Capacity"/> has
/// no such derivation and is therefore required, with no default. Construction throws
/// <see cref="ArgumentOutOfRangeException"/> for a single bad value and <see cref="ArgumentException"/>
/// for a combination that cannot work, because every check here is deterministic and entirely the
/// caller's — unlike <c>Send</c>, whose failures belong to the peer or the network and are reported
/// through a <c>Result</c> instead.
/// </remarks>
public sealed class ReactorOptions
{
    /// <summary>
    /// The number of share-holders this Reactor admits — Links in Raw, Peers in a Session
    /// (ADR 0014) — and the multiplier on every preallocation. Required; there is no default
    /// derivable from anything the library owns (ADR 0011, superseding ADR 0001's "default 64").
    /// Left unset it defaults to 0, which <see cref="Validate"/> rejects.
    /// </summary>
    public int Capacity { get; init; }

    /// <summary>
    /// The per-Pump bound on accepts and handshakes alike. Defaults to <see cref="Capacity"/>:
    /// one Pump never spends more admission work than a full Reactor's worth, and the remainder
    /// waits in the OS backlog or is dropped (ADR 0012).
    /// </summary>
    public int? AdmissionCap { get; init; }

    /// <summary>
    /// The frame arena's size in bytes. Left unset, a Reactor defaults it to its own profile
    /// floor — one maximum deliverable unit, since the floor is profile-specific and only the
    /// profile's constructor knows what it must exceed (ADR 0012). A value below that floor throws
    /// at construction, naming the arithmetic.
    /// </summary>
    public int? ArenaSize { get; init; }

    /// <summary>
    /// The source of monotonic time for every duration this library measures. Defaults to
    /// <see cref="System.TimeProvider.System"/>; only <c>GetTimestamp</c>/<c>GetElapsedTime</c>
    /// are ever consulted, never <c>GetUtcNow</c>.
    /// </summary>
    /// <remarks>
    /// The Raw profile measures no durations, so nothing in this slice reads this. It lives on the
    /// shared options value because it is the same knob every profile takes, and the Session
    /// profiles' liveness deadlines, retransmit timers and re-dial schedules are its first readers.
    /// </remarks>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>
    /// The Diagnostic Sink records are sent to, or <see langword="null"/> to disable diagnostics
    /// entirely. Must be supplied together with <see cref="MinimumLevel"/> — both present or both
    /// absent — because with no Sink there is nowhere for a record to go, so no level can matter
    /// (ADR 0012).
    /// </summary>
    public IReactorDiagnostics? Sink { get; init; }

    /// <summary>
    /// The minimum <see cref="DiagnosticLevel"/> the installed <see cref="Sink"/> receives.
    /// Must be supplied together with <see cref="Sink"/> — neither carries a default, since a
    /// default level would be a guess at how much diagnostic volume the consumer wants.
    /// </summary>
    public DiagnosticLevel? MinimumLevel { get; init; }

    /// <summary>The resolved admission cap: <see cref="AdmissionCap"/> if set, otherwise <see cref="Capacity"/>.</summary>
    public int ResolvedAdmissionCap => AdmissionCap ?? Capacity;

    /// <summary>
    /// Validates every knob that does not depend on a profile's arena floor, throwing
    /// <see cref="ArgumentOutOfRangeException"/> for a single bad value or
    /// <see cref="ArgumentException"/> for a combination that cannot work. Internal because a
    /// consumer never validates an options value themselves — every profile's constructor calls
    /// this, so a combination that cannot work throws at construction and nowhere else.
    /// </summary>
    internal void Validate()
    {
        if (Capacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Capacity),
                Capacity,
                $"capacity of {Capacity} admits no share-holders — capacity must be at least 1.");
        }

        if (AdmissionCap is int admissionCap && admissionCap < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(AdmissionCap),
                admissionCap,
                $"admission cap of {admissionCap} admits no accepts or handshakes in a Pump — admission cap must be at least 1.");
        }

        if (Sink is null != MinimumLevel is null)
        {
            string sinkState = Sink is null ? "absent" : "present";
            string levelState = MinimumLevel is null ? "absent" : "present";
            throw new ArgumentException(
                $"Sink was {sinkState} while MinimumLevel was {levelState} — supply both to enable diagnostics or neither to disable them.",
                Sink is null ? nameof(MinimumLevel) : nameof(Sink));
        }
    }

    /// <summary>
    /// Applies <see cref="ArenaSize"/> against a profile's arena floor — one maximum deliverable
    /// unit — defaulting to the floor when unset, and throwing <see cref="ArgumentException"/>
    /// naming the arithmetic when an explicit value falls short of it.
    /// </summary>
    /// <param name="arenaFloor">The profile's minimum arena size, in bytes.</param>
    /// <param name="floorDescription">
    /// What the floor represents in the calling profile, e.g. "one maximum UDP datagram of 65 507
    /// bytes", used verbatim in the failure message.
    /// </param>
    internal int ResolveArenaSize(int arenaFloor, string floorDescription)
    {
        int arenaSize = ArenaSize ?? arenaFloor;
        if (arenaSize < arenaFloor)
        {
            throw new ArgumentException(
                $"arena of {arenaSize} bytes cannot hold {floorDescription} — raise ArenaSize to at least {arenaFloor}.",
                nameof(ArenaSize));
        }

        return arenaSize;
    }
}
