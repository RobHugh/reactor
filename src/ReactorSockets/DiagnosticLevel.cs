namespace ReactorSockets;

/// <summary>
/// The minimum severity a <see cref="IReactorDiagnostics"/> Sink was installed to receive.
/// Ordered by ascending severity so that a level is enabled when it is at least as severe
/// as the installed <c>MinimumLevel</c>.
/// </summary>
public enum DiagnosticLevel
{
    /// <summary>The most verbose level: everything a Sink installed at this level receives.</summary>
    Trace,

    /// <summary>Routine, expected events worth recording but not acting on.</summary>
    Info,

    /// <summary>Recoverable conditions a consumer may want to notice.</summary>
    Warn,

    /// <summary>Conditions that end a Link or a Peer.</summary>
    Error,
}
