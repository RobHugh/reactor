using Microsoft.Extensions.Logging;

namespace ReactorSockets.Logging.Tests;

/// <summary>One call to <see cref="ILogger.Log{TState}"/>, copied out so it outlives the call.</summary>
internal sealed record LoggedEntry(
    LogLevel Level,
    EventId EventId,
    IReadOnlyDictionary<string, object?> Fields,
    string Message);

/// <summary>
/// An <see cref="ILogger"/> that keeps every entry and every level it was asked about, so a test reads
/// what a logging provider would have received. Every adapter in a test shares one of these, which is
/// what makes its <see cref="Entries"/> one stream.
/// </summary>
internal sealed class RecordingLogger : ILogger
{
    /// <summary>Whether <see cref="IsEnabled"/> answers yes.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Every entry logged, in the order it arrived.</summary>
    public List<LoggedEntry> Entries { get; } = [];

    /// <summary>Every level <see cref="IsEnabled"/> was asked about, in order.</summary>
    public List<LogLevel> LevelsQueried { get; } = [];

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel)
    {
        LevelsQueried.Add(logLevel);
        return Enabled;
    }

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        Dictionary<string, object?> fields = state is IEnumerable<KeyValuePair<string, object?>> pairs
            ? pairs.ToDictionary(pair => pair.Key, pair => pair.Value)
            : [];
        Entries.Add(new LoggedEntry(logLevel, eventId, fields, formatter(state, exception)));
    }
}
