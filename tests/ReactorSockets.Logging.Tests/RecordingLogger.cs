using Microsoft.Extensions.Logging;

namespace ReactorSockets.Logging.Tests;

/// <summary>One call to <see cref="ILogger.Log{TState}"/>, copied out so it outlives the call.</summary>
internal sealed record LoggedEntry(
    string Category,
    LogLevel Level,
    EventId EventId,
    IReadOnlyDictionary<string, object?> Fields,
    string Message);

/// <summary>
/// An <see cref="ILogger"/> that keeps every entry and every level it was asked about, so a test reads
/// what a logging provider would have received.
/// </summary>
/// <param name="category">The category every entry is recorded under.</param>
/// <param name="stream">
/// The list entries are appended to. Loggers given the same list write one stream, as loggers from one
/// <see cref="ILoggerFactory"/> do; a logger given none keeps its own.
/// </param>
internal sealed class RecordingLogger(string category = "", List<LoggedEntry>? stream = null) : ILogger
{
    /// <summary>Whether <see cref="IsEnabled"/> answers yes.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Every entry logged, in the order it arrived.</summary>
    public List<LoggedEntry> Entries { get; } = stream ?? [];

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
        Entries.Add(new LoggedEntry(category, logLevel, eventId, fields, formatter(state, exception)));
    }
}

/// <summary>
/// An <see cref="ILoggerFactory"/> whose every logger writes to one <see cref="Entries"/> list under its
/// own category: the setup the package README recommends, with the providers replaced by a list.
/// </summary>
internal sealed class RecordingLoggerFactory : ILoggerFactory
{
    /// <summary>Every entry any logger from this factory wrote, in the order it arrived.</summary>
    public List<LoggedEntry> Entries { get; } = [];

    public ILogger CreateLogger(string categoryName) => new RecordingLogger(categoryName, Entries);

    public void AddProvider(ILoggerProvider provider) =>
        throw new NotSupportedException("this factory records entries itself and takes no providers.");

    public void Dispose()
    {
    }
}
