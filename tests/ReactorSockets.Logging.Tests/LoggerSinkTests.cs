using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace ReactorSockets.Logging.Tests;

/// <summary>
/// The adapter's mapping, observed as a logging provider would see it. Records are built with the
/// public <see cref="DiagnosticRecord"/> factories rather than provoked, because what is under test is
/// the translation of a record the core already built, not when the core builds one.
/// </summary>
public class LoggerSinkTests
{
    private static readonly LinkId _link = new(Origin: 7, Index: 2, Generation: 3);
    private static readonly PeerId _peer = new(Origin: 9, Index: 4, Generation: 5);
    private static readonly Source _source = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Parse("203.0.113.7"), 4242));

    public static TheoryData<DiagnosticLevel, LogLevel> Levels => new()
    {
        { DiagnosticLevel.Trace, LogLevel.Trace },
        { DiagnosticLevel.Info, LogLevel.Information },
        { DiagnosticLevel.Warn, LogLevel.Warning },
        { DiagnosticLevel.Error, LogLevel.Error },
    };

    public static TheoryData<DiagnosticRecord, LogLevel> RecordsAtEachBuildableLevel => new()
    {
        { DiagnosticRecord.ForRetransmissionTimerBackedOff(_peer, newRtoMilliseconds: 400, consecutiveBackoffs: 2), LogLevel.Trace },
        { DiagnosticRecord.ForLingerNegotiated(_peer, oursMilliseconds: 5000, effectiveMilliseconds: 5000), LogLevel.Information },
        { DiagnosticRecord.ForLinkFailed(_link, SocketError.ConnectionRefused), LogLevel.Warning },
    };

    [Fact]
    public void Levels_EveryDiagnosticLevel_HasARow()
    {
        // Arrange — the mapping table above is the specification, so a level added without a row fails here.
        IEnumerable<DiagnosticLevel> mapped = Levels.Select(row => (DiagnosticLevel)row[0]);

        // Act
        DiagnosticLevel[] all = Enum.GetValues<DiagnosticLevel>();

        // Assert
        mapped.Should().BeEquivalentTo(all);
    }

    [Theory]
    [MemberData(nameof(Levels))]
    public void IsEnabled_EachDiagnosticLevel_AsksTheLoggerAboutTheMappedLogLevel(DiagnosticLevel level, LogLevel expected)
    {
        // Arrange
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);

        // Act
        sink.IsEnabled(level);

        // Assert
        logger.LevelsQueried.Should().Equal(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IsEnabled_AnyLevel_ReturnsTheLoggersAnswer(bool loggerEnabled)
    {
        // Arrange
        LoggerSink sink = new(new RecordingLogger { Enabled = loggerEnabled });

        // Act
        bool enabled = sink.IsEnabled(DiagnosticLevel.Warn);

        // Assert
        enabled.Should().Be(loggerEnabled);
    }

    [Theory]
    [MemberData(nameof(RecordsAtEachBuildableLevel))]
    public void Log_ARecord_IsLoggedAtTheMappedLogLevel(DiagnosticRecord record, LogLevel expected)
    {
        // Arrange — no factory builds an Error record (ADR 0013), so Error's mapping is covered by
        // IsEnabled above and this covers the three levels a record can carry.
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);

        // Act
        sink.Log(record);

        // Assert
        logger.Entries.Should().ContainSingle().Which.Level.Should().Be(expected);
    }

    [Fact]
    public void Log_ARecord_CarriesItsIdAsTheEventId()
    {
        // Arrange
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);

        // Act
        sink.Log(DiagnosticRecord.ForLinkFailed(_link, SocketError.ConnectionRefused));

        // Assert
        logger.Entries.Should().ContainSingle().Which.EventId.Should().Be(new EventId(100, "LinkFailed"));
    }

    [Fact]
    public void Log_ANoneSubjectRecord_RendersThroughTheFormatterAndCarriesNoSubjectFields()
    {
        // Arrange
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);
        DiagnosticRecord record = DiagnosticRecord.ForAdmissionCapReached(cap: 8, declined: 3);

        // Act
        sink.Log(record);

        // Assert
        LoggedEntry entry = logger.Entries.Should().ContainSingle().Subject;
        entry.Message.Should().Be(DiagnosticFormatter.Format(record));
        entry.Fields.Should().Equal(new Dictionary<string, object?>
        {
            ["DiagnosticId"] = DiagnosticId.AdmissionCapReached,
            ["DiagnosticLevel"] = DiagnosticLevel.Warn,
            ["SubjectKind"] = DiagnosticSubjectKind.None,
            ["Value0"] = 8L,
            ["Value1"] = 3L,
        });
    }

    [Fact]
    public void Log_ALinkSubjectRecord_RendersThroughTheFormatterAndCarriesTheLinksIdentity()
    {
        // Arrange
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);
        DiagnosticRecord record = DiagnosticRecord.ForLinkFailed(_link, SocketError.ConnectionRefused);

        // Act
        sink.Log(record);

        // Assert
        LoggedEntry entry = logger.Entries.Should().ContainSingle().Subject;
        entry.Message.Should().Be(DiagnosticFormatter.Format(record));
        entry.Fields.Should().Equal(new Dictionary<string, object?>
        {
            ["DiagnosticId"] = DiagnosticId.LinkFailed,
            ["DiagnosticLevel"] = DiagnosticLevel.Warn,
            ["SubjectKind"] = DiagnosticSubjectKind.Link,
            ["Reactor"] = 7u,
            ["Slot"] = 2,
            ["Generation"] = 3UL,
            ["Value0"] = (long)SocketError.ConnectionRefused,
            ["Value1"] = 0L,
        });
    }

    [Fact]
    public void Log_APeerSubjectRecord_RendersThroughTheFormatterAndCarriesThePeersIdentity()
    {
        // Arrange
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);
        DiagnosticRecord record = DiagnosticRecord.ForLingerNegotiated(_peer, oursMilliseconds: 8000, effectiveMilliseconds: 5000);

        // Act
        sink.Log(record);

        // Assert
        LoggedEntry entry = logger.Entries.Should().ContainSingle().Subject;
        entry.Message.Should().Be(DiagnosticFormatter.Format(record));
        entry.Fields.Should().Equal(new Dictionary<string, object?>
        {
            ["DiagnosticId"] = DiagnosticId.LingerNegotiated,
            ["DiagnosticLevel"] = DiagnosticLevel.Info,
            ["SubjectKind"] = DiagnosticSubjectKind.Peer,
            ["Reactor"] = 9u,
            ["Slot"] = 4,
            ["Generation"] = 5UL,
            ["Value0"] = 8000L,
            ["Value1"] = 5000L,
        });
    }

    [Fact]
    public void Log_ASourceSubjectRecord_RendersThroughTheFormatterAndNamesTheSourceAsAClaim()
    {
        // Arrange
        RecordingLogger logger = new();
        LoggerSink sink = new(logger);
        DiagnosticRecord record = DiagnosticRecord.ForDatagramTruncated(_source, bufferSize: 16);

        // Act
        sink.Log(record);

        // Assert — the field is named as a claim for the same reason the formatter's wording is: a
        // Source is chosen by whoever sent the datagram (ADR 0013).
        LoggedEntry entry = logger.Entries.Should().ContainSingle().Subject;
        entry.Message.Should().Be(DiagnosticFormatter.Format(record));
        entry.Fields.Should().Equal(new Dictionary<string, object?>
        {
            ["DiagnosticId"] = DiagnosticId.DatagramTruncated,
            ["DiagnosticLevel"] = DiagnosticLevel.Warn,
            ["SubjectKind"] = DiagnosticSubjectKind.Source,
            ["ClaimedSource"] = "203.0.113.7:4242",
            ["Value0"] = 16L,
            ["Value1"] = 0L,
        });
    }
}
