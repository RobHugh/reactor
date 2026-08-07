using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class DiagnosticRecordTests
{
    [Fact]
    public void ForAdmissionCapReached_CapAndDeclined_CarriesNoSubjectAndWarnLevel()
    {
        // Arrange
        // Act
        DiagnosticRecord record = DiagnosticRecord.ForAdmissionCapReached(cap: 64, declined: 3);

        // Assert
        record.Id.Should().Be(DiagnosticId.AdmissionCapReached);
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.None);
        record.Value0.Should().Be(64);
        record.Value1.Should().Be(3);
    }

    [Fact]
    public void ForCapacityExhausted_CapacityAndDeclined_CarriesNoSubjectAndWarnLevel()
    {
        // Arrange
        // Act
        DiagnosticRecord record = DiagnosticRecord.ForCapacityExhausted(capacity: 8, declined: 1);

        // Assert
        record.Id.Should().Be(DiagnosticId.CapacityExhausted);
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.None);
        record.Value0.Should().Be(8);
        record.Value1.Should().Be(1);
    }

    [Fact]
    public void ForArenaExhausted_LinkBytesRequestedAndFree_CarriesLinkSubject()
    {
        // Arrange
        LinkId link = new(Origin: 1, Index: 2, Generation: 1);

        // Act
        DiagnosticRecord record = DiagnosticRecord.ForArenaExhausted(link, bytesRequested: 20_000, bytesFree: 100);

        // Assert
        record.Id.Should().Be(DiagnosticId.ArenaExhausted);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Link);
        record.Link.Should().Be(link);
        record.Value0.Should().Be(20_000);
        record.Value1.Should().Be(100);
    }

    [Fact]
    public void ForLinkFailed_SocketError_CarriesErrorInValue0()
    {
        // Arrange
        LinkId link = new(Origin: 1, Index: 0, Generation: 5);

        // Act
        DiagnosticRecord record = DiagnosticRecord.ForLinkFailed(link, SocketError.ConnectionReset);

        // Assert
        record.Id.Should().Be(DiagnosticId.LinkFailed);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Link);
        record.Link.Should().Be(link);
        record.Value0.Should().Be((long)SocketError.ConnectionReset);
    }

    [Fact]
    public void ForDestinationUnreachable_SocketError_CarriesErrorInValue0()
    {
        // Arrange
        LinkId link = new(Origin: 1, Index: 1, Generation: 0);

        // Act
        DiagnosticRecord record = DiagnosticRecord.ForDestinationUnreachable(link, SocketError.ConnectionReset);

        // Assert
        record.Id.Should().Be(DiagnosticId.DestinationUnreachable);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Link);
        record.Value0.Should().Be((long)SocketError.ConnectionReset);
    }

    [Fact]
    public void ForDatagramTruncated_SourceAndBufferSize_CarriesSourceSubject()
    {
        // Arrange
        Source source = new(SourceFamily.IPv4, AddressBytes: 0x7F000001, ScopeId: 0, Port: 5000);

        // Act
        DiagnosticRecord record = DiagnosticRecord.ForDatagramTruncated(source, bufferSize: 65_507);

        // Assert
        record.Id.Should().Be(DiagnosticId.DatagramTruncated);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.Should().Be(source);
        record.Value0.Should().Be(65_507);
    }
}
