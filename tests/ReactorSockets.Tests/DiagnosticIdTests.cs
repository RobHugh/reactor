using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class DiagnosticIdTests
{
    [Theory]
    [InlineData(DiagnosticId.AdmissionCapReached, 1)]
    [InlineData(DiagnosticId.ArenaExhausted, 2)]
    [InlineData(DiagnosticId.CapacityExhausted, 3)]
    [InlineData(DiagnosticId.LinkFailed, 100)]
    [InlineData(DiagnosticId.DestinationUnreachable, 101)]
    [InlineData(DiagnosticId.DatagramTruncated, 102)]
    [InlineData(DiagnosticId.DatagramMalformed, 200)]
    [InlineData(DiagnosticId.CookieRejected, 201)]
    [InlineData(DiagnosticId.VersionRefused, 202)]
    [InlineData(DiagnosticId.PeerRefused, 300)]
    [InlineData(DiagnosticId.LingerNegotiated, 301)]
    [InlineData(DiagnosticId.MalformedFraming, 302)]
    [InlineData(DiagnosticId.DataAfterGoodbye, 303)]
    [InlineData(DiagnosticId.DeliveryCounterRefused, 308)]
    public void Values_EveryMember_MatchesItsReservedBlockNumber(DiagnosticId id, int expected)
    {
        // Arrange
        // Act
        int actual = (int)id;

        // Assert
        actual.Should().Be(expected);
    }

    [Fact]
    public void Values_NoMemberIsZero_BecauseEveryRecordHasACauseByConstruction()
    {
        // Arrange
        DiagnosticId[] members = Enum.GetValues<DiagnosticId>();

        // Act
        bool anyZero = members.Any(id => (int)id == 0);

        // Assert
        anyZero.Should().BeFalse();
    }
}
