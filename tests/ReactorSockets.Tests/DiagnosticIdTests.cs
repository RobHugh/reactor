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
    [InlineData(DiagnosticId.ResumeMatched, 304)]
    [InlineData(DiagnosticId.ResumeUnmatched, 305)]
    [InlineData(DiagnosticId.RedialAttempted, 306)]
    [InlineData(DiagnosticId.RedialFailed, 307)]
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
    public void Values_EveryMember_HasAValueNoOtherMemberShares()
    {
        // Arrange — C# lets two enum members share a value silently, which would make one of them an
        // alias for the other in every Sink that switches on the id. The per-member test above pins
        // each value it knows about; this is the guard that a member added later cannot land on one
        // already taken.
        DiagnosticId[] members = Enum.GetValues<DiagnosticId>();

        // Act
        int distinctValues = members.Select(id => (int)id).Distinct().Count();

        // Assert
        distinctValues.Should().Be(members.Length);
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
