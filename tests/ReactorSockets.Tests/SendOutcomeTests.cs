using FluentAssertions;
using ReactorSockets;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Confirms <see cref="SendOutcome"/>'s explicit values stay distinct, so a consumer that has
/// persisted one continues to mean what it meant when a new member is appended.
/// </summary>
public class SendOutcomeTests
{
    [Fact]
    public void MessageTooLarge_ComparedToTheOtherOutcomes_IsADistinctValue()
    {
        // Arrange
        SendOutcome[] outcomes =
        [
            SendOutcome.Sent,
            SendOutcome.RetentionExhausted,
            SendOutcome.Closed,
            SendOutcome.MessageTooLarge,
        ];

        // Act
        int distinctCount = outcomes.Distinct().Count();

        // Assert
        distinctCount.Should().Be(outcomes.Length);
    }
}
