using FluentAssertions;
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
            SendOutcome.RemoteMessageTooLarge,
            SendOutcome.NotEstablished,
        ];

        // Act
        int distinctCount = outcomes.Distinct().Count();

        // Assert
        distinctCount.Should().Be(outcomes.Length);
    }

    [Fact]
    public void MessageTooLargeAndRetentionExhausted_AreNeitherTheZeroDefault()
    {
        // Arrange — subtask 7.2.1 (issue #241). Both refusals must be values a caller had to be
        // handed, never the value an uninitialised SendOutcome already carries: a default(SendOutcome)
        // that read as a refusal would let a field nobody assigned pass for a decision the Reactor
        // made. Sent owns zero, which is the one member it is safe for.
        SendOutcome uninitialised = default;

        // Act & Assert
        uninitialised.Should().Be(SendOutcome.Sent, "zero is Sent's value, so it is the only member a default can be");
        SendOutcome.MessageTooLarge.Should().NotBe(uninitialised);
        SendOutcome.RetentionExhausted.Should().NotBe(uninitialised);
        ((int)SendOutcome.MessageTooLarge).Should().NotBe(0);
        ((int)SendOutcome.RetentionExhausted).Should().NotBe(0);
    }
}
