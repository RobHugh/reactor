using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 7.2.2 (issue #242): the arithmetic <see cref="FragmentPlan.Resolve"/> uses to decide how a
/// Message is cut, pinned directly — no Session, no Peer, no reactor — the way ADR 0015 lets a pure
/// algorithmic type be tested when it is made public for exactly that reason.
/// </summary>
public class FragmentPlanTests
{
    private const int FragmentSize = 1200;

    public static TheoryData<DeliveryMode> ReliableModes => new()
    {
        DeliveryMode.ReliableOrdered,
        DeliveryMode.ReliableUnordered,
    };

    public static TheoryData<DeliveryMode> UnreliableModes => new()
    {
        DeliveryMode.Unreliable,
        DeliveryMode.UnreliableSequenced,
    };

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Resolve_ReliableModeOneByte_ReturnsOneFragment(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(1, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(1);
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Resolve_ReliableModeExactlyOneFragmentSize_ReturnsOneFragment(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(FragmentSize, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(1);
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Resolve_ReliableModeOneByteOverOneFragmentSize_ReturnsTwoFragments(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(FragmentSize + 1, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(2);
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Resolve_ReliableModeExactMultipleOfFragmentSize_ReturnsExactQuotient(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(FragmentSize * 5, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(5);
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Resolve_ReliableModeAtMaxMessageSize_ReturnsCeilingOfTheDivision(DeliveryMode mode)
    {
        // Arrange — deliberately not an exact multiple, so the ceiling (not the truncated quotient)
        // is what proves this is exercising rounding rather than a coincidence of the numbers chosen.
        const int maxMessageSize = (FragmentSize * 37) + 1;

        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(maxMessageSize, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(38, "one byte past 37 whole Fragments still needs a 38th to carry it");
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Resolve_ReliableModeZeroLengthMessage_ReturnsOneFragmentRatherThanZero(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(0, FragmentSize, mode);

        // Assert — the one case the plain ceiling formula gets wrong on its own: an empty Message is
        // still one Message, and no Message occupies zero Fragments.
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(1);
    }

    [Theory]
    [MemberData(nameof(UnreliableModes))]
    public void Resolve_UnreliableModeZeroLengthMessage_ReturnsOneFragment(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(0, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(1);
    }

    [Theory]
    [MemberData(nameof(UnreliableModes))]
    public void Resolve_UnreliableModeAtTheCap_ReturnsOneFragment(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(FragmentSize, FragmentSize, mode);

        // Assert
        result.IsMessageTooLarge.Should().BeFalse();
        result.FragmentCount.Should().Be(1);
    }

    [Theory]
    [MemberData(nameof(UnreliableModes))]
    public void Resolve_UnreliableModeOneByteOverTheCap_ReturnsMessageTooLarge(DeliveryMode mode)
    {
        // Act
        FragmentPlanResult result = FragmentPlan.Resolve(FragmentSize + 1, FragmentSize, mode);

        // Assert — distinct from any success: an unreliable Message this wide is refused, never cut,
        // because nothing retransmits an unreliable Fragment if it is lost (ADR 0004).
        result.IsMessageTooLarge.Should().BeTrue();
    }

    [Fact]
    public void Resolve_AnUndefinedDeliveryMode_ThrowsRatherThanGuessingAnAnswer()
    {
        // Arrange — a value forced through a cast, which no caller can construct through the public
        // DeliveryMode enum; this is our bug to catch, not a remote input to answer for silently.
        const DeliveryMode undefinedMode = (DeliveryMode)99;

        // Act
        Action resolve = () => FragmentPlan.Resolve(1, FragmentSize, undefinedMode);

        // Assert
        resolve.Should().Throw<ArgumentOutOfRangeException>();
    }
}
