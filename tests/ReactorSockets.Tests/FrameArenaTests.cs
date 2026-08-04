using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class FrameArenaTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_ArenaSizeBelowOne_ThrowsArgumentOutOfRangeException(int arenaSize)
    {
        // Arrange
        // Act
        Action act = () => new FrameArena(arenaSize, capacity: 4);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("arenaSize");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_CapacityBelowOne_ThrowsArgumentOutOfRangeException(int capacity)
    {
        // Arrange
        // Act
        Action act = () => new FrameArena(arenaSize: 1024, capacity);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("capacity");
    }

    [Theory]
    [InlineData(65_507, 4)]
    [InlineData(65_507, 64)]
    [InlineData(2048, 3)]
    [InlineData(1, 1)]
    public void ReadShare_ArenaSizeAndCapacity_IsArenaSizeDividedByCapacity(int arenaSize, int capacity)
    {
        // Arrange
        FrameArena arena = new(arenaSize, capacity);

        // Act
        int readShare = arena.ReadShare;

        // Assert
        readShare.Should().Be(arenaSize / capacity);
    }

    [Fact]
    public void Reserve_LargerArenaAtSameCapacity_YieldsProportionallyLargerShare()
    {
        // Arrange
        FrameArena smallArena = new(arenaSize: 6000, capacity: 3);
        FrameArena largerArena = new(arenaSize: 12_000, capacity: 3);

        // Act
        int smallShare = smallArena.ReadShare;
        int largerShare = largerArena.ReadShare;

        // Assert
        largerShare.Should().Be(smallShare * 2);
    }

    [Fact]
    public void Reserve_FirstRequestForAShareHolderExceedingItsShare_IsCommittedViaTheEscape()
    {
        // Arrange
        FrameArena arena = new(arenaSize: 65_507, capacity: 4); // ReadShare = 16 376
        byte[] unit = new byte[20_000]; // exceeds the 16 376-byte share

        // Act
        ArenaReservation reservation = arena.Reserve(shareHolderId: 0, unit);

        // Assert
        reservation.Outcome.Should().Be(ArenaReservationOutcome.Committed);
        reservation.Payload.Length.Should().Be(unit.Length);
    }

    [Fact]
    public void Reserve_SecondRequestForSameShareHolderExceedingRemainingShare_IsRefusedAsShareExhausted()
    {
        // Arrange
        FrameArena arena = new(arenaSize: 65_507, capacity: 4); // ReadShare = 16 376
        byte[] unit = new byte[20_000]; // exceeds the share, so the first request must escape
        arena.Reserve(shareHolderId: 0, unit);

        // Act
        ArenaReservation reservation = arena.Reserve(shareHolderId: 0, unit);

        // Assert
        reservation.Outcome.Should().Be(ArenaReservationOutcome.ShareExhausted);
        reservation.Payload.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void Reserve_EachShareHolderExceedingItsShareOnce_YieldsExactlyOneUnitPerShareHolderThisFrame()
    {
        // Arrange — the composed default behaviour ADR 0014 describes: at the Raw default arena
        // (one maximum datagram) shared across several Links, any unit that exceeds ReadShare
        // escapes on a share-holder's first request and is refused on its second.
        FrameArena arena = new(arenaSize: 65_507, capacity: 4); // ReadShare = 16 376
        byte[] unit = new byte[20_000]; // exceeds the share

        // Act
        ArenaReservation firstHolderFirstRead = arena.Reserve(shareHolderId: 0, unit);
        ArenaReservation secondHolderFirstRead = arena.Reserve(shareHolderId: 1, unit);
        ArenaReservation firstHolderSecondRead = arena.Reserve(shareHolderId: 0, unit);
        ArenaReservation secondHolderSecondRead = arena.Reserve(shareHolderId: 1, unit);

        // Assert
        firstHolderFirstRead.Outcome.Should().Be(ArenaReservationOutcome.Committed);
        secondHolderFirstRead.Outcome.Should().Be(ArenaReservationOutcome.Committed);
        firstHolderSecondRead.Outcome.Should().Be(ArenaReservationOutcome.ShareExhausted);
        secondHolderSecondRead.Outcome.Should().Be(ArenaReservationOutcome.ShareExhausted);
    }

    [Fact]
    public void Reserve_RequestExceedingArenaFreeSpace_IsRefusedAsArenaExhaustedCarryingRequestedAndFreeBytes()
    {
        // Arrange
        FrameArena arena = new(arenaSize: 1000, capacity: 2);
        arena.Reserve(shareHolderId: 0, new byte[900]); // BytesFree is now 100
        byte[] tooLarge = new byte[150];

        // Act
        ArenaReservation reservation = arena.Reserve(shareHolderId: 1, tooLarge);

        // Assert
        reservation.Outcome.Should().Be(ArenaReservationOutcome.ArenaExhausted);
        reservation.BytesRequested.Should().Be(150);
        reservation.BytesFree.Should().Be(100);
    }

    [Fact]
    public void Reserve_AfterArenaExhausted_FrameStillCompletesAndResetRecoversFullCapacity()
    {
        // Arrange
        FrameArena arena = new(arenaSize: 1000, capacity: 2);
        arena.Reserve(shareHolderId: 0, new byte[900]);
        arena.Reserve(shareHolderId: 1, new byte[150]); // refused: ArenaExhausted

        // Act
        arena.Reset();

        // Assert
        arena.BytesUsed.Should().Be(0);
        arena.BytesFree.Should().Be(1000);
    }

    [Fact]
    public void Reserve_ShareHolderIdOutsideCapacity_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        FrameArena arena = new(arenaSize: 1024, capacity: 4);

        // Act
        Action act = () => arena.Reserve(shareHolderId: 4, new byte[10]);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("shareHolderId");
    }

    [Fact]
    public void Reset_BytesCommittedInPriorFrame_AreOverwrittenByTheNextFrame()
    {
        // Arrange — proves reuse by observation rather than by asserting a crash: the same backing
        // offset is handed out again next Frame, so data from Frame N reads back as Frame N+1's
        // bytes if a consumer were to (incorrectly) keep using it.
        FrameArena arena = new(arenaSize: 64, capacity: 1);
        ArenaReservation frameOne = arena.Reserve(shareHolderId: 0, [1, 2, 3]);
        Span<byte> frameOnePayload = frameOne.Payload;
        frameOnePayload[0].Should().Be(1);

        // Act
        arena.Reset();
        arena.Reserve(shareHolderId: 0, [9, 9, 9]);

        // Assert
        frameOnePayload[0].Should().Be(9);
    }
}
