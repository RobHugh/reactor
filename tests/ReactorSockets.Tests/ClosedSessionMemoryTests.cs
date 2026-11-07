using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;
using static ReactorSockets.Tests.PacketRecordBytes;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="ClosedSessionMemory"/> decides whether a datagram naming a closed Session is answered and
/// with which ack blocks. Public for the same reason <see cref="FragmentGroup"/> is (ADR 0015). The
/// reactor-level re-ack is pinned in <see cref="DatagramSessionGoodbyeReAckThroughProxyTests"/>.
/// </summary>
public class ClosedSessionMemoryTests
{
    private const uint LocalSessionId = 0x1111_1111;
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly Source _remote = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 40_000));
    private static readonly Source _elsewhere = Source.FromIPEndPoint(new IPEndPoint(IPAddress.Loopback, 40_001));

    /// <summary>The Peer's final blocks: ordered base 5 with 6 and 7 already received, unordered base 9.</summary>
    private static readonly ClosedSessionAck _finalAck = new(
        _remote,
        RemoteSessionId: 0x2222_2222,
        ReliableOrdered: new ReactorSockets.AckBlock(Base: 5, Bits: 0b11),
        ReliableUnordered: new ReactorSockets.AckBlock(Base: 9, Bits: 0));

    [Fact]
    public void TryReAck_SessionNeverRemembered_Declines()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());

        // Act
        bool answered = memory.TryReAck(LocalSessionId, _remote, Datagram(Goodbye(5)), out _);

        // Assert
        answered.Should().BeFalse();
    }

    [Fact]
    public void TryReAck_GoodbyeAtTheNextOrderedSequence_AcksItAndEverythingAlreadyReceivedPastIt()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);

        // Act
        memory.TryReAck(LocalSessionId, _remote, Datagram(Goodbye(5)), out ClosedSessionAck ack);

        // Assert — 5 is the Goodbye, 6 and 7 were already held, so the next expected is 8.
        ack.Should().Be(_finalAck with { ReliableOrdered = new ReactorSockets.AckBlock(Base: 8, Bits: 0) });
    }

    [Fact]
    public void TryReAck_GoodbyeBehindAHole_AnswersWithTheStoredBlocksUnchanged()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);

        // Act
        memory.TryReAck(LocalSessionId, _remote, Datagram(Goodbye(9)), out ClosedSessionAck ack);

        // Assert
        ack.Should().Be(_finalAck);
    }

    [Fact]
    public void TryReAck_OrderedMessageAtTheNextSequence_AnswersWithoutAcknowledgingIt()
    {
        // Arrange — nothing is delivered after the close, so a Message is never acked.
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);

        // Act
        memory.TryReAck(LocalSessionId, _remote, Datagram(Record(ModeReliableOrdered, 5, [0x01])), out ClosedSessionAck ack);

        // Assert
        ack.Should().Be(_finalAck);
    }

    [Fact]
    public void TryReAck_FromAnotherSource_Declines()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);

        // Act
        bool answered = memory.TryReAck(LocalSessionId, _elsewhere, Datagram(Goodbye(5)), out _);

        // Assert
        answered.Should().BeFalse();
    }

    [Fact]
    public void TryReAck_NoReliableOrderedRecord_Declines()
    {
        // Arrange — a header-only reply carries no ordered record, so two closed ends never ping-pong.
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);

        // Act
        bool answered = memory.TryReAck(LocalSessionId, _remote, Datagram(Record(ModeUnreliable, 1, [0x01])), out _);

        // Assert
        answered.Should().BeFalse();
    }

    [Fact]
    public void TryReAck_TruncatedRecord_Declines()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);
        byte[] datagram = Datagram(Goodbye(5));

        // Act
        bool answered = memory.TryReAck(LocalSessionId, _remote, datagram.AsSpan(0, datagram.Length - 1), out _);

        // Assert
        answered.Should().BeFalse();
    }

    [Fact]
    public void TryReAck_AtTheLingerDeadline_DeclinesAndForgetsTheSession()
    {
        // Arrange
        FakeTimeProvider time = new();
        ClosedSessionMemory memory = new(capacity: 2, time);
        memory.Remember(LocalSessionId, _finalAck, _linger);
        time.Advance(_linger);

        // Act
        bool answered = memory.TryReAck(LocalSessionId, _remote, Datagram(Goodbye(5)), out _);

        // Assert
        new { answered, held = memory.Holds(LocalSessionId) }.Should().Be(new { answered = false, held = false });
    }

    [Fact]
    public void TryReAck_JustInsideTheLinger_Answers()
    {
        // Arrange
        FakeTimeProvider time = new();
        ClosedSessionMemory memory = new(capacity: 2, time);
        memory.Remember(LocalSessionId, _finalAck, _linger);
        time.Advance(_linger - TimeSpan.FromMilliseconds(1));

        // Act
        bool answered = memory.TryReAck(LocalSessionId, _remote, Datagram(Goodbye(5)), out _);

        // Assert
        answered.Should().BeTrue();
    }

    [Fact]
    public void Remember_MoreSessionsThanCapacity_OverwritesTheOldest()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());

        // Act
        memory.Remember(1, _finalAck, _linger);
        memory.Remember(2, _finalAck, _linger);
        memory.Remember(3, _finalAck, _linger);

        // Assert
        new[] { memory.Holds(1), memory.Holds(2), memory.Holds(3) }.Should().Equal(false, true, true);
    }

    [Fact]
    public void Remember_SameSessionAgainAfterItsEntryWasOverwritten_KeepsTheNewerEntry()
    {
        // Arrange — session 1's first entry sits at ring 0; it is remembered again at ring 1 before
        // ring 0 is overwritten, and that overwrite must not drop the newer entry's index.
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(1, _finalAck, _linger);
        memory.Remember(1, _finalAck, _linger);

        // Act
        memory.Remember(2, _finalAck, _linger);

        // Assert
        memory.Holds(1).Should().BeTrue();
    }

    [Fact]
    public void ForgetAll_AfterRemembering_HoldsNothing()
    {
        // Arrange
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());
        memory.Remember(LocalSessionId, _finalAck, _linger);

        // Act
        memory.ForgetAll();

        // Assert
        memory.Holds(LocalSessionId).Should().BeFalse();
    }

    [Fact]
    public void Remember_SessionIdZero_Throws()
    {
        // Arrange — zero marks an unused ring entry and is the control session id.
        ClosedSessionMemory memory = new(capacity: 2, new FakeTimeProvider());

        // Act
        Action act = () => memory.Remember(0, _finalAck, _linger);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    /// <summary>Builds a data datagram: a 20-byte header naming <see cref="LocalSessionId"/>, then <paramref name="records"/>.</summary>
    private static byte[] Datagram(byte[] records) => [.. Header(LocalSessionId), .. records];
}
