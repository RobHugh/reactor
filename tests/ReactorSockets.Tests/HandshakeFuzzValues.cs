namespace ReactorSockets.Tests;

/// <summary>
/// The seed, clock steps and scalar draws the handshake fuzz tests of both Session profiles share.
/// Each scalar draw is either a boundary value or a uniformly random one, so the corpus hits both the
/// edges of every legal range and the rest of the field's space. A uniformly random <c>ulong</c> is
/// almost never a legal interval, so without the boundary values almost nothing would get past the
/// codec to the code that converts the announced values.
/// </summary>
internal static class HandshakeFuzzValues
{
    /// <summary>
    /// The fixed seed for every handshake corpus, chosen only so a failure is reproducible. Never
    /// re-rolled to make a failing case pass.
    /// </summary>
    public const int Seed = 20261107;

    /// <summary>
    /// The clock steps a test takes after its corpus, so every timer driven by an accepted
    /// announcement is evaluated against elapsed time from a millisecond up to beyond the largest
    /// legal interval.
    /// </summary>
    public static readonly TimeSpan[] ClockSteps =
    [
        TimeSpan.FromMilliseconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromHours(1),
        TimeSpan.FromDays(1),
        TimeSpan.FromDays(30),
        TimeSpan.FromDays(50),
    ];

    private static readonly ulong[] _intervalBoundaries =
    [
        0,
        1,
        2,
        999,
        1000,
        60_000,
        uint.MaxValue - 1,
        uint.MaxValue,
        (ulong)uint.MaxValue + 1,
        310_000_000_000_000,
        600_000_000_000_000,
        922_000_000_000_000,
        long.MaxValue,
        (ulong)long.MaxValue + 1,
        ulong.MaxValue - 1,
        ulong.MaxValue,
    ];

    private static readonly uint[] _maxMessageSizeBoundaries =
    [
        0,
        1,
        4095,
        4096,
        4097,
        int.MaxValue,
        (uint)int.MaxValue + 1,
        uint.MaxValue - 1,
        uint.MaxValue,
    ];

    /// <summary>A uniformly random <c>uint</c> over its whole range.</summary>
    public static uint NextUInt32(Random random) => (uint)random.NextInt64(0, (long)uint.MaxValue + 1);

    /// <summary>A uniformly random <c>ulong</c> over its whole range.</summary>
    public static ulong NextUInt64(Random random) => ((ulong)NextUInt32(random) << 32) | NextUInt32(random);

    /// <summary>A send-idle interval or Linger in milliseconds.</summary>
    public static ulong NextInterval(Random random) => random.Next(4) switch
    {
        0 => NextUInt64(random),
        1 => NextUInt32(random),
        _ => _intervalBoundaries[random.Next(_intervalBoundaries.Length)],
    };

    /// <summary>An announced MaxMessageSize.</summary>
    public static uint NextMaxMessageSize(Random random) => random.Next(3) switch
    {
        0 => NextUInt32(random),
        _ => _maxMessageSizeBoundaries[random.Next(_maxMessageSizeBoundaries.Length)],
    };

    /// <summary>A protocol version: mostly version 1, so the intervals are judged, otherwise any.</summary>
    public static uint NextVersion(Random random) => random.Next(5) switch
    {
        0 => NextUInt32(random),
        _ => 1,
    };

    /// <summary>A Refusal reason byte, legal or not.</summary>
    public static byte NextRefusalReason(Random random) => random.Next(2) switch
    {
        0 => (byte)random.Next(256),
        _ => (byte)random.Next(8),
    };

    /// <summary>Returns <paramref name="bytes"/> with up to three random bytes overwritten.</summary>
    public static byte[] Mutate(Random random, byte[] bytes)
    {
        byte[] mutated = [.. bytes];
        int flips = random.Next(1, 4);
        for (int i = 0; i < flips; i++)
        {
            mutated[random.Next(mutated.Length)] = (byte)random.Next(256);
        }

        return mutated;
    }

    /// <summary>Returns <paramref name="length"/> random bytes.</summary>
    public static byte[] NextBytes(Random random, int length)
    {
        byte[] bytes = new byte[length];
        random.NextBytes(bytes);
        return bytes;
    }
}
