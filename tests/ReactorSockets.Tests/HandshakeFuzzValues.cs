namespace ReactorSockets.Tests;

/// <summary>
/// Draws the scalar fields a hostile handshake announces for the handshake fuzz tests. Each draw is
/// either a boundary value or a uniformly random one, so the corpus hits both the edges of every legal
/// range and the rest of the field's space. A uniformly random <c>ulong</c> is almost never a legal
/// interval, so without the boundary values almost nothing would get past the codec to the code
/// that converts the announced values.
/// </summary>
internal static class HandshakeFuzzValues
{
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

    /// <summary>A send-idle interval or Linger in milliseconds.</summary>
    public static ulong NextInterval(Random random) => random.Next(4) switch
    {
        0 => (ulong)random.NextInt64(),
        1 => (uint)random.Next() | ((uint)random.Next(2) << 31),
        _ => _intervalBoundaries[random.Next(_intervalBoundaries.Length)],
    };

    /// <summary>An announced MaxMessageSize.</summary>
    public static uint NextMaxMessageSize(Random random) => random.Next(3) switch
    {
        0 => (uint)random.Next() | ((uint)random.Next(2) << 31),
        _ => _maxMessageSizeBoundaries[random.Next(_maxMessageSizeBoundaries.Length)],
    };

    /// <summary>A protocol version: mostly version 1, so the intervals are judged, otherwise any.</summary>
    public static uint NextVersion(Random random) => random.Next(5) switch
    {
        0 => (uint)random.Next() | ((uint)random.Next(2) << 31),
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
}
