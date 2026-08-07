using System.Buffers.Binary;
using System.Security.Cryptography;

namespace ReactorSockets.Session;

/// <summary>
/// The stateless-cookie machinery for the datagram Session handshake (ADR 0009): lets a Reactor prove
/// that a sender can receive at the address it claims without allocating a single byte of per-sender
/// state until that proof arrives. A cookie is an HMAC tag a Reactor mints unilaterally and a client
/// must echo back verbatim before anything is committed to it — forging one requires the secret this
/// type holds, and holding it never requires remembering the client.
/// </summary>
/// <remarks>
/// Two secrets are held at once, indexed by <c>epoch &amp; 1</c>, so that a cookie minted a moment
/// before a rotation still verifies afterwards: <see cref="Verify"/> accepts the current epoch and the
/// one before it, giving every client at least
/// <see cref="DatagramSessionProtocol.CookieEpochSeconds"/> to complete a round trip and never less.
/// Epoch numbers are private bookkeeping about this instance's own secrets — two Reactors never
/// compare them, so no shared clock is needed or assumed.
/// </remarks>
internal sealed class CookieMinter
{
    /// <summary>
    /// The exact size in bytes of the HMAC input: family(1) + address(16) + scopeId(8) + port(4) +
    /// clientNonce(8) + epoch(8). Named rather than left as a magic number because it is the one place
    /// the wire order below must stay in lock-step with the buffer it is written into.
    /// </summary>
    private const int MacInputLength = 1 + 16 + 8 + 4 + 8 + 8;

    private readonly TimeProvider _timeProvider;
    private readonly RandomNumberGenerator _random;
    private readonly long _startTimestamp;
    private readonly byte[][] _secrets;

    /// <summary>
    /// The epoch this instance currently mints and accepts under, advancing lazily as time passes.
    /// Never compared against another Reactor's value — see the type-level remarks.
    /// </summary>
    public long CurrentEpoch { get; private set; }

    /// <summary>
    /// Creates a minter with two freshly generated epoch secrets, timed from this moment. Neither
    /// argument may be <see langword="null"/>.
    /// </summary>
    /// <param name="timeProvider">
    /// The source of monotonic time epochs are measured against. Only <c>GetTimestamp</c> and
    /// <c>GetElapsedTime</c> are ever consulted, never <c>GetUtcNow</c>, matching the rest of this
    /// library's rule that wall-clock time never governs a duration.
    /// </param>
    /// <param name="random">
    /// The source the two epoch secrets are filled from. This type does not own it: it is never
    /// disposed here.
    /// </param>
    public CookieMinter(TimeProvider timeProvider, RandomNumberGenerator random)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(random);

        _timeProvider = timeProvider;
        _random = random;
        _startTimestamp = timeProvider.GetTimestamp();
        _secrets =
        [
            new byte[DatagramSessionProtocol.EpochSecretLength],
            new byte[DatagramSessionProtocol.EpochSecretLength],
        ];
        _random.GetBytes(_secrets[0]);
        _random.GetBytes(_secrets[1]);
    }

    /// <summary>
    /// Mints a cookie for <paramref name="source"/> under the current epoch, writing the
    /// <see cref="DatagramSessionProtocol.CookieLength"/>-byte tag into <paramref name="cookie"/> and
    /// reporting the epoch it was minted under. Throws <see cref="ArgumentException"/> if
    /// <paramref name="cookie"/> is not exactly that length — only our own code supplies this buffer,
    /// so a mismatch is our own bug, not a hostile input.
    /// </summary>
    public void Mint(Source source, ulong clientNonce, Span<byte> cookie, out long epoch)
    {
        if (cookie.Length != DatagramSessionProtocol.CookieLength)
        {
            throw new ArgumentException(
                $"A cookie buffer must be exactly {DatagramSessionProtocol.CookieLength} bytes.",
                nameof(cookie));
        }

        AdvanceToCurrentEpoch();
        epoch = CurrentEpoch;

        Span<byte> input = stackalloc byte[MacInputLength];
        WriteMacInput(input, source, clientNonce, epoch);
        HMACSHA256.HashData(_secrets[epoch & 1], input, cookie);
    }

    /// <summary>
    /// Verifies that <paramref name="cookie"/> is a genuine tag this instance minted for
    /// <paramref name="source"/>, <paramref name="clientNonce"/> and <paramref name="epoch"/> under an
    /// epoch it still accepts. Every input here is a value a hostile stranger chose, so this never
    /// throws — a malformed or forged cookie simply fails to verify.
    /// </summary>
    public bool Verify(Source source, ulong clientNonce, long epoch, ReadOnlySpan<byte> cookie)
    {
        if (cookie.Length != DatagramSessionProtocol.CookieLength || epoch < 0)
        {
            return false;
        }

        AdvanceToCurrentEpoch();
        if (epoch != CurrentEpoch && epoch != CurrentEpoch - 1)
        {
            return false;
        }

        Span<byte> input = stackalloc byte[MacInputLength];
        WriteMacInput(input, source, clientNonce, epoch);

        Span<byte> expected = stackalloc byte[DatagramSessionProtocol.CookieLength];
        HMACSHA256.HashData(_secrets[epoch & 1], input, expected);

        return CryptographicOperations.FixedTimeEquals(expected, cookie);
    }

    /// <summary>
    /// Packs the HMAC input in the fixed wire order this type's remarks and doc comments both
    /// describe: family, address, scope id, port, client nonce, epoch — all big-endian, no padding.
    /// </summary>
    private static void WriteMacInput(Span<byte> destination, Source source, ulong clientNonce, long epoch)
    {
        int offset = 0;
        destination[offset] = (byte)source.Family;
        offset += sizeof(byte);
        BinaryPrimitives.WriteUInt128BigEndian(destination[offset..], source.AddressBytes);
        offset += 16;
        BinaryPrimitives.WriteInt64BigEndian(destination[offset..], source.ScopeId);
        offset += sizeof(long);
        BinaryPrimitives.WriteInt32BigEndian(destination[offset..], source.Port);
        offset += sizeof(int);
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], clientNonce);
        offset += sizeof(ulong);
        BinaryPrimitives.WriteInt64BigEndian(destination[offset..], epoch);
    }

    /// <summary>
    /// Steps <see cref="CurrentEpoch"/> forward to whatever epoch the elapsed time since construction
    /// now implies, regenerating only the secret(s) that fell out of acceptance. Every public member
    /// calls this first, so no caller can observe a stale epoch. A gap of two epochs or more —
    /// possible after a clock jump — regenerates both secrets in one step rather than looping, keeping
    /// this O(1) regardless of how large the gap is.
    /// </summary>
    private void AdvanceToCurrentEpoch()
    {
        TimeSpan elapsed = _timeProvider.GetElapsedTime(_startTimestamp);
        long impliedEpoch = (long)(elapsed.TotalSeconds / DatagramSessionProtocol.CookieEpochSeconds);
        if (impliedEpoch <= CurrentEpoch)
        {
            return;
        }

        long gap = impliedEpoch - CurrentEpoch;
        if (gap >= 2)
        {
            _random.GetBytes(_secrets[0]);
            _random.GetBytes(_secrets[1]);
        }
        else
        {
            _random.GetBytes(_secrets[impliedEpoch & 1]);
        }

        CurrentEpoch = impliedEpoch;
    }
}
