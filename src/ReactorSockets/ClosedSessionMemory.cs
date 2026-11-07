using ReactorSockets.Session;

namespace ReactorSockets;

/// <summary>
/// Remembers datagram Sessions that closed on a Goodbye for one effective Linger each, so a resend of the
/// remote's Goodbye can still be acknowledged after the Peer's capacity Slot is gone (ADR 0017, second
/// amendment). A fixed ring of one entry per capacity Slot, indexed by the closed Session's local id.
/// When the ring is full the oldest entry is overwritten, so a burst of closes larger than the capacity
/// within one Linger can lose the oldest re-ack, which degrades that sender to
/// <see cref="DisconnectReason.LingerExpired"/>.
/// </summary>
/// <remarks>
/// Public for the same reason <see cref="FragmentGroup"/> is (ADR 0015): a test pins the ring's
/// overwrite, expiry and Goodbye-acknowledgement rules directly with a fake <see cref="TimeProvider"/>
/// and hand-built datagrams. Nothing here is reached from a consumer's request path.
/// <see cref="DatagramSessionReactor"/> owns one instance and does the sending: this type decides only
/// whether to answer and with which ack blocks. Not thread-safe.
/// </remarks>
public sealed class ClosedSessionMemory
{
    private readonly Entry[] _ring;
    private readonly Dictionary<uint, int> _indexBySession;
    private readonly TimeProvider _timeProvider;
    private int _next;

    /// <summary>Creates an empty memory of <paramref name="capacity"/> entries. Nothing is allocated after this.</summary>
    /// <param name="capacity">The number of closed Sessions held at once; the owning Reactor's Capacity.</param>
    /// <param name="timeProvider">The clock each entry's Linger is measured on.</param>
    public ClosedSessionMemory(int capacity, TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _ring = new Entry[capacity];
        _indexBySession = new Dictionary<uint, int>(capacity);
        _timeProvider = timeProvider;
    }

    /// <summary>
    /// Remembers the Session with <paramref name="localSessionId"/> for <paramref name="linger"/> from now,
    /// answering later resends with <paramref name="finalAck"/>. Overwrites the oldest entry when the ring
    /// is full.
    /// </summary>
    /// <param name="localSessionId">The id this end assigned, which the remote stamps on its datagrams. Never zero.</param>
    /// <param name="finalAck">The remote endpoint, its session id, and the Peer's final ack blocks.</param>
    /// <param name="linger">How long the entry answers for: the Peer's effective Linger.</param>
    public void Remember(uint localSessionId, ClosedSessionAck finalAck, TimeSpan linger)
    {
        ArgumentOutOfRangeException.ThrowIfZero(localSessionId);

        int ring = _next;
        _next = (ring + 1) % _ring.Length;

        uint overwritten = _ring[ring].LocalSessionId;
        if (overwritten != 0 && _indexBySession.TryGetValue(overwritten, out int overwrittenRing) && overwrittenRing == ring)
        {
            _indexBySession.Remove(overwritten);
        }

        _ring[ring] = new Entry(localSessionId, finalAck, _timeProvider.GetTimestamp(), linger);
        _indexBySession[localSessionId] = ring;
    }

    /// <summary>
    /// Reports whether an entry for <paramref name="localSessionId"/> is held, expired or not. A held id
    /// is not reissued, so a late datagram for the closed Session cannot reach a new one.
    /// </summary>
    public bool Holds(uint localSessionId) => _indexBySession.ContainsKey(localSessionId);

    /// <summary>
    /// Decides whether a data datagram naming <paramref name="localSessionId"/> is answered, and with
    /// what. Declines unless the id has an entry inside its Linger, <paramref name="source"/> is the stored
    /// remote endpoint, the datagram is well-formed, and it carries at least one <c>ReliableOrdered</c>
    /// record. The last condition stops two closed ends answering each other's header-only replies. An
    /// entry found past its Linger is forgotten. The stored blocks acknowledge exactly one thing beyond
    /// what the Peer had received: the remote's Goodbye, when it is a <c>ReliableOrdered</c> control record
    /// at the next ordered sequence. A Message is never acknowledged, since nothing is delivered after the
    /// close.
    /// </summary>
    /// <param name="localSessionId">The session id the datagram's header names.</param>
    /// <param name="source">Where the datagram came from.</param>
    /// <param name="datagram">The whole data datagram, header included.</param>
    /// <param name="ack">The reply's contents when this returns <see langword="true"/>.</param>
    public bool TryReAck(uint localSessionId, Source source, ReadOnlySpan<byte> datagram, out ClosedSessionAck ack)
    {
        ack = default;
        if (!_indexBySession.TryGetValue(localSessionId, out int ring))
        {
            return false;
        }

        ref Entry entry = ref _ring[ring];
        if (_timeProvider.GetElapsedTime(entry.ClosedTimestamp) >= entry.Linger)
        {
            _indexBySession.Remove(localSessionId);
            entry = default;
            return false;
        }

        if (entry.Ack.Remote != source || !DatagramHeaderCodec.TryReadHeader(datagram, out _, out _, out _, out _, out _))
        {
            return false;
        }

        // The Goodbye is applied to a copy, committed only once the whole body has parsed and proved to
        // carry an ordered record, so a datagram that is declined leaves the entry untouched.
        ClosedSessionAck candidate = entry.Ack;
        bool carriesReliableOrderedRecord = false;
        ReadOnlySpan<byte> remainder = datagram[DatagramSessionProtocol.DatagramHeaderSize..];
        while (remainder.Length > 0)
        {
            if (!PacketRecordCodec.TryRead(
                    remainder, out PacketRecordMode mode, out uint sequence, out _, out _, out _, out _,
                    out bool isControl, out int bytesConsumed))
            {
                return false;
            }

            if (mode == PacketRecordMode.ReliableOrdered)
            {
                carriesReliableOrderedRecord = true;
                if (isControl)
                {
                    candidate = AcknowledgeGoodbye(candidate, sequence);
                }
            }

            remainder = remainder[bytesConsumed..];
        }

        if (!carriesReliableOrderedRecord)
        {
            return false;
        }

        entry.Ack = candidate;
        ack = candidate;
        return true;
    }

    /// <summary>Forgets every entry.</summary>
    public void ForgetAll()
    {
        Array.Clear(_ring);
        _indexBySession.Clear();
        _next = 0;
    }

    /// <summary>
    /// Acknowledges the remote's Goodbye at <paramref name="sequence"/>. It counts only when it is exactly
    /// the next <c>ReliableOrdered</c> sequence, meaning every ordered record before it was delivered and
    /// the ack truthfully says the Goodbye reached its turn. A Goodbye behind a hole, or already below the
    /// base, changes nothing. Uses the base-plus-32-bits shape of <see cref="ReceiveWindow.DescribeAsAckBlock"/>.
    /// </summary>
    private static ClosedSessionAck AcknowledgeGoodbye(ClosedSessionAck ack, uint sequence)
    {
        if (sequence != ack.ReliableOrderedBase)
        {
            return ack;
        }

        uint ackBase = ack.ReliableOrderedBase;
        uint bits = ack.ReliableOrderedBits;
        bool nextAlreadyReceived;
        do
        {
            ackBase = unchecked(ackBase + 1);
            nextAlreadyReceived = (bits & 1) != 0;
            bits >>= 1;
        }
        while (nextAlreadyReceived);

        return ack with { ReliableOrderedBase = ackBase, ReliableOrderedBits = bits };
    }

    /// <summary>One ring entry. A zero <see cref="LocalSessionId"/> marks it unused.</summary>
    private struct Entry(uint localSessionId, ClosedSessionAck ack, long closedTimestamp, TimeSpan linger)
    {
        public readonly uint LocalSessionId = localSessionId;

        public ClosedSessionAck Ack = ack;

        public readonly long ClosedTimestamp = closedTimestamp;

        public readonly TimeSpan Linger = linger;
    }
}

/// <summary>
/// The contents of a header-only ack datagram for a closed Session: where it goes, the session id it
/// carries, and the two reliable Spaces' ack blocks.
/// </summary>
/// <param name="Remote">The only endpoint a reply is sent to.</param>
/// <param name="RemoteSessionId">The session id to stamp on the reply.</param>
/// <param name="ReliableOrderedBase">The <c>ReliableOrdered</c> ack block's base.</param>
/// <param name="ReliableOrderedBits">The <c>ReliableOrdered</c> ack block's bitfield.</param>
/// <param name="ReliableUnorderedBase">The <c>ReliableUnordered</c> ack block's base.</param>
/// <param name="ReliableUnorderedBits">The <c>ReliableUnordered</c> ack block's bitfield.</param>
public readonly record struct ClosedSessionAck(
    Source Remote,
    uint RemoteSessionId,
    uint ReliableOrderedBase,
    uint ReliableOrderedBits,
    uint ReliableUnorderedBase,
    uint ReliableUnorderedBits);
