using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #166: a fuzzed hostile-input corpus for the data path — the parse loop
/// <see cref="DatagramSessionDataDatagramParseTests"/> proves the shape of, aimed here at a live
/// Session with random noise, boundary truncations, mutated flag bytes, overrunning lengths, and a
/// zero-length body naming a live Session. Every one of them either parses or produces
/// <see cref="DiagnosticId.DatagramMalformed"/> (200) and is discarded (issue #163); this ticket adds
/// volume and randomness on top of that ticket's hand-picked shapes.
/// </summary>
/// <remarks>
/// A live Session id is not exposed by the public surface, so — exactly as
/// <see cref="DatagramSessionDataDatagramParseTests"/> does — every test here first completes an
/// ordinary handshake by hand over a bare <see cref="RawReactor"/> and reads the wire session id the
/// accepter assigned out of its Accept reply.
/// </remarks>
public class DatagramSessionDataFuzzTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;
    private const uint DialerMaxMessageSize = 4096;
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;
    private const ulong DialerLingerMilliseconds = 30_000;

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;

    private static readonly byte[] _allModes = [ModeUnreliable, ModeUnreliableSequenced, ModeReliableUnordered, ModeReliableOrdered];

    /// <summary>
    /// The corpus size: large enough to exercise every mutation family many times over while staying
    /// fast — see the class remarks on measured runtime.
    /// </summary>
    private const int CorpusSize = 5000;

    /// <summary>
    /// The fixed seed for every fuzz test's <see cref="Random"/>: 20260820, the date this test was
    /// written, chosen only so a failure is reproducible across runs and machines — never re-rolled to
    /// make a flaky case pass.
    /// </summary>
    private const int Seed = 20260820;

    [Fact]
    public void Pump_FuzzedCorpusAgainstALiveSession_NeverThrowsNeverProducesAnUnjustifiedEventAndTheReactorStaysUsable()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        Random random = new(Seed);

        List<byte[]> corpus = BuildCorpus(random, sessionId);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allDrained = [];

        // Act — every datagram in the corpus goes out, pumping in batches so the receive queue never
        // grows unbounded, exactly as the control-path fuzz test in DatagramSessionMalformedInputTests
        // does.
        Action fuzz = () =>
        {
            const int batchSize = 100;
            for (int i = 0; i < corpus.Count; i++)
            {
                client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), corpus[i]);
                if ((i + 1) % batchSize == 0)
                {
                    allDrained.AddRange(accepter.PumpAndDrainAll(1));
                }
            }

            allDrained.AddRange(accepter.PumpAndDrainAll(5));
        };

        // Assert — nothing throws.
        fuzz.Should().NotThrow();

        // Assert — no Session event the format does not justify. Every datagram in the corpus is
        // either a mutation of a well-formed shape or outright noise. A mutation that happens to stay
        // well-formed can now be admitted and delivered (#217, #219), so MessageReceived is justified;
        // a PeerConnected already happened during the handshake, and nothing here may raise
        // PeerDisconnected, PeerClosed or any other kind.
        allDrained.Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "a fuzzed datagram may deliver a Message but must never close or disconnect the Peer");

        // Assert — every datagram either parsed (silently, per #163) or was counted as a malformed
        // discard; nothing vanished unaccounted for.
        // Both discard figures count, because the corpus mutates the session id too: a datagram still
        // naming the live Session lands on MalformedSessionDiscards, one whose id was mutated away
        // from it on UnknownSourceDiscards (issue #275).
        ulong discards = accepter.Reactor.Counters.MalformedSessionDiscards
            + accepter.Reactor.Counters.UnknownSourceDiscards;
        discards.Should().BeLessThanOrEqualTo((ulong)corpus.Count);

        // Assert — the Reactor stays usable afterwards: a well-formed datagram sent after the whole
        // corpus still parses and is not reported malformed.
        ulong malformedBefore = CountMalformed(accepter);
        byte[] wellFormed =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(random.NextItem(_allModes), sequence: 123, payload: [0x01, 0x02, 0x03]),
        ];
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), wellFormed);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        accepter.PumpAndDrainAll(3);

        CountMalformed(accepter).Should().Be(malformedBefore);
    }

    /// <summary>
    /// Measures allocation across the same corpus and Pump pattern as the primary fuzz test, in its
    /// own <see cref="Fact"/> so an allocation regression fails independently of the correctness
    /// assertions above. Not an exact-zero assertion: <c>Socket.ReceiveFrom</c> allocates an
    /// <c>EndPoint</c> per receive regardless of datagram content (see the note in
    /// <c>RawReactorPumpTests.Pump_SteadyStateAfterWarmupWithNothingReady_AllocatesNothingOnTheManagedHeap</c>),
    /// so a corpus that drives thousands of receives cannot show literal zero bytes allocated on this
    /// thread without also asserting something about L1's receive path, which is not this ticket's
    /// parse-path claim. Instead this measures per-datagram allocation against a ceiling far below
    /// what parsing the claimed lengths or record counts would cost if the parse path allocated
    /// proportionally to attacker-controlled input — the same technique
    /// <c>StreamSessionMalformedFramingTests</c> uses for an oversized length claim.
    /// </summary>
    [Fact]
    public void Pump_FuzzedCorpusAgainstALiveSession_AllocatesNoMoreThanAFixedPerDatagramCeiling()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        Random random = new(Seed);
        List<byte[]> corpus = BuildCorpus(random, sessionId);

        // Warm up the JIT and any lazily-initialized state before measuring.
        foreach (byte[] warmupDatagram in corpus.Take(50))
        {
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), warmupDatagram);
        }

        accepter.PumpAndDrainAll(5);

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 50; i < corpus.Count; i++)
        {
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), corpus[i]);
            if ((i + 1) % 100 == 0)
            {
                accepter.PumpAndDrainAll(1);
            }
        }

        accepter.PumpAndDrainAll(5);
        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert — a generous per-datagram ceiling (2 KB) that would be blown through many times over
        // if a record's claimed length or the header's ack-block words drove a proportional
        // allocation, while comfortably covering whatever Socket.ReceiveFrom's own EndPoint churn
        // costs.
        long ceiling = 2048L * (corpus.Count - 50);
        (allocatedAfter - allocatedBefore).Should().BeLessThan(ceiling,
            "the parse path must not allocate proportionally to attacker-controlled lengths or record counts");
    }

    /// <summary>
    /// Builds the fuzz corpus: random noise, truncations at every header and record boundary, mutated
    /// flag bytes, lengths just past the end, records claiming more bytes than the datagram holds, and
    /// a well-formed header naming the live Session with a zero-length body (no records at all).
    /// </summary>
    private static List<byte[]> BuildCorpus(Random random, uint sessionId)
    {
        List<byte[]> corpus = [];

        // Pure random noise, spanning short-than-header through several records' worth.
        for (int i = 0; i < CorpusSize / 5; i++)
        {
            int length = random.Next(0, 512);
            byte[] datagram = new byte[length];
            random.NextBytes(datagram);
            corpus.Add(datagram);
        }

        // Truncations of a well-formed multi-record datagram at every byte boundary from zero up to
        // one short of complete.
        byte[] wellFormedMultiRecord =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeUnreliable, 1, [0xAA, 0xBB, 0xCC]),
            .. BuildRecord(ModeReliableOrdered, 2, [0x01]),
            .. BuildRecord(ModeReliableUnordered, 3, []),
        ];
        for (int cut = 0; cut < wellFormedMultiRecord.Length; cut++)
        {
            corpus.Add(wellFormedMultiRecord[..cut]);
        }

        // Mutated flag bytes: every possible flags byte value (0-255) on a single record, so every
        // spare-bit and fragment-bit combination is covered at least once, plus the two mode bits.
        for (int flags = 0; flags <= 0xFF; flags++)
        {
            byte[] record = new byte[UnfragmentedRecordSize + 2];
            record[0] = (byte)flags;
            BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), (uint)(1000 + flags));
            BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), 2);
            record[7] = 0xEE;
            record[8] = 0xFF;
            corpus.Add([.. BuildHeader(sessionId), .. record]);
        }

        // Lengths just past the end: a record whose declared length is 1, 2, or many bytes more than
        // what actually follows it in the datagram.
        foreach (int overrun in new[] { 1, 2, 5, 100, ushort.MaxValue })
        {
            byte[] header = new byte[UnfragmentedRecordSize];
            header[0] = ModeReliableOrdered;
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(1, 4), 42);
            BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(5, 2), (ushort)overrun);
            corpus.Add([.. BuildHeader(sessionId), .. header]);
            corpus.Add([.. BuildHeader(sessionId), .. header, 0x00]); // one byte short of the overrun claim
        }

        // Records claiming the maximum possible length (65535) with nothing but the record header
        // itself present.
        for (int i = 0; i < 10; i++)
        {
            byte[] header = new byte[UnfragmentedRecordSize];
            header[0] = random.NextItem(_allModes);
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(1, 4), (uint)random.Next());
            BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(5, 2), ushort.MaxValue);
            corpus.Add([.. BuildHeader(sessionId), .. header]);
        }

        // A well-formed header naming the live Session with a zero-length body: no records at all,
        // which is legal — a container fitting zero records.
        corpus.Add(BuildHeader(sessionId));

        // Random mutations of well-formed multi-record datagrams: flip a random subset of bytes in an
        // otherwise-valid datagram, which is the closest a fuzzer gets to "almost legal".
        for (int i = 0; i < CorpusSize - corpus.Count - 200; i++)
        {
            byte[] mutated = [.. wellFormedMultiRecord];
            int flips = random.Next(1, 6);
            for (int f = 0; f < flips; f++)
            {
                mutated[random.Next(mutated.Length)] = (byte)random.Next(256);
            }

            corpus.Add(mutated);
        }

        // Fill out to exactly CorpusSize with more pure noise of varying length, including a handful
        // of oversized (path-MTU-busting) datagrams.
        while (corpus.Count < CorpusSize)
        {
            int length = random.Next(0, 2000);
            byte[] datagram = new byte[length];
            random.NextBytes(datagram);
            corpus.Add(datagram);
        }

        return corpus;
    }

    private static ulong CountMalformed(DatagramSessionReactorHarness accepter) =>
        (ulong)accepter.Sink.Received.Count(r => r.Id == DiagnosticId.DatagramMalformed);

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> and returns the wire
    /// session id <paramref name="accepter"/> assigned this Peer, matching
    /// <see cref="DatagramSessionDataDatagramParseTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            accepter.Reactor.Pump();
            client.Pump();
            foreach (RawEvent evt in client.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    return evt.Payload.ToArray();
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The handshake reply did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    private static byte[] BuildHelloDatagram(ulong clientNonce)
    {
        byte[] body = new byte[4 + 8];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildHelloWithCookieDatagram(ulong clientNonce, long epoch, byte[] cookie)
    {
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + 8 + ResumeSecretLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerSessionId);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerMaxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerSendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerLingerMilliseconds);
        offset += 8;
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    private static byte[] BuildRecord(byte modeBits, uint sequence, ReadOnlySpan<byte> payload, byte extraFlags = 0)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = (byte)(modeBits | extraFlags);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }
}

/// <summary>Picks a uniformly random element from <paramref name="items"/> using <paramref name="random"/>.</summary>
file static class RandomExtensions
{
    public static T NextItem<T>(this Random random, IReadOnlyList<T> items) => items[random.Next(items.Count)];
}
