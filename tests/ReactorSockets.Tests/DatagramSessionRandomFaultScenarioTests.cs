using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Every promise in <c>docs/delivery-modes.md</c>, asserted over a seeded run of random faults: one
/// Session through a <see cref="LossyProxy"/>, all four modes sending at once, and every round queuing a
/// random drop, duplicate, reorder or delay in each direction. Where
/// <see cref="DatagramSessionReliableDeliveryPromiseTests"/> scripts a handful of faults by hand, this
/// suite draws hundreds from a seed, so a weekly run with fresh seeds explores paths no scripted test
/// names.
/// </summary>
/// <remarks>
/// <para>
/// The seed fixes every fault, Message size and clock step, so a failure is reproduced by setting
/// <see cref="SeedVariable"/> to the seed in the failing test's name. Without it the suite runs
/// <see cref="_fixedSeeds"/>, which is what every CI run does. The weekly job sets
/// <see cref="SeedVariable"/> to a fresh base seed and <see cref="CountVariable"/> to the number of
/// consecutive seeds to run from it.
/// </para>
/// <para>
/// Loopback delivery is not synchronous with the send, so the order the wire produces still varies
/// between runs of one seed. Every assertion here is a promise that holds on any order, so that
/// variation can only find a bug, never fail a correct Reactor.
/// </para>
/// <para>
/// One run per seed is shared by every test in this class, and each test asserts one invariant of it.
/// Running the scenario once per invariant would cost eight times the time for the same coverage.
/// </para>
/// </remarks>
public class DatagramSessionRandomFaultScenarioTests
{
    /// <summary>The environment variable naming the first seed to run in place of <see cref="_fixedSeeds"/>.</summary>
    public const string SeedVariable = "REACTOR_SCENARIO_SEED";

    /// <summary>The environment variable naming how many consecutive seeds to run from <see cref="SeedVariable"/>. One when unset.</summary>
    public const string CountVariable = "REACTOR_SCENARIO_COUNT";

    private static readonly int[] _fixedSeeds = [1729, 40961, 65537];

    /// <summary>Below the 64-Message window <c>Unreliable</c> suppresses duplicates within, so any duplicate is a broken promise.</summary>
    private const int MessagesPerMode = 48;

    /// <summary>Three Fragments at the default 1200-byte <c>FragmentSize</c>, inside <see cref="ReceiveWindow"/>.</summary>
    private const int MaxReliableMessageSize = 3000;

    /// <summary>Inside one Datagram at the default <c>FragmentSize</c>, the cap on the unreliable modes.</summary>
    private const int MaxUnreliableMessageSize = 1000;

    /// <summary>The mode byte and the two-byte index every Message starts with.</summary>
    private const int HeaderSize = 3;

    private const int ReceiveWindow = 16;
    private const int RetentionBytes = 64 * 1024;

    /// <summary>A Raw Link delivers one datagram per Pump, so the proxy Pumps several times per hop to keep up with a multi-Fragment flight.</summary>
    private const int ProxyPumpsPerHop = 4;

    /// <summary>
    /// The cap on rounds spent Sending. A Reactor that stops freeing retention refuses every reliable
    /// Send from then on, and without a cap that refusal would be retried forever.
    /// </summary>
    private const int MaxSendRounds = 1000;

    /// <summary>The cap on rounds after the last Send for the reliable modes to finish delivering.</summary>
    private const int MaxSettleRounds = 600;

    /// <summary>Rounds Pumped after the reliable modes finish, to catch a late duplicate.</summary>
    private const int QuietRounds = 30;

    private static readonly TimeSpan _faultRoundStep = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan _settleRoundStep = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan _maxFaultDelay = TimeSpan.FromMilliseconds(200);

    private static readonly DeliveryMode[] _modes =
        [DeliveryMode.Unreliable, DeliveryMode.UnreliableSequenced, DeliveryMode.ReliableUnordered, DeliveryMode.ReliableOrdered];

    private static readonly ConcurrentDictionary<int, ScenarioRun> _runs = new();

    /// <summary>The seeds every test runs: <see cref="_fixedSeeds"/>, or the range <see cref="SeedVariable"/> and <see cref="CountVariable"/> name.</summary>
    public static TheoryData<int> Seeds()
    {
        TheoryData<int> seeds = new();
        string? first = Environment.GetEnvironmentVariable(SeedVariable);
        if (string.IsNullOrWhiteSpace(first))
        {
            foreach (int seed in _fixedSeeds)
            {
                seeds.Add(seed);
            }

            return seeds;
        }

        int start = int.Parse(first, CultureInfo.InvariantCulture);
        int count = int.TryParse(Environment.GetEnvironmentVariable(CountVariable), CultureInfo.InvariantCulture, out int parsed) ? parsed : 1;
        for (int offset = 0; offset < count; offset++)
        {
            seeds.Add(unchecked(start + offset));
        }

        return seeds;
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_ReliableOrderedUnderRandomFaults_DeliversEveryMessageOnceInSendOrder(int seed)
    {
        // Arrange
        IEnumerable<int> sendOrder = Enumerable.Range(0, MessagesPerMode);

        // Act
        ScenarioRun run = ScenarioRun.For(seed);

        // Assert
        run.Delivered[DeliveryMode.ReliableOrdered].Should().Equal(sendOrder, run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_ReliableUnorderedUnderRandomFaults_DeliversEveryMessageExactlyOnce(int seed)
    {
        // Arrange
        IEnumerable<int> sent = Enumerable.Range(0, MessagesPerMode);

        // Act
        ScenarioRun run = ScenarioRun.For(seed);

        // Assert — the multiset, never the order: this mode promises nothing about order.
        run.Delivered[DeliveryMode.ReliableUnordered].Should().BeEquivalentTo(sent, run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_UnreliableSequencedUnderRandomFaults_NeverDeliversAnOlderMessageAfterANewerOne(int seed)
    {
        // Arrange
        DeliveryMode mode = DeliveryMode.UnreliableSequenced;

        // Act
        ScenarioRun run = ScenarioRun.For(seed);

        // Assert — strictly ascending, so a repeat of the last delivered is a failure too.
        run.Delivered[mode].Should().BeInAscendingOrder(run.Because).And.OnlyHaveUniqueItems(run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_UnreliableUnderRandomFaults_DeliversNoMessageTwice(int seed)
    {
        // Arrange — fewer Messages than the 64-Message suppression window, so the promise covers them all.
        DeliveryMode mode = DeliveryMode.Unreliable;

        // Act
        ScenarioRun run = ScenarioRun.For(seed);

        // Assert
        run.Delivered[mode].Should().OnlyHaveUniqueItems(run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_AllModesUnderRandomFaults_DeliverEveryPayloadByteForByte(int seed)
    {
        // Arrange — every Message's bytes are a function of its mode, index and length.
        ScenarioRun run;

        // Act
        run = ScenarioRun.For(seed);

        // Assert
        run.CorruptPayloads.Should().BeEmpty(run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_AllModesUnderRandomFaults_NeitherEndInterruptsOrClosesThePeer(int seed)
    {
        // Arrange — loss, duplication, reordering and delay are ordinary on a datagram path.
        ScenarioRun run;

        // Act
        run = ScenarioRun.For(seed);

        // Assert
        run.LifecycleEvents.Should().BeEmpty(run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Pump_AllModesUnderRandomFaults_NeitherEndLogsAnError(int seed)
    {
        // Arrange — Error means something is wrong with this Reactor, and an ordinary fault is not that.
        ScenarioRun run;

        // Act
        run = ScenarioRun.For(seed);

        // Assert
        run.ErrorRecords.Should().BeEmpty(run.Because);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void Send_AllModesUnderRandomFaults_AcceptsEveryMessage(int seed)
    {
        // Arrange — RetentionExhausted and SendQueueFull are backpressure and are retried next round,
        // so only another outcome, or a refusal that lasts until the round cap, fails this.
        ScenarioRun run;

        // Act
        run = ScenarioRun.For(seed);

        // Assert
        run.UnexpectedOutcomes.Should().BeEmpty(run.Because);
    }

    /// <summary>
    /// The bytes of Message <paramref name="index"/> in <paramref name="mode"/>: the mode, the index, and
    /// a fill derived from all three, so a receiver can check a payload without a copy of what was sent.
    /// </summary>
    private static byte[] MessageBytes(DeliveryMode mode, int index, int length)
    {
        byte[] message = new byte[length];
        message[0] = (byte)mode;
        BinaryPrimitives.WriteUInt16BigEndian(message.AsSpan(1, 2), (ushort)index);
        for (int position = HeaderSize; position < length; position++)
        {
            message[position] = unchecked((byte)((index * 131) + (position * 7) + (int)mode));
        }

        return message;
    }

    /// <summary>What one seeded scenario produced, read by every test for that seed.</summary>
    private sealed class ScenarioRun
    {
        private ScenarioRun(int seed) => Seed = seed;

        public int Seed { get; }

        /// <summary>The index of every Message delivered, per mode, in delivery order.</summary>
        public Dictionary<DeliveryMode, List<int>> Delivered { get; } = _modes.ToDictionary(mode => mode, _ => new List<int>());

        public List<string> CorruptPayloads { get; } = [];

        public List<string> LifecycleEvents { get; } = [];

        public List<string> ErrorRecords { get; } = [];

        public List<string> UnexpectedOutcomes { get; } = [];

        public string Because =>
            $"seed {Seed} drew this run's faults; rerun it with {SeedVariable}={Seed.ToString(CultureInfo.InvariantCulture)}";

        public static ScenarioRun For(int seed) => _runs.GetOrAdd(seed, Execute);

        private static ScenarioRun Execute(int seed)
        {
            ScenarioRun run = new(seed);
            Random random = new(seed);
            FakeTimeProvider clock = new();

            using DatagramSessionReactorHarness sender = new(
                bindLoopback: false, retentionBytes: RetentionBytes, receiveWindowFragments: ReceiveWindow, timeProvider: clock);
            using DatagramSessionReactorHarness receiver = new(
                retentionBytes: RetentionBytes, receiveWindowFragments: ReceiveWindow, timeProvider: clock);
            using LossyProxy proxy = new(receiver.Endpoint, clock);
            PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
            DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

            Dictionary<DeliveryMode, byte[][]> messages = _modes.ToDictionary(
                mode => mode,
                mode => Enumerable.Range(0, MessagesPerMode)
                    .Select(index => MessageBytes(mode, index, random.Next(HeaderSize, MaxSizeOf(mode) + 1)))
                    .ToArray());
            Dictionary<DeliveryMode, int> nextToSend = _modes.ToDictionary(mode => mode, _ => 0);

            for (int round = 0; nextToSend.Values.Any(next => next < MessagesPerMode); round++)
            {
                if (round == MaxSendRounds)
                {
                    run.RecordStalledSends(nextToSend);
                    break;
                }

                run.SendNextOfEachMode(sender, peer, messages, nextToSend);
                QueueRandomFault(proxy, random, RelayDirection.ClientToUpstream, faultPercent: 45);
                QueueRandomFault(proxy, random, RelayDirection.UpstreamToClient, faultPercent: 30);
                run.PumpRound(sender, proxy, receiver, messages);
                clock.Advance(_faultRoundStep + TimeSpan.FromMilliseconds(random.Next(0, 20)));
            }

            for (int round = 0; round < MaxSettleRounds && !run.ReliableModesComplete(); round++)
            {
                run.PumpRound(sender, proxy, receiver, messages);
                clock.Advance(_settleRoundStep);
            }

            for (int round = 0; round < QuietRounds; round++)
            {
                run.PumpRound(sender, proxy, receiver, messages);
                clock.Advance(_settleRoundStep);
            }

            run.RecordErrors("sender", sender);
            run.RecordErrors("receiver", receiver);
            return run;
        }

        /// <summary>
        /// Sends each mode's next Message once. A Message refused for backpressure stays next and is
        /// retried in the following round. Any other refusal is recorded and ends that mode's sending.
        /// </summary>
        private void SendNextOfEachMode(
            DatagramSessionReactorHarness sender,
            PeerId peer,
            Dictionary<DeliveryMode, byte[][]> messages,
            Dictionary<DeliveryMode, int> nextToSend)
        {
            foreach (DeliveryMode mode in _modes.Where(mode => nextToSend[mode] < MessagesPerMode))
            {
                int next = nextToSend[mode];
                SendOutcome outcome = sender.Reactor.Send(peer, mode, messages[mode][next]);
                if (outcome == SendOutcome.Sent)
                {
                    nextToSend[mode] = next + 1;
                }
                else if (outcome is not (SendOutcome.RetentionExhausted or SendOutcome.SendQueueFull))
                {
                    UnexpectedOutcomes.Add($"{mode} Message {next}: {outcome}");
                    nextToSend[mode] = MessagesPerMode;
                }
            }
        }

        private void RecordStalledSends(Dictionary<DeliveryMode, int> nextToSend) =>
            UnexpectedOutcomes.AddRange(nextToSend
                .Where(pair => pair.Value < MessagesPerMode)
                .Select(pair => $"{pair.Key} Message {pair.Value}: still refused after {MaxSendRounds} rounds"));

        private void RecordErrors(string side, DatagramSessionReactorHarness harness) =>
            ErrorRecords.AddRange(harness.Sink.Received
                .Where(record => record.Level == DiagnosticLevel.Error)
                .Select(record => $"{side}: {record.Id}"));

        private static int MaxSizeOf(DeliveryMode mode) =>
            mode is DeliveryMode.ReliableOrdered or DeliveryMode.ReliableUnordered ? MaxReliableMessageSize : MaxUnreliableMessageSize;

        /// <summary>Queues one random fault for the next relay in <paramref name="direction"/>, or none <c>100 - faultPercent</c> percent of the time.</summary>
        private static void QueueRandomFault(LossyProxy proxy, Random random, RelayDirection direction, int faultPercent)
        {
            if (random.Next(100) >= faultPercent)
            {
                return;
            }

            switch (random.Next(4))
            {
                case 0:
                    proxy.DropNext(direction);
                    break;
                case 1:
                    proxy.DuplicateNext(direction);
                    break;
                case 2:
                    proxy.ReorderNextTwo(direction);
                    break;
                default:
                    proxy.DelayNext(direction, TimeSpan.FromMilliseconds(random.Next(1, (int)_maxFaultDelay.TotalMilliseconds)));
                    break;
            }
        }

        private bool ReliableModesComplete() =>
            Delivered[DeliveryMode.ReliableOrdered].Count >= MessagesPerMode
            && Delivered[DeliveryMode.ReliableUnordered].Count >= MessagesPerMode;

        /// <summary>
        /// One round: the sender, the proxy toward the receiver, the receiver, and the proxy back. Each end
        /// drains straight after its own Pump, because a Pump resets the Frame and an undrained Message is gone.
        /// </summary>
        private void PumpRound(
            DatagramSessionReactorHarness sender,
            LossyProxy proxy,
            DatagramSessionReactorHarness receiver,
            Dictionary<DeliveryMode, byte[][]> messages)
        {
            sender.Reactor.Pump();
            Collect("sender", sender, messages);
            PumpProxy(proxy);
            receiver.Reactor.Pump();
            Collect("receiver", receiver, messages);
            PumpProxy(proxy);
            PumpDeadline.Yield();
        }

        private static void PumpProxy(LossyProxy proxy)
        {
            for (int pump = 0; pump < ProxyPumpsPerHop; pump++)
            {
                proxy.Pump();
            }
        }

        private void Collect(string side, DatagramSessionReactorHarness harness, Dictionary<DeliveryMode, byte[][]> messages)
        {
            foreach (SessionEvent evt in harness.Reactor.Drain())
            {
                switch (evt.Kind)
                {
                    case SessionEventKind.MessageReceived:
                        Record(evt.Payload, messages);
                        break;
                    case SessionEventKind.PeerInterrupted or SessionEventKind.PeerClosed:
                        LifecycleEvents.Add($"{side}: {evt.Kind} ({evt.Reason})");
                        break;
                }
            }
        }

        private void Record(ReadOnlySpan<byte> payload, Dictionary<DeliveryMode, byte[][]> messages)
        {
            if (payload.Length < HeaderSize || !Enum.IsDefined((DeliveryMode)payload[0]))
            {
                CorruptPayloads.Add($"{payload.Length}-byte payload with no valid header");
                return;
            }

            DeliveryMode mode = (DeliveryMode)payload[0];
            int index = BinaryPrimitives.ReadUInt16BigEndian(payload.Slice(1, 2));
            if (index >= MessagesPerMode || !payload.SequenceEqual(messages[mode][index]))
            {
                CorruptPayloads.Add($"{mode} Message {index}: {payload.Length} bytes that differ from what was sent");
                return;
            }

            Delivered[mode].Add(index);
        }
    }
}
