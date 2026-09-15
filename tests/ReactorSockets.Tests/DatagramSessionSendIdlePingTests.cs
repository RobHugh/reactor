using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 8.1.4 (issue #340, parent #53): the send-idle timer, and the Ping it emits. An Established
/// Peer that has sent no data datagram for <see cref="DatagramSessionOptions.SendIdleInterval"/> queues
/// one Ping — a seven-byte <c>ReliableUnordered</c> control record — which takes a sequence and a
/// retention slot exactly like an ordinary reliable Send, marked in retention as control so a first send
/// and any later resend both pack it through the control-record shape rather than as an empty Message.
/// Every test here drives a real <see cref="DatagramSessionReactor"/> against another one through a
/// <see cref="WireTap"/>, so every assertion reads the exact bytes the tap captured off the wire — never
/// the far end's own behaviour. That matters here specifically: until issue #339 lands, the far end's
/// own <see cref="DatagramSessionReactor"/> discards a datagram carrying a control record as malformed,
/// so any test that waited on the accepter to observe or acknowledge a Ping would never pass. Driven
/// entirely by <see cref="DatagramSessionReactorHarness.TimeProvider"/> — no test here sleeps.
/// </summary>
public class DatagramSessionSendIdlePingTests
{
    private const int DatagramHeaderSize = 20;
    private const int ControlRecordSize = 7;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ControlBit = 0b0000_1000;

    // =========================================================================================
    // AC1 — nothing reaches the wire before SendIdleInterval elapses
    // =========================================================================================

    [Fact]
    public void Pump_QuietEstablishedSession_PutsNoControlRecordOnTheWireBeforeSendIdleIntervalElapses()
    {
        // Arrange — Linger is pushed far out so nothing here is ever near a retransmit deadline; this
        // test is about the send-idle timer alone. The harness's own default SendIdleInterval is an hour
        // (issue #340: chosen so tests that never mention the timer never see a Ping), so this test names
        // its own short one.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromSeconds(1));
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        int capturesBeforeIdleWait = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — one Pump short of the interval.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        tap.Pump();

        // Assert — nothing new left the dialler.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesBeforeIdleWait, "the send-idle interval has not fully elapsed yet");
    }

    // =========================================================================================
    // AC2 — once the interval elapses, exactly one datagram carries exactly one seven-byte
    // control record: the control bit set alongside ReliableUnordered's mode bits, length zero
    // =========================================================================================

    [Fact]
    public void Pump_SendIdleIntervalElapses_EmitsOneDatagramCarryingOneSevenByteControlRecord()
    {
        // Arrange — Linger is pushed far out so the Ping's own retransmit deadline (armed like any
        // other reliable record) never collides with the send-idle timer under test; the accepter never
        // acknowledges anything here, since #339 has not landed and a control record is still discarded
        // as malformed on receipt.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromSeconds(1));
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        int capturesBeforeIdleWait = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — the interval fully elapses.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval);
        dialler.Reactor.Pump();
        tap.Pump();

        // Assert — exactly one further datagram left the dialler, carrying exactly one record.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesBeforeIdleWait + 1, "exactly one Ping datagram must leave once the interval elapses");

        byte[] datagram = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();

        datagram.Length.Should().Be(
            DatagramHeaderSize + ControlRecordSize, "the datagram is the 20-byte header plus exactly one seven-byte control record");

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        decoded.IsSuccess.Should().BeTrue();
        decoded.Datagram!.Records.Should().ContainSingle();

        DecodedPacketRecord record = decoded.Datagram!.Records[0];
        record.Length.Should().Be(0, "a control record never carries a payload");
        record.Payload.Should().BeEmpty();
        (record.Flags & ModeReliableUnordered).Should().Be(
            ModeReliableUnordered, "a control record is always ReliableUnordered");
        (record.Flags & ControlBit).Should().Be(ControlBit, "the control bit marks this record a Ping, not an ordinary empty Message");
    }

    // =========================================================================================
    // AC3 — one Ping per interval, not one per Pump; and a further interval emits exactly one more
    // =========================================================================================

    [Fact]
    public void Pump_ManyPumpsWithinOneInterval_EmitsAtMostOnePingAndExactlyOneMorePerFurtherInterval()
    {
        // Arrange — Linger is pushed far out for the same reason as the previous test: two full
        // intervals pass here, and without a generous Linger the first Ping's own retransmit deadline
        // would otherwise fall due in the same Pump as the second interval's new Ping and add an
        // unrelated extra record to the datagram this test counts.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromSeconds(1));
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — cross the first interval, producing the first Ping.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval);
        dialler.Reactor.Pump();
        tap.Pump();

        int capturesAfterFirstPing = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — many further Pumps inside the same next interval: none of them may add a second Ping,
        // since the first Ping itself just restarted the interval (issue #340: the Ping counts as a
        // sent datagram too).
        for (int i = 0; i < 20; i++)
        {
            dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
            dialler.Reactor.Pump();
            tap.Pump();
        }

        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesAfterFirstPing, "no further Ping may appear before a full interval has passed since the first one");

        // Act — close out that interval fully.
        TimeSpan remaining = dialler.Options.SendIdleInterval - TimeSpan.FromMilliseconds(10 * 20);
        dialler.TimeProvider.Advance(remaining);
        dialler.Reactor.Pump();
        tap.Pump();

        // Assert — exactly one more Ping, for the second interval.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesAfterFirstPing + 1, "exactly one further Ping is emitted once the second interval elapses");
    }

    // =========================================================================================
    // AC4 — a Peer whose ReliableUnordered retention is full emits no Ping that Pump, and emits
    // it on the first Pump after a slot frees
    // =========================================================================================

    /// <remarks>
    /// This codebase's send-side <c>RetentionPool.Release</c> has no production caller yet anywhere in
    /// <see cref="DatagramSessionReactor"/> (a pre-existing gap this ticket does not touch — acknowledging
    /// a reliable record today updates <c>OutstandingReliableSequences</c>' own bookkeeping only, not
    /// retention), so a live Peer's <c>ReliableUnordered</c> retention cannot currently be freed mid-Session
    /// by a real acknowledgement through the public surface. The one retention-free path this codebase's
    /// send side does wire up — <c>ResetShareHolder</c>, called on admission — is what
    /// <see cref="DatagramSessionRetentionSlotRecycleTests"/> already exercises for "the Slot's retention
    /// is reset on admission": a capacity Slot's retention outlives whichever Peer occupies it, and a new
    /// Peer admitted into a Slot a previous Peer filled finds it empty. This test uses that same, real
    /// mechanism to free the Slot: closing the first Peer and admitting a second one into the Slot it
    /// vacated (capacity is pinned to one, forcing the reuse), then proving the second Peer's own first
    /// eligible Pump emits the Ping the first Peer's exhausted retention could never have let out.
    /// </remarks>
    [Fact]
    public void Pump_ReliableUnorderedRetentionFull_EmitsNoPingThatPump_AndEmitsItOnceTheSlotIsFreedByReuse()
    {
        // Arrange — one ReliableUnordered retention slot, on a Reactor pinned to capacity one, so a
        // single unacknowledged reliable Send exhausts the Space the Ping also draws from and a second
        // Peer can only be admitted into the same Slot once the first vacates it. fragmentSize is 600 —
        // RFC 791's 576-byte guaranteed reassembly minimum is DatagramSessionOptions' own floor — rather
        // than anything smaller.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness dialler = new(
            capacity: 1, bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize,
            maxMessageSize: fragmentSize, linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromSeconds(1));
        using DatagramSessionReactorHarness firstRemote = new();
        using DatagramSessionReactorHarness secondRemote = new();
        using WireTap firstTap = new(firstRemote.Endpoint, dialler.TimeProvider);

        PeerId first = dialler.Reactor.Connect(firstTap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, firstTap, firstRemote);

        // Fill the one ReliableUnordered retention slot and flush it to the wire.
        dialler.Reactor.Send(first, DeliveryMode.ReliableUnordered, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(first, DeliveryMode.ReliableUnordered, [4, 5, 6]).Should().Be(
            SendOutcome.RetentionExhausted, "the one slot is genuinely filled by the first Send");
        dialler.Reactor.Pump();
        firstTap.Pump();

        int capturesBeforeIdleWait = firstTap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — the interval elapses while the slot is still held.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval);
        dialler.Reactor.Pump();
        firstTap.Pump();

        // Assert — nothing new left the dialler: retention had no free slot for a Ping to take.
        firstTap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesBeforeIdleWait, "ReliableUnordered retention is full, so no Ping can be queued this Pump");

        // Act — the first Peer closes, and a second is admitted into the Slot it vacated: this Reactor's
        // capacity of one forces the reuse, and admission resets that Slot's retention (the mechanism
        // DatagramSessionRetentionSlotRecycleTests proves directly), so the second Peer's own
        // ReliableUnordered Space starts empty rather than inheriting the first Peer's exhausted one.
        dialler.Reactor.Close(first);
        dialler.Reactor.Pump();
        firstTap.Pump();
        dialler.DrainToList();

        using WireTap secondTap = new(secondRemote.Endpoint, dialler.TimeProvider);
        dialler.Reactor.Connect(secondTap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, secondTap, secondRemote);

        int capturesOnSecondTapBeforeIdleWait = secondTap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — the second Peer's own send-idle interval, measured from its own Establish, elapses.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval);
        dialler.Reactor.Pump();
        secondTap.Pump();

        // Assert — exactly one Ping leaves for the second Peer, on this Pump: the freed Slot's retention
        // no longer refuses it.
        List<byte[]> afterReuse = [.. secondTap.PayloadsInDirection(TapDirection.DiallerToAccepter)];
        afterReuse.Should().HaveCount(
            capturesOnSecondTapBeforeIdleWait + 1, "the reused Slot's retention is free, so the Ping goes out on this Pump");

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(afterReuse[^1]);
        decoded.IsSuccess.Should().BeTrue();
        decoded.Datagram!.Records.Should().ContainSingle();
        (decoded.Datagram!.Records[0].Flags & ControlBit).Should().Be(ControlBit, "the record freed onto the wire is the Ping");
    }

    // =========================================================================================
    // AC5 — DatagramSessionOptions has no member that disables the heartbeat
    // =========================================================================================

    [Fact]
    public void DatagramSessionOptions_HasNoOptionToDisableTheHeartbeat()
    {
        // Arrange
        System.Reflection.PropertyInfo[] properties =
            typeof(DatagramSessionOptions).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

        // Assert — no property anywhere on the surface names disabling, enabling or toggling either the
        // heartbeat, the Ping, or the send-idle timer that produces it.
        properties.Should().NotContain(
            p => (p.Name.Contains("Heartbeat", StringComparison.OrdinalIgnoreCase)
                    || p.Name.Contains("Ping", StringComparison.OrdinalIgnoreCase))
                && (p.Name.Contains("Disable", StringComparison.OrdinalIgnoreCase)
                    || p.Name.Contains("Enable", StringComparison.OrdinalIgnoreCase)),
            "the Ping is never optional and cannot be turned off");

        properties.Should().NotContain(
            p => p.Name.Contains("SendIdle", StringComparison.OrdinalIgnoreCase)
                && (p.Name.Contains("Disable", StringComparison.OrdinalIgnoreCase)
                    || p.Name.Contains("Enable", StringComparison.OrdinalIgnoreCase)),
            "the send-idle timer that drives the Ping cannot be turned off");

        // Assert — SendIdleInterval itself remains the one required, non-nullable knob: a nullable
        // TimeSpan would itself be a way to smuggle in a disable switch.
        System.Reflection.PropertyInfo sendIdleInterval = properties.Should()
            .ContainSingle(p => p.Name == nameof(DatagramSessionOptions.SendIdleInterval)).Which;
        sendIdleInterval.PropertyType.Should().Be(typeof(TimeSpan),
            "a nullable SendIdleInterval would itself be a way to disable the Ping");
    }
}
