using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #289: a Peer that has Resumed still validates a reliable <see cref="DatagramSessionReactor.Send"/>
/// against the peer's own announced <c>MaxMessageSize</c> — captured on the Peer's <c>Slot</c> at
/// handshake (issue #284) — rather than losing it and falling back to zero or to this Reactor's own
/// setting. The Datagram profile has no wire-level Resume of its own yet (that is Slice 8, issue #52,
/// still open); what already exists, and is exercised here, is the property that makes such a Resume
/// unnecessary for this purpose in the first place: a Session is identified by its wire token rather
/// than by the address or link a datagram arrived on
/// (<see cref="DatagramSessionReactor.ProcessDataDatagram"/>'s remarks). An "interruption" here is
/// therefore a real transport-level outage — a sustained stretch with nothing crossing the wire in
/// either direction — rather than a protocol event, and "resume" is nothing more than the outage
/// ending: no handshake, no counter exchange, nothing this library has to do to make the Slot's
/// captured <c>RemoteMaxMessageSize</c> still be there afterwards.
/// </summary>
/// <remarks>
/// Every test drives two real <see cref="DatagramSessionReactor"/>s through a <see cref="WireTap"/>
/// chained ahead of a <see cref="LossyProxy"/> (dialler -&gt; tap -&gt; proxy -&gt; accepter), the same
/// composition <see cref="DatagramSessionFragmentSurvivesInterruptionTests"/> uses. No fault is ever
/// queued on the proxy here, unlike that file: this profile raises no heartbeat or other idle traffic
/// of its own (<c>MissedHeartbeats</c> is a Stream-profile-only setting — nothing in
/// <see cref="DatagramSessionReactor"/> reads it), so once the handshake is done, nothing crosses the
/// wire at all until this file's own Send does. For this profile a "sustained run of dropped
/// datagrams" and a "sustained silence" are the same outage: there is nothing to drop. What is proved
/// below is what survives that silence — the tap and proxy stay in the composition purely to match the
/// established topology, not because either does anything during the gap.
/// </remarks>
/// <remarks>
/// The dialler and the accepter are given deliberately different <c>MaxMessageSize</c> values. Every
/// payload here is sized against the accepter's — the smaller of the two — so a Send that validated
/// against this Reactor's own setting (the dialler's, far larger) rather than the peer's announced one
/// would accept both the at-limit and the one-over-limit payload; only validating against the peer's
/// own Slot value refuses the second. And a Send that validated against a Slot value lost to the
/// interruption — zero, if the Resume path failed to keep it — would refuse both payloads, including
/// the at-limit one that a working implementation must accept. That is why the accepted case, not the
/// refused one, is what actually establishes the property: a test asserting only the refusal would pass
/// just as well against the very bug this ticket exists to catch.
/// </remarks>
public class DatagramSessionResumedRemoteMessageSizeTests
{
    /// <summary>The RFC 791 reassembly-buffer floor <see cref="DatagramSessionOptions"/> enforces as the minimum fragment size.</summary>
    private const int FragmentSize = 576;

    /// <summary>The accepter's announced limit: an exact multiple of <see cref="FragmentSize"/>, so the at-limit Message below cuts into exactly three whole Fragments with no partial remainder.</summary>
    private const int AccepterMaxMessageSize = FragmentSize * 3;

    /// <summary>
    /// The dialler's own announced limit — far larger than <see cref="AccepterMaxMessageSize"/> and
    /// never itself the thing under test. Its only job is to be different enough from the accepter's
    /// that a Send validated against the wrong end's limit would visibly accept payloads this file
    /// expects refused.
    /// </summary>
    private const int DiallerMaxMessageSize = FragmentSize * 20;

    /// <summary>Enough retention on the dialler to hold every Fragment one at-limit Send cuts into, plus headroom.</summary>
    private const int DiallerRetentionBytes = FragmentSize * 4;

    /// <summary>Comfortably longer than any timer this profile owns, so the gap is a genuine sustained outage rather than a merely slow round trip.</summary>
    private static readonly TimeSpan _interruptionDuration = TimeSpan.FromMinutes(5);

    [Fact]
    public void Send_AfterAResumedPeersInterruption_AMessageAtThePeersAnnouncedLimitIsAcceptedAndArrivesWhole()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        fixture.LetTheConnectionSitInterruptedThenResume();

        byte[] message = new byte[AccepterMaxMessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        // Act — the send-side proof: a Slot that had lost the announced value (fallen back to zero)
        // would refuse this too, so the outcome alone is not yet decisive.
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, DeliveryMode.ReliableOrdered, message);

        // Assert — the outcome, then the delivery: only a Message that actually arrives whole at the
        // far end proves the Slot still held the real limit, not zero, through the interruption.
        outcome.Should().Be(SendOutcome.Sent, "the resumed Peer's Slot still holds the peer's real announced limit, not zero");

        List<byte[]> delivered = fixture.PumpRoundsCollectingPayloads(rounds: 12);
        delivered.Should().ContainSingle(
                "a Slot that had lost the announced value would have refused the Send outright, so nothing would ever reach the wire")
            .Which.Should().Equal(message, "the whole Message must arrive intact, not merely be accepted at the call");
    }

    [Fact]
    public void Send_AfterAResumedPeersInterruption_AMessageOneByteOverThePeersAnnouncedLimitIsRefused()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        fixture.LetTheConnectionSitInterruptedThenResume();
        byte[] tooLarge = new byte[AccepterMaxMessageSize + 1];

        // Act — well under the dialler's own DiallerMaxMessageSize, so only validation against the
        // peer's own announced limit, still held after the interruption, can refuse this.
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, DeliveryMode.ReliableOrdered, tooLarge);

        // Assert
        outcome.Should().Be(
            SendOutcome.RemoteMessageTooLarge,
            "the resumed Peer still judges a reliable Send against the peer's announced limit, one byte under this payload, not this Reactor's own far larger setting");
    }

    /// <summary>
    /// The fixture both tests share: a dialler and an accepter, with deliberately different
    /// <see cref="MaxMessageSize"/>s, connected through a <see cref="WireTap"/> chained ahead of a
    /// <see cref="LossyProxy"/>, plus the interruption-then-resume choreography.
    /// </summary>
    private sealed class Fixture : IDisposable
    {
        public required DatagramSessionReactorHarness Dialler { get; init; }
        public required DatagramSessionReactorHarness Accepter { get; init; }
        public required WireTap Tap { get; init; }
        public required LossyProxy Proxy { get; init; }
        public required PeerId DiallerPeer { get; init; }
        public required PeerId AccepterPeer { get; init; }

        public static Fixture Build()
        {
            DatagramSessionReactorHarness dialler = new(
                capacity: 1, bindLoopback: false, maxMessageSize: DiallerMaxMessageSize,
                retentionBytes: DiallerRetentionBytes, fragmentSize: FragmentSize);
            DatagramSessionReactorHarness accepter = new(
                capacity: 1, maxMessageSize: AccepterMaxMessageSize, fragmentSize: FragmentSize,
                arenaSize: AccepterMaxMessageSize);
            LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
            WireTap tap = new(proxy.PublicEndPoint, dialler.TimeProvider);

            PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
            PeerId accepterPeer = ConnectThroughTapAndProxy(dialler, tap, proxy, accepter);

            return new Fixture
            {
                Dialler = dialler,
                Accepter = accepter,
                Tap = tap,
                Proxy = proxy,
                DiallerPeer = diallerPeer,
                AccepterPeer = accepterPeer,
            };
        }

        /// <summary>
        /// The interruption: nothing crosses the wire for <see cref="_interruptionDuration"/> — this
        /// profile raises no idle traffic of its own, so simulating the outage is nothing more than
        /// advancing the clock while pumping every party with nothing queued to send (matching how
        /// <see cref="DatagramSessionFragmentSurvivesInterruptionTests"/>'s own dual-Space test
        /// simulates silence). The resume that follows is equally uneventful: no handshake, no counter
        /// exchange, just the clock moving on and traffic being allowed to flow again.
        /// </summary>
        public void LetTheConnectionSitInterruptedThenResume()
        {
            Dialler.TimeProvider.Advance(_interruptionDuration);
            for (int i = 0; i < 5; i++)
            {
                PumpRelayRound();
            }
        }

        public List<byte[]> PumpRoundsCollectingPayloads(int rounds)
        {
            List<byte[]> delivered = [];
            for (int i = 0; i < rounds; i++)
            {
                PumpRelayRound();
                delivered.AddRange(Accepter.DrainPayloads());
            }

            return delivered;
        }

        public void PumpRelayRound()
        {
            Dialler.Reactor.Pump();
            Tap.Pump();
            Proxy.Pump();
            Accepter.Reactor.Pump();
            Proxy.Pump();
            Tap.Pump();
        }

        private static PeerId ConnectThroughTapAndProxy(
            DatagramSessionReactorHarness dialler, WireTap tap, LossyProxy proxy, DatagramSessionReactorHarness accepter)
        {
            PumpDeadline deadline = PumpDeadline.Start();
            PeerId? accepterPeer = null;
            bool diallerConnected = false;

            while (true)
            {
                dialler.Reactor.Pump();
                tap.Pump();
                proxy.Pump();
                accepter.Reactor.Pump();
                proxy.Pump();
                tap.Pump();

                foreach ((SessionEventKind kind, PeerId eventPeer, DisconnectReason _) in accepter.DrainToList())
                {
                    if (kind == SessionEventKind.PeerConnected)
                    {
                        accepterPeer = eventPeer;
                    }
                }

                diallerConnected |= dialler.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);

                if (diallerConnected && accepterPeer is PeerId established)
                {
                    return established;
                }

                if (deadline.Expired)
                {
                    throw new InvalidOperationException("The tapped-and-proxied handshake did not complete within the Pump budget.");
                }

                PumpDeadline.Yield();
            }
        }

        public void Dispose()
        {
            Dialler.Dispose();
            Accepter.Dispose();
            Tap.Dispose();
            Proxy.Dispose();
        }
    }
}
