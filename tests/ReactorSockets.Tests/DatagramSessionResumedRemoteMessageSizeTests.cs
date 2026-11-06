using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Peer that has Resumed still validates a reliable <see cref="DatagramSessionReactor.Send"/>
/// against the peer's own announced <c>MaxMessageSize</c> — captured on the Peer's <c>Slot</c> at
/// handshake (issue #284) — rather than losing it and falling back to zero or to this Reactor's own
/// setting. The Datagram profile has a wire-level Resume that carries no
/// parameters and only moves the send address; this class exercises the property that keeps the
/// captured limit intact across it: a Session is identified by its wire token rather than by the
/// address or link a datagram arrived on
/// (<see cref="DatagramSessionReactor.ProcessDataDatagram"/>'s remarks). The "interruption" here is a
/// real transport-level outage — a sustained run of dropped Datagrams in both directions, through the
/// same <see cref="LossyProxy"/> <see cref="DatagramSessionFragmentSurvivesInterruptionTests"/> uses —
/// with no address change, and it ends with the outage: no handshake, no counter exchange, nothing
/// this library has to do to make the Slot's captured <c>RemoteMaxMessageSize</c> still be there
/// afterwards.
/// </summary>
/// <remarks>
/// Every test drives two real <see cref="DatagramSessionReactor"/>s through a <see cref="WireTap"/>
/// chained ahead of a <see cref="LossyProxy"/> (dialler -&gt; tap -&gt; proxy -&gt; accepter), the same
/// composition <see cref="DatagramSessionFragmentSurvivesInterruptionTests"/> uses. Before the
/// limit-boundary Send under test, the fixture drives one ordinary Message to completion — proving the
/// connection is not merely handshaken but has actually carried application data — then puts a second
/// Message in flight and interrupts it for real. That Message cuts into two Fragments, but at this
/// <see cref="FragmentSize"/> both always ride the same Datagram: <c>DatagramSessionReactor.FlushPeer</c>
/// packs a whole Peer's send queue into one Datagram per Pump, so there is no smaller unit than "the
/// Datagram" for the proxy to drop. The outage drops that Datagram's first transmission attempt and its
/// first retransmission outbound, and a further accepter Send inbound, so real Datagrams are lost in
/// both directions, not merely withheld, across more than one retransmit deadline. Recovery is asserted,
/// not assumed — the interrupted Message must arrive whole once the drops stop, or the fixture itself
/// fails before either limit-boundary test gets to its own Act.
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
/// just as well against the very bug this file exists to catch.
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

    /// <summary>Well under one Fragment, sent and delivered before the outage as the connection's genuine pre-interruption traffic.</summary>
    private const int WarmupMessageSize = 3;

    /// <summary>
    /// One full Fragment plus a short remainder: the interrupted Message cuts into two Fragments. At
    /// this <see cref="FragmentSize"/>, <see cref="DatagramSessionReactor"/>'s per-Pump flush packs a
    /// whole Peer's queue into one Datagram (<c>FlushPeer</c>'s own remarks), so both Fragments always
    /// ride the same Datagram — the outage below drops that Datagram and its retransmissions, not one
    /// Fragment carved out of the other.
    /// </summary>
    private const int InterruptionLastFragmentRemainder = 100;

    /// <summary>The interrupted Message's size: one full Fragment plus <see cref="InterruptionLastFragmentRemainder"/>.</summary>
    private const int InterruptionMessageSize = FragmentSize + InterruptionLastFragmentRemainder;

    /// <summary>
    /// Enough retention on the dialler to permanently hold the warm-up Message's one Fragment, the
    /// interrupted Message's two Fragments, and the at-limit boundary Message's three Fragments —
    /// retention slots are never released for a Datagram Peer's send-side pool once occupied — plus two
    /// spare slots.
    /// </summary>
    private const int DiallerRetentionBytes = FragmentSize * 8;

    /// <summary>Comfortably past the RTO, whose ceiling is the harness's one-second Linger.</summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Send_AfterAResumedPeersInterruption_AMessageAtThePeersAnnouncedLimitIsAcceptedAndArrivesWhole()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        fixture.DriveGenuineTrafficThenSurviveARealOutage();

        byte[] message = new byte[AccepterMaxMessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        // Act
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, DeliveryMode.ReliableOrdered, message);
        List<byte[]> delivered = fixture.PumpRoundsCollectingPayloads(rounds: 12);

        // Assert — the outcome, then the delivery: a Slot that had lost the announced value (fallen
        // back to zero) would refuse this Send outright, so only a Message that actually arrives whole
        // at the far end proves the Slot still held the real limit through the outage.
        outcome.Should().Be(SendOutcome.Sent, "the resumed Peer's Slot still holds the peer's real announced limit, not zero");
        delivered.Should().ContainSingle(
                "a Slot that had lost the announced value would have refused the Send outright, so nothing would ever reach the wire")
            .Which.Should().Equal(message, "the whole Message must arrive intact, not merely be accepted at the call");
    }

    [Fact]
    public void Send_AfterAResumedPeersInterruption_AMessageOneByteOverThePeersAnnouncedLimitIsRefused()
    {
        // Arrange
        using Fixture fixture = Fixture.Build();
        fixture.DriveGenuineTrafficThenSurviveARealOutage();
        byte[] tooLarge = new byte[AccepterMaxMessageSize + 1];

        // Act — well under the dialler's own DiallerMaxMessageSize, so only validation against the
        // peer's own announced limit, still held after the outage, can refuse this.
        SendOutcome outcome = fixture.Dialler.Reactor.Send(fixture.DiallerPeer, DeliveryMode.ReliableOrdered, tooLarge);

        // Assert
        outcome.Should().Be(
            SendOutcome.RemoteMessageTooLarge,
            "the resumed Peer still judges a reliable Send against the peer's announced limit, one byte under this payload, not this Reactor's own far larger setting");
    }

    /// <summary>
    /// The fixture both tests share: a dialler and an accepter, with deliberately different
    /// <see cref="MaxMessageSize"/>s, connected through a <see cref="WireTap"/> chained ahead of a
    /// <see cref="LossyProxy"/>, plus the genuine-traffic-then-outage choreography.
    /// </summary>
    private sealed class Fixture : IDisposable
    {
        public required DatagramSessionReactorHarness Dialler { get; init; }
        public required DatagramSessionReactorHarness Accepter { get; init; }
        public required WireTap Tap { get; init; }
        public required LossyProxy Proxy { get; init; }
        public required PeerId DiallerPeer { get; init; }
        public required PeerId AccepterPeer { get; init; }

        /// <summary>Connects a dialler to an accepter through a tap then a proxy, and returns the fixture once both ends have connected.</summary>
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
        /// Drives one ordinary Message to completion first, then puts a second Message in flight and
        /// interrupts it for real: the still-missing Fragment's retransmissions are dropped outbound
        /// and a further accepter Send is dropped inbound, so the outage runs in both directions, not
        /// merely as silence. Ends by letting the drops stop and asserting the interrupted Message
        /// arrives whole — the fixture fails here, not in a test's own Act, if resuming does not
        /// actually work.
        /// </summary>
        public void DriveGenuineTrafficThenSurviveARealOutage()
        {
            byte[] warmup = new byte[WarmupMessageSize];
            for (int i = 0; i < warmup.Length; i++)
            {
                warmup[i] = (byte)(i + 1);
            }

            Dialler.Reactor.Send(DiallerPeer, DeliveryMode.ReliableOrdered, warmup).Should().Be(SendOutcome.Sent);
            PumpRoundsCollectingPayloads(rounds: 6).Should().ContainSingle(
                    "the connection must actually carry a Message before the outage, not merely complete a handshake")
                .Which.Should().Equal(warmup);

            byte[] interrupted = new byte[InterruptionMessageSize];
            for (int i = 0; i < interrupted.Length; i++)
            {
                interrupted[i] = (byte)(i + 1);
            }

            // The outage starts before this Message ever reaches the wire: its own first transmission
            // attempt, and its first retransmission, are both dropped outbound. A further accepter Send
            // is dropped inbound in the same window, so real Datagrams are lost in both directions, not
            // merely withheld.
            Dialler.Reactor.Send(DiallerPeer, DeliveryMode.ReliableOrdered, interrupted).Should().Be(SendOutcome.Sent);
            Proxy.DropNext(RelayDirection.ClientToUpstream);
            Proxy.DropNext(RelayDirection.ClientToUpstream);
            Proxy.DropNext(RelayDirection.UpstreamToClient);
            Accepter.Reactor.Send(AccepterPeer, DeliveryMode.Unreliable, [1]).Should().Be(SendOutcome.Sent);

            PumpRelayRound();
            Accepter.DrainPayloads().Should().BeEmpty("the interrupted Message's first transmission attempt was dropped, not delivered");
            Dialler.DrainPayloads().Should().BeEmpty("the accepter's own Send in this window was dropped too; the loss runs in both directions");

            Dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);
            PumpRelayRound();
            Accepter.DrainPayloads().Should().BeEmpty("the outage is still ongoing; the first retransmission was dropped as well");

            // The outage ends: nothing further is dropped. Recovery is asserted, not assumed — a Resume
            // that had failed to keep the Fragment reassembly state, or a Send path that never retried,
            // would leave this empty and fail the fixture here, before either limit-boundary test gets
            // to its own Act.
            Dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);
            PumpRoundsCollectingPayloads(rounds: 12).Should().ContainSingle(
                    "the outage has ended and the interrupted Message's next retransmission finally lands")
                .Which.Should().Equal(interrupted);
        }

        /// <summary>Runs <paramref name="rounds"/> relay rounds and returns every payload the accepter received, in order.</summary>
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

        /// <summary>Pumps the dialler, tap, proxy, accepter, proxy and tap once each, so a datagram can cross the path in either direction.</summary>
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
