using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// A matched Resume whose source IP differs from the Peer's current address does not move
/// where the Peer is sent until that address proves it receives. The accepting end answers with one
/// PathChallenge to the Resume's source and moves only when the challenge's nonce comes back in a
/// PathResponse from that same source. Without this, anyone who completed a handshake could name a
/// third party as its new address and have the Reactor's sends reflected there. A Resume from the same
/// IP on a new port still moves at once (<see cref="DatagramSessionMatchedResumeTests"/>).
/// </summary>
/// <remarks>
/// 127.0.0.2 is another loopback address, so a resumer bound there differs from the Peer by address,
/// not only by port.
/// </remarks>
public class DatagramSessionResumePathChallengeTests
{
    private static readonly IPAddress _otherAddress = IPAddress.Parse("127.0.0.2");

    [Fact]
    public void Pump_MatchingResumeFromANewAddress_AnswersWithOnePathChallengeNamingTheDiallersSession()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);

        // Act
        byte[] challenge = SendResumeAndReadChallenge(scenario, resumer);

        // Assert
        Scenario.IsControlOfType(challenge, Scenario.PathChallengeType).Should().BeTrue();
        Scenario.PathTargetSessionId(challenge).Should().Be(scenario.DiallerSessionId);
        Scenario.PathNonce(challenge).Should().Contain(b => b != 0, "the nonce is random, not left as padding");
    }

    [Fact]
    public void Pump_MatchingResumeFromANewAddress_DoesNotResumeThePeerBeforeTheChallengeIsAnswered()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);

        // Act
        SendResumeAndReadChallenge(scenario, resumer);
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(0);
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
    }

    [Fact]
    public void Pump_MatchingResumeFromANewAddress_SendsNoDataDatagramThereBeforeTheChallengeIsAnswered()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        SendResumeAndReadChallenge(scenario, resumer);

        // Act — two send-idle intervals, so the Peer Pings at least once.
        for (int i = 0; i < 2; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
            scenario.PumpAccepter();
        }

        // Assert
        Scenario.WaitForDatagram(resumer, TimeSpan.FromMilliseconds(100))
            .Should().BeFalse("an unproven address receives the challenge and nothing else");
    }

    [Fact]
    public void Pump_PathResponseEchoingTheChallengeFromTheResumesSource_ResumesThePeerOnce()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        byte[] challenge = SendResumeAndReadChallenge(scenario, resumer);

        // Act
        resumer.Send(Scenario.BuildPathResponse(scenario.AccepterSessionId, Scenario.PathNonce(challenge)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed)
            .Which.Peer.Should().Be(scenario.AccepterPeer);
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(1);
    }

    [Fact]
    public void Pump_PathResponseEchoingTheChallengeFromTheResumesSource_SendsTheNextDatagramThere()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        byte[] challenge = SendResumeAndReadChallenge(scenario, resumer);
        resumer.Send(Scenario.BuildPathResponse(scenario.AccepterSessionId, Scenario.PathNonce(challenge)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));

        // Act — one send-idle interval later the Peer Pings, and a Ping goes to the Peer's send address.
        bool reachedTheResumer = false;
        for (int i = 0; i < 2 && !reachedTheResumer; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
            scenario.PumpAccepter();
            reachedTheResumer = Scenario.WaitForDatagram(resumer);
        }

        // Assert
        reachedTheResumer.Should().BeTrue("a proven address is where the Peer is sent from then on");
    }

    [Fact]
    public void Pump_PathResponseWithAWrongNonce_DoesNotResumeThePeer()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        byte[] nonce = Scenario.PathNonce(SendResumeAndReadChallenge(scenario, resumer));
        nonce[^1] ^= 0xFF;
        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;

        // Act
        resumer.Send(Scenario.BuildPathResponse(scenario.AccepterSessionId, nonce), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntilReceivedMoreThan(receivedBefore);
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(0);
    }

    [Fact]
    public void Pump_PathResponseEchoingTheChallengeFromAnotherPort_DoesNotResumeThePeer()
    {
        // Arrange — the echo must come from the address the challenge went to, or it proves nothing about it.
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        using UdpClient elsewhere = Scenario.NewResumer(_otherAddress);
        byte[] challenge = SendResumeAndReadChallenge(scenario, resumer);
        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;

        // Act
        elsewhere.Send(Scenario.BuildPathResponse(scenario.AccepterSessionId, Scenario.PathNonce(challenge)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntilReceivedMoreThan(receivedBefore);
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(0);
    }

    [Fact]
    public void Pump_SecondResumeFromTheSameNewSource_RepeatsTheSameNonce()
    {
        // Arrange — the dialler resends Resume every send-idle interval. A fresh nonce each time would
        // void an answer still in flight whenever the round trip exceeds that interval.
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        byte[] first = SendResumeAndReadChallenge(scenario, resumer);

        // Act
        byte[] second = SendResumeAndReadChallenge(scenario, resumer);

        // Assert
        Scenario.PathNonce(second).Should().Equal(Scenario.PathNonce(first));
    }

    [Fact]
    public void Pump_MatchingResumeFromTheSameAddressOnANewPort_SendsNoChallengeAndResumesAtOnce()
    {
        // Arrange — a NAT rebinding that changes only the port must not cost a round trip.
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));

        // Assert
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed);
        Scenario.WaitForDatagram(resumer, TimeSpan.FromMilliseconds(100)).Should().BeFalse("nothing is sent before the next Ping is due");
    }

    [Fact]
    public void Pump_ResumeFromAnotherNewSource_ChallengesItWithAFreshNonce()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient first = Scenario.NewResumer(_otherAddress);
        using UdpClient second = Scenario.NewResumer(_otherAddress);
        byte[] firstChallenge = SendResumeAndReadChallenge(scenario, first);

        // Act
        byte[] secondChallenge = SendResumeAndReadChallenge(scenario, second);

        // Assert
        Scenario.PathNonce(secondChallenge).Should().NotEqual(Scenario.PathNonce(firstChallenge));
    }

    [Fact]
    public void Pump_PathResponseArrivingAfterASameAddressResume_DoesNotMoveThePeerAgain()
    {
        // Arrange — a challenge is pending when the Peer resumes from its own IP, which settles where it is.
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient elsewhere = Scenario.NewResumer(_otherAddress);
        using UdpClient sameAddress = Scenario.NewResumer();
        byte[] challenge = SendResumeAndReadChallenge(scenario, elsewhere);
        sameAddress.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;

        // Act — the late echo, then a send-idle interval so the Peer Pings wherever it is sent.
        elsewhere.Send(Scenario.BuildPathResponse(scenario.AccepterSessionId, Scenario.PathNonce(challenge)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntilReceivedMoreThan(receivedBefore);
        bool reachedSameAddress = false;
        for (int i = 0; i < 2 && !reachedSameAddress; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
            scenario.PumpAccepter();
            reachedSameAddress = Scenario.WaitForDatagram(sameAddress);
        }

        // Assert
        reachedSameAddress.Should().BeTrue("the Peer stays where the same-address Resume moved it");
        Scenario.WaitForDatagram(elsewhere, TimeSpan.FromMilliseconds(100)).Should().BeFalse();
    }

    [Fact]
    public void Pump_ResumeFromANewAddressWithAWrongSecret_SendsNoChallenge()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer(_otherAddress);
        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.WrongSecret()), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntilReceivedMoreThan(receivedBefore);
        scenario.PumpAccepter();

        // Assert
        Scenario.WaitForDatagram(resumer, TimeSpan.FromMilliseconds(100)).Should().BeFalse();
    }

    [Fact]
    public void Pump_PathChallengeNamingTheDiallersSession_IsEchoedToTheAccepterWithTheSameNonce()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(bindDialler: true);
        using UdpClient challenger = Scenario.NewResumer();
        byte[] nonce = [.. Enumerable.Range(1, Scenario.PathNonceLength).Select(i => (byte)i)];
        int relayedBefore = Scenario.CapturedCount(scenario.Proxy, RelayDirection.ClientToUpstream);
        ulong sentBefore = scenario.Dialler.Reactor.Counters.DatagramsSent;

        // Act
        challenger.Send(Scenario.BuildPathChallenge(scenario.DiallerSessionId, nonce), scenario.Dialler.Endpoint);
        scenario.PumpDiallerUntil(() => scenario.Dialler.Reactor.Counters.DatagramsSent > sentBefore);
        scenario.SettleProxy();

        // Assert
        byte[] response = scenario.Captured(RelayDirection.ClientToUpstream).Skip(relayedBefore)
            .Select(c => c.Payload)
            .Should().ContainSingle(p => Scenario.IsControlOfType(p, Scenario.PathResponseType)).Which;
        Scenario.PathTargetSessionId(response).Should().Be(scenario.AccepterSessionId);
        Scenario.PathNonce(response).Should().Equal(nonce);
    }

    [Fact]
    public void Pump_PathChallengeNamingNoSession_IsDiscardedAndAnsweredWithNothing()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(bindDialler: true);
        using UdpClient challenger = Scenario.NewResumer();
        byte[] nonce = new byte[Scenario.PathNonceLength];
        ulong sentBefore = scenario.Dialler.Reactor.Counters.DatagramsSent;

        // Act
        challenger.Send(Scenario.BuildPathChallenge(scenario.DiallerSessionId ^ 0x5A5A_5A5A, nonce), scenario.Dialler.Endpoint);
        scenario.PumpDiallerUntil(() => scenario.Dialler.Sink.Received.Any(r => r.Id == DiagnosticId.DatagramMalformed));

        // Assert
        scenario.Dialler.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed);
        scenario.Dialler.Reactor.Counters.DatagramsSent.Should().Be(sentBefore);
    }

    /// <summary>
    /// Sends a matching Resume from <paramref name="resumer"/>, Pumps the accepter until it has read it, and
    /// returns the one datagram the accepter answered with.
    /// </summary>
    private static byte[] SendResumeAndReadChallenge(Scenario scenario, UdpClient resumer)
    {
        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntilReceivedMoreThan(receivedBefore);
        Scenario.WaitForDatagram(resumer).Should().BeTrue("a Resume from a new address is answered with a PathChallenge");
        IPEndPoint? from = null;
        return resumer.Receive(ref from);
    }
}
