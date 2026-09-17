using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #363: both establishing paths — the dialler's <c>OnAccept</c> and the accepter's path reached
/// from <c>OnHelloWithCookie</c> — store the peer's announced Linger and compute the effective
/// (minimum) Linger. Nothing reads <c>EffectiveLinger</c> yet (issue #364 adds the Diagnostic, issue
/// #365 the RTO ceiling, issue #367 expiry), so this file proves only the acceptance criterion that is
/// observable through the public L2 seam today: two Reactors configured with different Lingers still
/// establish, in both dialling directions. That "both paths set EffectiveLinger to the minimum" is met
/// by construction is not asserted here — see the sibling XML docs on
/// <c>DatagramSessionReactor.PeerSlot.EffectiveLinger</c>.
/// </summary>
public class DatagramSessionLingerNegotiationTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Connect_TwoReactorsWithDifferentLingers_BothEndsRaisePeerConnected(bool shorterLingerEndDials)
    {
        // Arrange — a 5 s end and an 8 s end, with which one dials varied by the Theory so the mismatch
        // is exercised from both directions (ADR 0012: a mismatch between independently deployed ends
        // is the normal case, not a misconfiguration).
        TimeSpan shorterLinger = TimeSpan.FromSeconds(5);
        TimeSpan longerLinger = TimeSpan.FromSeconds(8);

        using DatagramSessionReactorHarness shorterLingerHarness = new(linger: shorterLinger, bindLoopback: !shorterLingerEndDials);
        using DatagramSessionReactorHarness longerLingerHarness = new(linger: longerLinger, bindLoopback: shorterLingerEndDials);

        DatagramSessionReactorHarness dialler = shorterLingerEndDials ? shorterLingerHarness : longerLingerHarness;
        DatagramSessionReactorHarness accepter = shorterLingerEndDials ? longerLingerHarness : shorterLingerHarness;

        dialler.Reactor.Connect(accepter.Endpoint);

        // Act
        (var diallerEvents, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert — both ends establish despite the mismatched Lingers.
        diallerEvents.Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);
        accepterEvents.Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);
    }
}
