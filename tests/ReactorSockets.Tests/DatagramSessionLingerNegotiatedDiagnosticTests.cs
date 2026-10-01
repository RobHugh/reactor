using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #364: both datagram establishing paths — the dialler's <c>OnAccept</c> and the accepter's
/// path reached from <c>OnHelloWithCookie</c> via <c>NewAcceptedPeer</c> — report
/// <see cref="DiagnosticId.LingerNegotiated"/> once establishment completes, mirroring
/// <c>StreamSessionReactor</c>. This is also the first test to observe
/// <c>DatagramSessionReactor.PeerSlot.EffectiveLinger</c> (issue #363) through the public seam: the
/// Diagnostic's <c>Value1</c> is exactly that field, so a wrong stored value (the local Linger, or the
/// maximum rather than the minimum) fails here even though issue #363 shipped without its own test.
/// </summary>
public class DatagramSessionLingerNegotiatedDiagnosticTests
{
    private static readonly TimeSpan _shorterLinger = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan _longerLinger = TimeSpan.FromSeconds(8);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Connect_TwoReactorsWithDifferentLingers_BothEndsRecordTheirOwnLingerAndTheMinimumAsEffective(
        bool shorterLingerEndDials)
    {
        // Arrange — a 5 s end and an 8 s end, with which one dials varied by the Theory so both
        // dialling directions are exercised cheaply in the one file.
        using DatagramSessionReactorHarness shorterLingerHarness = new(linger: _shorterLinger, bindLoopback: !shorterLingerEndDials);
        using DatagramSessionReactorHarness longerLingerHarness = new(linger: _longerLinger, bindLoopback: shorterLingerEndDials);

        DatagramSessionReactorHarness dialler = shorterLingerEndDials ? shorterLingerHarness : longerLingerHarness;
        DatagramSessionReactorHarness accepter = shorterLingerEndDials ? longerLingerHarness : shorterLingerHarness;
        TimeSpan diallerLinger = shorterLingerEndDials ? _shorterLinger : _longerLinger;
        TimeSpan accepterLinger = shorterLingerEndDials ? _longerLinger : _shorterLinger;

        dialler.Reactor.Connect(accepter.Endpoint);

        // Act
        (var diallerEvents, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        PeerId diallerPeer = diallerEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Assert — each end records exactly one LingerNegotiated for the Peer it sees, carrying its
        // own Linger in Value0 and the 5 s minimum in Value1, regardless of which end dialled.
        List<DiagnosticRecord> diallerLingerRecords =
            dialler.Sink.Received.Where(r => r.Id == DiagnosticId.LingerNegotiated).ToList();
        List<DiagnosticRecord> accepterLingerRecords =
            accepter.Sink.Received.Where(r => r.Id == DiagnosticId.LingerNegotiated).ToList();

        diallerLingerRecords.Should().ContainSingle()
            .Which.Should().Match<DiagnosticRecord>(r =>
                r.Peer == diallerPeer
                && r.Value0 == (long)diallerLinger.TotalMilliseconds
                && r.Value1 == 5000);

        accepterLingerRecords.Should().ContainSingle()
            .Which.Should().Match<DiagnosticRecord>(r =>
                r.Peer == accepterPeer
                && r.Value0 == (long)accepterLinger.TotalMilliseconds
                && r.Value1 == 5000);
    }

    [Fact]
    public void Connect_MinimumLevelAboveInfo_RecordsNoLingerNegotiated()
    {
        // Arrange — both ends configured with a Sink whose minimum level is above Info, so
        // IsEnabled(DiagnosticLevel.Info) reads false on both establishing paths.
        using DatagramSessionReactorHarness dialler = new(
            linger: _shorterLinger, bindLoopback: false, minimumLevel: DiagnosticLevel.Warn);
        using DatagramSessionReactorHarness accepter = new(
            linger: _longerLinger, minimumLevel: DiagnosticLevel.Warn);

        dialler.Reactor.Connect(accepter.Endpoint);

        // Act
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert — establishment still completed, but neither Sink recorded the Diagnostic.
        dialler.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.LingerNegotiated);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.LingerNegotiated);
    }
}
