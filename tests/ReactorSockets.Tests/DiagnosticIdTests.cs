using System.Reflection;
using System.Xml.Linq;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The roster audit (issue #59, ADR 0013, ADR 0020). These tests assert properties of the permanent
/// public <see cref="DiagnosticId"/> contract rather than behaviours of the network: every member's
/// value and level is pinned, sits inside its reserved block, is documented with both value slots, and
/// is produced by a named test elsewhere in the suite.
/// </summary>
public class DiagnosticIdTests
{
    /// <summary>
    /// ADR 0013's roster as shipped: the value and level of every member. A member added to the enum
    /// without a row here fails <see cref="Roster_PinnedTable_CoversEveryMemberAndNoOther"/>, so the
    /// table cannot fall behind the code.
    /// </summary>
    private static readonly Dictionary<DiagnosticId, RosterRow> _roster = new()
    {
        [DiagnosticId.AdmissionCapReached] = new(1, DiagnosticLevel.Warn),
        [DiagnosticId.ArenaExhausted] = new(2, DiagnosticLevel.Warn),
        [DiagnosticId.CapacityExhausted] = new(3, DiagnosticLevel.Warn),
        [DiagnosticId.LinkFailed] = new(100, DiagnosticLevel.Warn),
        [DiagnosticId.DestinationUnreachable] = new(101, DiagnosticLevel.Warn),
        [DiagnosticId.DatagramTruncated] = new(102, DiagnosticLevel.Warn),
        [DiagnosticId.DatagramMalformed] = new(200, DiagnosticLevel.Warn),
        [DiagnosticId.CookieRejected] = new(201, DiagnosticLevel.Warn),
        [DiagnosticId.VersionRefused] = new(202, DiagnosticLevel.Warn),
        [DiagnosticId.FragmentSetAbandoned] = new(203, DiagnosticLevel.Warn),
        [DiagnosticId.RetransmissionTimerBackedOff] = new(204, DiagnosticLevel.Trace),
        [DiagnosticId.OversizedFragmentSet] = new(205, DiagnosticLevel.Warn),
        [DiagnosticId.UnreliableFragment] = new(206, DiagnosticLevel.Warn),
        [DiagnosticId.OversizedMessage] = new(207, DiagnosticLevel.Warn),
        [DiagnosticId.AddressPeerLimitReached] = new(208, DiagnosticLevel.Warn),
        [DiagnosticId.PeerRefused] = new(300, DiagnosticLevel.Info),
        [DiagnosticId.LingerNegotiated] = new(301, DiagnosticLevel.Info),
        [DiagnosticId.MalformedFraming] = new(302, DiagnosticLevel.Warn),
        [DiagnosticId.DataAfterGoodbye] = new(303, DiagnosticLevel.Warn),
        [DiagnosticId.ResumeMatched] = new(304, DiagnosticLevel.Info),
        [DiagnosticId.ResumeUnmatched] = new(305, DiagnosticLevel.Warn),
        [DiagnosticId.RedialAttempted] = new(306, DiagnosticLevel.Info),
        [DiagnosticId.RedialFailed] = new(307, DiagnosticLevel.Info),
        [DiagnosticId.DeliveryCounterRefused] = new(308, DiagnosticLevel.Warn),
    };

    /// <summary>
    /// For every member, a test elsewhere in the suite that provokes it through the public surface and
    /// asserts the record arrived. Each row is a review-backed claim: <c>nameof</c> proves the test
    /// exists, and the named test's own assertion proves the member is produced. A member shared by two
    /// profiles (ADR 0020) names one producer per profile.
    /// </summary>
    private static readonly ProducerRow[] _producers =
    [
        new(DiagnosticId.AdmissionCapReached, typeof(RawReactorListenTests), nameof(RawReactorListenTests.Pump_MoreConnectionsPendingThanTheAdmissionCap_LogsAdmissionCapReachedExactlyOnce)),
        new(DiagnosticId.AdmissionCapReached, typeof(DatagramSessionRefusalTests), nameof(DatagramSessionRefusalTests.Pump_AFrameThatTurnsWorkAway_EmitsExactlyOneAdmissionCapReachedRecordCarryingTheCapAndCount)),
        new(DiagnosticId.ArenaExhausted, typeof(RawReactorFrameTests), nameof(RawReactorFrameTests.Pump_ArenaWithNoRoomForASecondLinksDatagram_ReportsArenaExhaustedWithTheArithmetic)),
        new(DiagnosticId.ArenaExhausted, typeof(DatagramSessionMessageDeliveryTests), nameof(DatagramSessionMessageDeliveryTests.Pump_ARecordTheArenaHasNoRoomFor_IsNotCountedAsReceivedAndReportsArenaExhausted)),
        new(DiagnosticId.CapacityExhausted, typeof(RawReactorListenTests), nameof(RawReactorListenTests.Pump_MorePendingThanFreeSlots_LogsCapacityExhaustedRatherThanAdmissionCapReached)),
        new(DiagnosticId.CapacityExhausted, typeof(DatagramSessionRefusalTests), nameof(DatagramSessionRefusalTests.OnHelloWithCookie_EveryCapacitySlotOccupied_RaisesAdmissionDiscardsAndLogsCapacityExhausted)),
        new(DiagnosticId.LinkFailed, typeof(RawReactorDialTests), nameof(RawReactorDialTests.Connect_ClosedPort_LogsLinkFailedCarryingThePlatformSocketError)),
        new(DiagnosticId.DestinationUnreachable, typeof(RawReactorTests), nameof(RawReactorTests.Pump_AfterSendToAClosedPort_RecordsDestinationUnreachable)),
        new(DiagnosticId.DatagramTruncated, typeof(RawReactorDiagnosticsTests), nameof(RawReactorDiagnosticsTests.Log_RecordReachingTheSink_CarriesItsIdLevelAndTaggedSubject)),
        new(DiagnosticId.DatagramMalformed, typeof(DatagramSessionCookieTests), nameof(DatagramSessionCookieTests.OnHelloWithCookie_AnnouncingAZeroSendIdleInterval_IsDiscardedAsMalformedAndNoPeerConnected)),
        new(DiagnosticId.CookieRejected, typeof(DatagramSessionCookieTests), nameof(DatagramSessionCookieTests.OnHelloWithCookie_ForgedCookie_IsRejectedWithWarnDiagnosticAndNoPeerConnected)),
        new(DiagnosticId.VersionRefused, typeof(DatagramSessionRefusalTests), nameof(DatagramSessionRefusalTests.OnHelloWithCookie_OfferingAVersionTheReactorDoesNotSpeak_LogsVersionRefusedAtWarnNotError)),
        new(DiagnosticId.FragmentSetAbandoned, typeof(DatagramSessionFragmentSetAbandonedTests), nameof(DatagramSessionFragmentSetAbandonedTests.Pump_RingLapClobbersAnIncompleteFragmentSetsSlot_EmitsFragmentSetAbandonedOnceWithHeldAndDeclaredCounts)),
        new(DiagnosticId.RetransmissionTimerBackedOff, typeof(DatagramSessionAdaptiveRtoTests), nameof(DatagramSessionAdaptiveRtoTests.Pump_ConsecutiveLossesWithNothingAcknowledged_EmitOneRecordEachWithARisingCountAndADoublingRtoThatSaturatesAtLinger)),
        new(DiagnosticId.OversizedFragmentSet, typeof(DatagramSessionOversizedFragmentSetTests), nameof(DatagramSessionOversizedFragmentSetTests.Pump_FragmentDeclaringOneMoreThanReceiveWindowFloor_ClosesThePeerWithProtocolViolation)),
        new(DiagnosticId.UnreliableFragment, typeof(DatagramSessionHostileFragmentDiscardTests), nameof(DatagramSessionHostileFragmentDiscardTests.Pump_HostileFragmentInAnUnreliableMode_DiscardsWholeDatagramAndReportsTheRefusal)),
        new(DiagnosticId.OversizedMessage, typeof(DatagramSessionInboundMessageLimitTests), nameof(DatagramSessionInboundMessageLimitTests.Pump_FragmentSetAssemblingPastTheLimit_DiscardsItAndReportsItsLengthAgainstTheLimit)),
        new(DiagnosticId.AddressPeerLimitReached, typeof(DatagramSessionRefusalTests), nameof(DatagramSessionRefusalTests.OnHelloWithCookie_AddressAtItsPeerLimit_CountsADiscardAndLogsAddressPeerLimitReached)),
        new(DiagnosticId.PeerRefused, typeof(StreamSessionHandshakeGuardTests), nameof(StreamSessionHandshakeGuardTests.Connect_OfferingAVersionTheReactorDoesNotSpeak_LogsPeerRefusedAtInfoWithTheSourceSubject)),
        new(DiagnosticId.LingerNegotiated, typeof(StreamSessionNegotiatedLingerTests), nameof(StreamSessionNegotiatedLingerTests.Pump_BothRolesInterrupted_WithFractionalMillisecondOwnLinger_RoundUpOnceAndCloseAtTheRoundedInstant)),
        new(DiagnosticId.LingerNegotiated, typeof(DatagramSessionLingerNegotiatedDiagnosticTests), nameof(DatagramSessionLingerNegotiatedDiagnosticTests.Connect_TwoReactorsWithDifferentLingers_BothEndsRecordTheirOwnLingerAndTheMinimumAsEffective)),
        new(DiagnosticId.MalformedFraming, typeof(StreamSessionMalformedFramingTests), nameof(StreamSessionMalformedFramingTests.DeliverCompleteMessages_PrefixAnnouncing4GB_LogsMalformedFramingAtWarnWithThePeerSubject)),
        new(DiagnosticId.DataAfterGoodbye, typeof(StreamSessionGoodbyeTests), nameof(StreamSessionGoodbyeTests.DataAfterGoodbye_MessageArrivesBehindGoodbyeInTheSameWrite_LogsDataAfterGoodbyeAtWarnWithThePeerSubject)),
        new(DiagnosticId.ResumeMatched, typeof(StreamSessionResumeDiagnosticsTests), nameof(StreamSessionResumeDiagnosticsTests.Handshake_AResumePresentingAHeldPeersSecret_LogsExactlyOneResumeMatchedNamingThatPeer)),
        new(DiagnosticId.ResumeMatched, typeof(DatagramSessionMatchedResumeTests), nameof(DatagramSessionMatchedResumeTests.Pump_MatchingResumeAgainstAPeerThatIsNotInterrupted_RaisesNoEventLogsResumeMatchedAndLeavesResumesUnchanged)),
        new(DiagnosticId.ResumeUnmatched, typeof(StreamSessionResumeDiagnosticsTests), nameof(StreamSessionResumeDiagnosticsTests.Handshake_AResumePresentingASecretMatchingNoHeldPeer_LogsResumeUnmatchedAgainstAClaimedSource)),
        new(DiagnosticId.ResumeUnmatched, typeof(DatagramSessionDiallerRejectsResumeTests), nameof(DatagramSessionDiallerRejectsResumeTests.Pump_ResumeNamingADialledPeerWithItsOwnSecret_LogsResumeUnmatchedAndRaisesNoEvent)),
        new(DiagnosticId.RedialAttempted, typeof(StreamSessionRedialTests), nameof(StreamSessionRedialTests.Pump_AReDialAttempt_LogsRedialAttemptedAtInfoNamingThePeerItBelongsTo)),
        new(DiagnosticId.RedialFailed, typeof(StreamSessionRedialTests), nameof(StreamSessionRedialTests.Pump_ReDialAttemptThatFails_LeavesThePeerInterruptedRatherThanClosingIt)),
        new(DiagnosticId.DeliveryCounterRefused, typeof(StreamSessionDeliveryCounterFramingTests), nameof(StreamSessionDeliveryCounterFramingTests.DeliverCompleteMessages_ACountAboveWhatThisEndEverSent_LogsDeliveryCounterRefusedRatherThanMalformedFraming)),
    ];

    /// <summary>Every member the enum declares, read from the enum rather than from <see cref="_roster"/>.</summary>
    public static TheoryData<DiagnosticId> Members => [.. Enum.GetValues<DiagnosticId>()];

    [Fact]
    public void Roster_PinnedTable_CoversEveryMemberAndNoOther()
    {
        // Arrange
        DiagnosticId[] members = Enum.GetValues<DiagnosticId>();

        // Act
        IEnumerable<DiagnosticId> pinned = _roster.Keys;

        // Assert
        pinned.Should().BeEquivalentTo(members);
    }

    [Theory]
    [MemberData(nameof(Members))]
    public void Values_EveryMember_MatchesItsPinnedNumber(DiagnosticId id)
    {
        // Arrange
        int expected = _roster[id].Value;

        // Act
        int actual = (int)id;

        // Assert
        actual.Should().Be(expected);
    }

    [Theory]
    [MemberData(nameof(Members))]
    public void Values_EveryMember_SitsInsideTheReservedBlockRange(DiagnosticId id)
    {
        // Arrange — common 1-99, Raw 100-199, datagram session 200-299, stream session 300-399. A
        // value outside every block breaks every Sink that range-checks, which is the style the enum's
        // docs say is supported.
        int value = (int)id;

        // Act
        bool inABlock = value is >= 1 and <= 399;

        // Assert
        inABlock.Should().BeTrue($"{id} = {value} must sit in one of the four reserved blocks");
    }

    [Fact]
    public void Values_EveryMember_HasAValueNoOtherMemberShares()
    {
        // Arrange — C# lets two enum members share a value silently, which would make one of them an
        // alias for the other in every Sink that switches on the id.
        DiagnosticId[] members = Enum.GetValues<DiagnosticId>();

        // Act
        int distinctValues = members.Select(id => (int)id).Distinct().Count();

        // Assert
        distinctValues.Should().Be(members.Length);
    }

    [Fact]
    public void Values_NoMemberIsZero_BecauseEveryRecordHasACauseByConstruction()
    {
        // Arrange
        DiagnosticId[] members = Enum.GetValues<DiagnosticId>();

        // Act
        bool anyZero = members.Any(id => (int)id == 0);

        // Assert
        anyZero.Should().BeFalse();
    }

    [Fact]
    public void Names_NoMemberIsACatchAll()
    {
        // Arrange
        string[] names = Enum.GetNames<DiagnosticId>();

        // Act
        IEnumerable<string> catchAlls = names.Where(name => name is "None" or "Unknown");

        // Assert
        catchAlls.Should().BeEmpty();
    }

    [Fact]
    public void Levels_EveryFactory_BuildsTheLevelItsMemberIsPinnedAt()
    {
        // Arrange — the level is fixed per member by the public DiagnosticRecord factories, so building
        // one record from each is the whole set of (id, level) pairs a Sink can ever receive.
        DiagnosticRecord[] built = BuildOneRecordFromEveryFactory();

        // Act
        IEnumerable<(DiagnosticId, DiagnosticLevel)> pairs = built.Select(record => (record.Id, record.Level)).Distinct();

        // Assert
        pairs.Should().BeEquivalentTo(_roster.Select(row => (row.Key, row.Value.Level)));
    }

    [Fact]
    public void Levels_NoFactory_BuildsAnErrorRecord()
    {
        // Arrange — ADR 0013: Error ships empty. A member proposed for it is a signal to check whether
        // the condition should be a thrown exception instead.
        DiagnosticRecord[] built = BuildOneRecordFromEveryFactory();

        // Act
        IEnumerable<DiagnosticId> atError = built.Where(record => record.Level == DiagnosticLevel.Error).Select(record => record.Id);

        // Assert
        atError.Should().BeEmpty();
    }

    [Theory]
    [MemberData(nameof(Members))]
    public void Docs_EveryMember_HasASummaryNamingBothValueSlots(DiagnosticId id)
    {
        // Arrange — ADR 0005: each value slot's meaning is documented on the member itself, including
        // when the slot is unused.
        XElement? member = LoadDocumentation()
            .Descendants("member")
            .SingleOrDefault(element => (string?)element.Attribute("name") == $"F:ReactorSockets.DiagnosticId.{id}");

        // Act
        string summary = member?.Element("summary")?.Value ?? string.Empty;

        // Assert
        summary.Should().Contain("Value0").And.Contain("Value1");
    }

    [Theory]
    [MemberData(nameof(Members))]
    public void Reachability_EveryMember_IsProducedByANamedTest(DiagnosticId id)
    {
        // Arrange
        IEnumerable<ProducerRow> producers = _producers.Where(row => row.Id == id);

        // Act
        int count = producers.Count();

        // Assert
        count.Should().BePositive($"a member nothing produces is either dead or untested, and {id} has no producer");
    }

    [Fact]
    public void Reachability_EveryNamedProducer_IsARunnableTest()
    {
        // Arrange
        ProducerRow[] rows = _producers;

        // Act
        IEnumerable<string> notRunnable = rows
            .Where(row => !IsRunnableTest(row.TestClass.GetMethod(row.TestMethod)))
            .Select(row => $"{row.TestClass.Name}.{row.TestMethod}");

        // Assert
        notRunnable.Should().BeEmpty();
    }

    private static bool IsRunnableTest(MethodInfo? method)
    {
        FactAttribute? fact = method?.GetCustomAttribute<FactAttribute>(inherit: true);
        return fact is not null && fact.Skip is null;
    }

    private static DiagnosticRecord[] BuildOneRecordFromEveryFactory() =>
        [.. typeof(DiagnosticRecord)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(method => method.ReturnType == typeof(DiagnosticRecord) && method.Name.StartsWith("For", StringComparison.Ordinal))
            .Select(method => method.Invoke(
                null,
                [.. method.GetParameters().Select(parameter => Activator.CreateInstance(parameter.ParameterType))]))
            .OfType<DiagnosticRecord>()];

    private static XDocument LoadDocumentation()
    {
        string assemblyPath = typeof(DiagnosticId).Assembly.Location;
        return XDocument.Load(Path.ChangeExtension(assemblyPath, ".xml"));
    }

    /// <summary>One member's pinned number and level in <see cref="_roster"/>.</summary>
    private readonly record struct RosterRow(int Value, DiagnosticLevel Level);

    /// <summary>One member and a test that produces it, in <see cref="_producers"/>.</summary>
    private readonly record struct ProducerRow(DiagnosticId Id, Type TestClass, string TestMethod);
}
