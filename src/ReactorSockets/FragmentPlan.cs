using System.Diagnostics;

namespace ReactorSockets;

/// <summary>
/// The outcome of <see cref="FragmentPlan.Resolve"/>: either the Fragment count a Message is cut
/// into, or a permanent refusal — the caller maps <see cref="IsMessageTooLarge"/> to
/// <see cref="SendOutcome.MessageTooLarge"/> or <see cref="SendOutcome.RemoteMessageTooLarge"/> by mode
/// alone (issue #284). Named-value pass/fail, the same shape <see cref="Result"/> uses for the public
/// surface, extended with the count a success needs to hand back.
/// </summary>
public readonly record struct FragmentPlanResult
{
    private FragmentPlanResult(bool isMessageTooLarge, int fragmentCount)
    {
        IsMessageTooLarge = isMessageTooLarge;
        FragmentCount = fragmentCount;
    }

    /// <summary>
    /// Whether the Message cannot be carried at all: either an unreliable mode and the Message is wider
    /// than one Datagram, or a reliable mode and the Message is wider than <c>maxMessageLength</c> — the
    /// peer's announced receive limit (ADR 0004, issue #284). <see cref="FragmentCount"/> is meaningless
    /// when this is <see langword="true"/>.
    /// </summary>
    public bool IsMessageTooLarge { get; }

    /// <summary>
    /// The number of Fragments the Message is cut into. Valid only when
    /// <see cref="IsMessageTooLarge"/> is <see langword="false"/>. Always at least 1: a Message that
    /// fits in zero Fragments does not exist, because even an empty Message is one Message that needs
    /// one record to carry it (issue #242).
    /// </summary>
    public int FragmentCount { get; }

    /// <summary>
    /// Builds a success carrying <paramref name="fragmentCount"/> Fragments. Public because the type
    /// itself is (ADR 0015): a consumer that can observe this result through <see cref="FragmentPlan.Resolve"/> must
    /// be able to construct one, the same coherence <see cref="MessageTooLarge"/> already has as a
    /// public singleton.
    /// </summary>
    public static FragmentPlanResult ForFragmentCount(int fragmentCount) => new(false, fragmentCount);

    /// <summary>The refusal: the Message cannot be carried by the mode it was sent on.</summary>
    public static FragmentPlanResult MessageTooLarge { get; } = new(true, 0);
}

/// <summary>
/// The arithmetic that decides how a Message is cut into Fragments, extracted as a pure function
/// (issue #242, part of #48's slice-7 fragmentation work) so the boundary conditions — one byte,
/// exactly one Fragment, one byte over, an exact multiple, the cap — are fast unit tests rather than
/// something only a live Session and a live Peer could exercise. <see cref="Resolve"/> needs no
/// Session, no Peer and no reactor: it is total over its four inputs and never throws for a
/// <see cref="DeliveryMode"/> a caller could actually construct.
/// </summary>
/// <remarks>
/// <para>
/// Public for the same reason <see cref="RoundTripEstimator"/> is (ADR 0015): a test pins this
/// behaviour by calling <see cref="Resolve"/> directly with hand-picked lengths, with no
/// <c>InternalsVisibleTo</c> and no interface invented for the purpose. It is not part of a
/// consumer's request path — nothing here is reached by name from application code — but the seam
/// this library tests through has no other way to reach arithmetic this precise.
/// </para>
/// <para>
/// <b>Fragmentation is reliable-only (ADR 0004).</b> <see cref="DeliveryMode.ReliableOrdered"/> and
/// <see cref="DeliveryMode.ReliableUnordered"/> cut a Message of <c>n</c> bytes at or under
/// <c>maxMessageLength</c> into <c>ceil(n / fragmentSize)</c> Fragments, because a lost Fragment is
/// repaired by the same retransmit machinery that repairs a lost small Message; a Message wider than
/// <c>maxMessageLength</c> is refused with <see cref="FragmentPlanResult.MessageTooLarge"/> regardless
/// of mode, the one statement of that rule. <see cref="DeliveryMode.Unreliable"/> and
/// <see cref="DeliveryMode.UnreliableSequenced"/> never fragment: a Message that already fits in one
/// Datagram's worth of payload is exactly one Fragment, and anything wider is refused outright with
/// <see cref="FragmentPlanResult.MessageTooLarge"/> rather than cut, because nothing retransmits an
/// unreliable Fragment if it is lost — you may only fragment what you can retransmit.
/// </para>
/// <para>
/// <b>A zero-length Message answers 1 Fragment in every mode.</b> An empty payload is still one
/// Message that needs one record to carry it, so <c>ceil(0 / fragmentSize)</c> is defined as 1 rather
/// than 0 — a Message can never occupy zero Fragments — and it is trivially at or under an unreliable
/// mode's one-Datagram cap. This is the one case the plain ceiling formula would get wrong on its own,
/// which is why it is called out here rather than left to fall out of the arithmetic.
/// </para>
/// </remarks>
public static class FragmentPlan
{
    /// <summary>
    /// Decides how a Message of <paramref name="messageLength"/> bytes is cut when sent under
    /// <paramref name="mode"/> with <paramref name="fragmentSize"/>-byte Fragments.
    /// </summary>
    /// <param name="messageLength">
    /// The Message's length in bytes. Zero is a defined input, not an edge case a caller must special-
    /// case first: it answers 1 Fragment in every mode.
    /// </param>
    /// <param name="fragmentSize">
    /// The bytes one Fragment carries — a Reactor's resolved fragment size
    /// (<see cref="DatagramSessionOptions.ResolvedFragmentSize"/>). Must be at least 1; this is a
    /// caller invariant enforced by <see cref="DatagramSessionOptions.Validate"/> long before a Message
    /// reaches here, so it is asserted rather than reported through the return value.
    /// </param>
    /// <param name="mode">The delivery mode the Message is sent under.</param>
    /// <param name="maxMessageLength">
    /// The reliable receive limit a Message is checked against — the peer's own announced
    /// <c>MaxMessageSize</c> (issue #284), not this Reactor's <see cref="DatagramSessionOptions.MaxMessageSize"/>:
    /// a reliable Send is capped by how much the far end is willing to receive, not by this end's own
    /// setting. Ignored by the two unreliable modes, which are capped at <paramref name="fragmentSize"/>
    /// instead. Defaults to <see cref="int.MaxValue"/> — effectively no cap — so a caller with no
    /// reliable limit to enforce (every existing caller of this overload before issue #242's too-large
    /// rule moved here) does not have to invent one.
    /// </param>
    /// <returns>
    /// A <see cref="FragmentPlanResult"/> carrying the Fragment count, or
    /// <see cref="FragmentPlanResult.MessageTooLarge"/> when <paramref name="mode"/> cannot carry a
    /// Message this large.
    /// </returns>
    public static FragmentPlanResult Resolve(
        int messageLength, int fragmentSize, DeliveryMode mode, int maxMessageLength = int.MaxValue)
    {
        Debug.Assert(messageLength >= 0, "a Message length is never negative.");
        Debug.Assert(fragmentSize >= 1, "a fragment size of zero could never carry a byte.");
        Debug.Assert(maxMessageLength >= 0, "a maximum message length is never negative.");

        return mode switch
        {
            // Reliable modes divide, but only up to the receive limit the peer announced: past it there
            // is no width the mode could carry, however many Fragments cut it, so this is the one
            // statement of the reliable too-large rule (issue #48's standards fix — Send used to repeat
            // it as its own message.Length > MaxMessageSize check). Long math avoids overflow for a
            // messageLength near int.MaxValue before the division brings the result back down.
            DeliveryMode.ReliableOrdered or DeliveryMode.ReliableUnordered =>
                messageLength > maxMessageLength
                    ? FragmentPlanResult.MessageTooLarge
                    : FragmentPlanResult.ForFragmentCount(
                        messageLength == 0 ? 1 : (int)(((long)messageLength + fragmentSize - 1) / fragmentSize)),

            // Unreliable modes never fragment (ADR 0004): at or under the cap is exactly one Fragment,
            // and anything over it is refused rather than cut, because a lost Fragment here is never
            // retransmitted.
            DeliveryMode.Unreliable or DeliveryMode.UnreliableSequenced =>
                messageLength <= fragmentSize
                    ? FragmentPlanResult.ForFragmentCount(1)
                    : FragmentPlanResult.MessageTooLarge,

            // Every DeliveryMode a caller can construct is one of the four arms above; this catches
            // only a value forced through a cast, which is our bug rather than something to answer
            // for silently.
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, $"unrecognised delivery mode {mode}."),
        };
    }
}
