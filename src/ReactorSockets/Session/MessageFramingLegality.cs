namespace ReactorSockets.Session;

/// <summary>
/// Every state the front of an established Peer's receive buffer can be in once its length prefix is
/// weighed against this Reactor's announced <c>MaxMessageSize</c> (ADR 0010).
/// </summary>
internal enum MessageFramingOutcome
{
    /// <summary>
    /// Fewer than the four-byte length prefix has arrived yet. An ordinary, expected state while a
    /// receive buffer is filling — not a violation.
    /// </summary>
    AwaitingPrefix,

    /// <summary>
    /// A full, legal prefix has arrived, but not yet the Message body it describes. An ordinary,
    /// expected state while a receive buffer is filling — not a violation.
    /// </summary>
    AwaitingMessage,

    /// <summary>A whole, legal Message is buffered at the front and ready to be reserved and delivered.</summary>
    Ready,

    /// <summary>
    /// The prefix is <see cref="MessageFramingCodec.GoodbyeSentinel"/>: a whole Goodbye control
    /// record, carrying no body, is buffered at the front (ADR 0010).
    /// </summary>
    GoodbyeReceived,

    /// <summary>
    /// The prefix announces more bytes than this Reactor's announced <c>MaxMessageSize</c> allows.
    /// <c>MaxMessageSize</c> is a receive limit precisely so this case can be refused before any
    /// allocation sized by the claim — the whole reason it is announced.
    /// </summary>
    OversizedPrefix,

    /// <summary>The prefix announces zero bytes — not a legal empty Message.</summary>
    ZeroLengthPrefix,

    /// <summary>
    /// Bytes have arrived at the front of the buffer behind a Goodbye already classified for this Peer.
    /// A Peer that announced it is leaving and then keeps sending is contradicting its own announcement
    /// — nothing legitimate is ever buffered ahead of a Goodbye, by construction, so any further bytes
    /// are a violation regardless of what they would otherwise have classified as.
    /// </summary>
    DataAfterGoodbye,

    /// <summary>
    /// The prefix is <see cref="MessageFramingCodec.DeliveryCounterSentinel"/>, but fewer than
    /// <see cref="MessageFramingCodec.DeliveryCounterBodySize"/> bytes of its <c>u64</c> body have
    /// arrived yet. An ordinary, expected state while a receive buffer is filling — not a violation,
    /// and not yet a whole record either.
    /// </summary>
    AwaitingDeliveryCounterBody,

    /// <summary>
    /// A whole delivery-counter control record — <see cref="MessageFramingCodec.DeliveryCounterSentinel"/>
    /// followed by its full <c>u64</c> body — is buffered at the front. Decoding the count itself is
    /// left to the caller via <see cref="MessageFramingCodec.ReadDeliveryCounter"/>, exactly as
    /// <see cref="Ready"/> leaves Message-body extraction to its caller.
    /// </summary>
    DeliveryCounterReceived,
}

/// <summary>
/// Classifies every state the front of an established Peer's receive buffer can be in — the legal
/// states (<see cref="MessageFramingOutcome.Ready"/>, <see cref="MessageFramingOutcome.GoodbyeReceived"/>)
/// alongside the named violations — into either "wait for more", "deliver it" or a named violation.
/// Stated once, here, exactly as <see cref="RawLinkLegality"/> states the Link-kind legality matrix and
/// <c>StreamSessionReactor.TryReserveMessage</c> states the arena-reservation outcomes, rather than
/// inferred a branch at a time inside the Reactor that consumes it.
/// </summary>
/// <remarks>
/// <b>The discard arm refuses rather than permits.</b> <see cref="MessageFramingOutcome"/> is a plain
/// enum, so nothing stops a future member from being added without a row here; a
/// <see cref="StreamSessionReactor"/> switch over <see cref="Classify"/>'s result therefore treats an
/// unnamed outcome as a violation rather than silently continuing to buffer or deliver it — the same
/// fail-closed rule <see cref="RawLinkLegality"/> follows for a Link kind with no row.
/// </remarks>
internal static class MessageFramingLegality
{
    /// <summary>
    /// Classifies the front of <paramref name="filled"/> — the bytes an established Peer's receive
    /// buffer holds right now — against <paramref name="maxMessageSize"/>. <paramref name="remoteGoodbyeAlreadyReceived"/>
    /// is whether an inbound Goodbye has already been classified for this Peer; when it has, any bytes
    /// at the front of <paramref name="filled"/> are <see cref="MessageFramingOutcome.DataAfterGoodbye"/>
    /// regardless of what they would otherwise have classified as — checked before the length prefix is
    /// even read, so no claimed length ever drives this decision. <paramref name="announcedLength"/>
    /// is the length prefix once it has fully arrived, 0 otherwise. <paramref name="messageEnd"/> is
    /// the offset one past the Message's last byte, valid only when the result is
    /// <see cref="MessageFramingOutcome.Ready"/> — computing it eagerly for
    /// <see cref="MessageFramingOutcome.OversizedPrefix"/> would overflow for a prefix near
    /// <see cref="uint.MaxValue"/>, so it is left 0 there.
    /// </summary>
    public static MessageFramingOutcome Classify(
        ReadOnlySpan<byte> filled, int maxMessageSize, bool remoteGoodbyeAlreadyReceived,
        out uint announcedLength, out int messageEnd)
    {
        // TryReadLengthPrefix is a pure peek at bytes already in hand — calling it unconditionally
        // costs nothing and lets every outcome below be one switch expression. Its own out parameter
        // is 0 whenever it returns false, which is what feeds AwaitingPrefix; DataAfterGoodbye reports
        // 0 regardless of what (if anything) it read, matching the original short-circuit that never
        // let a claimed length drive that decision.
        bool hasPrefix = MessageFramingCodec.TryReadLengthPrefix(filled, out uint length);
        bool dataAfterGoodbye = remoteGoodbyeAlreadyReceived && filled.Length > 0;

        (MessageFramingOutcome outcome, uint reportedLength, int end) = (dataAfterGoodbye, hasPrefix) switch
        {
            (true, _) => (MessageFramingOutcome.DataAfterGoodbye, 0u, 0),
            (false, false) => (MessageFramingOutcome.AwaitingPrefix, 0u, 0),
            (false, true) when length == MessageFramingCodec.GoodbyeSentinel =>
                (MessageFramingOutcome.GoodbyeReceived, length, MessageFramingCodec.LengthPrefixSize),

            // Both sentinel checks sit ahead of ZeroLengthPrefix and OversizedPrefix below on purpose
            // (issue #109, mirroring how #95 pinned GoodbyeSentinel's own precedence): each sentinel is
            // itself a "length" far larger than any legal MaxMessageSize, so checking size first would
            // misclassify a whole sentinel-led record as an oversized claim before ever recognising what
            // it actually is.
            (false, true) when length == MessageFramingCodec.DeliveryCounterSentinel
                && filled.Length < MessageFramingCodec.DeliveryCounterRecordSize =>
                (MessageFramingOutcome.AwaitingDeliveryCounterBody, length, 0),
            (false, true) when length == MessageFramingCodec.DeliveryCounterSentinel =>
                (MessageFramingOutcome.DeliveryCounterReceived, length, MessageFramingCodec.DeliveryCounterRecordSize),
            (false, true) when length == 0 => (MessageFramingOutcome.ZeroLengthPrefix, length, 0),
            (false, true) when length > (uint)maxMessageSize => (MessageFramingOutcome.OversizedPrefix, length, 0),
            (false, true) when filled.Length < MessageFramingCodec.LengthPrefixSize + (int)length =>
                (MessageFramingOutcome.AwaitingMessage, length, 0),
            (false, true) => (MessageFramingOutcome.Ready, length, MessageFramingCodec.LengthPrefixSize + (int)length),
        };

        announcedLength = reportedLength;
        messageEnd = end;
        return outcome;
    }
}
