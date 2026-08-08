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
    /// record, carrying no body, is buffered at the front (ADR 0010, issue #23).
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
    /// Bytes have arrived at the front of the buffer behind a Goodbye already classified for this Peer
    /// (issue #93). A Peer that announced it is leaving and then keeps sending is contradicting its own
    /// announcement — nothing legitimate is ever buffered ahead of a Goodbye, by construction, so any
    /// further bytes are a violation regardless of what they would otherwise have classified as.
    /// </summary>
    DataAfterGoodbye,
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
        if (remoteGoodbyeAlreadyReceived && filled.Length > 0)
        {
            announcedLength = 0;
            messageEnd = 0;
            return MessageFramingOutcome.DataAfterGoodbye;
        }

        if (!MessageFramingCodec.TryReadLengthPrefix(filled, out announcedLength))
        {
            messageEnd = 0;
            return MessageFramingOutcome.AwaitingPrefix;
        }

        if (announcedLength == MessageFramingCodec.GoodbyeSentinel)
        {
            messageEnd = MessageFramingCodec.LengthPrefixSize;
            return MessageFramingOutcome.GoodbyeReceived;
        }

        if (announcedLength == 0)
        {
            messageEnd = 0;
            return MessageFramingOutcome.ZeroLengthPrefix;
        }

        if (announcedLength > (uint)maxMessageSize)
        {
            messageEnd = 0;
            return MessageFramingOutcome.OversizedPrefix;
        }

        messageEnd = MessageFramingCodec.LengthPrefixSize + (int)announcedLength;
        return filled.Length >= messageEnd ? MessageFramingOutcome.Ready : MessageFramingOutcome.AwaitingMessage;
    }
}
