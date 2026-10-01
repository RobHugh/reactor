namespace ReactorSockets;

/// <summary>
/// A point-in-time snapshot of one Link's counters (ADR 0013): monotonic figures running from that
/// Link's registration, with no reset, fetched by value and keyed by <see cref="LinkId"/> — there is
/// no accessor that enumerates every Link's counters in one call.
/// </summary>
/// <remarks>
/// There is no read-share figure here, unlike <see cref="PeerCounters"/>. The Raw profile reads a
/// ready Link once per Pump, so a Link never reaches a second reservation in one Frame and its read
/// share can never be exhausted. A field that could only ever read zero fails ADR 0013's counter test.
/// </remarks>
/// <param name="BytesSent">
/// Bytes the transport accepted for sending on this Link. Not derivable: the Reactor-scope datagram
/// figures count datagrams rather than bytes, cover every Link at once, and never see a stream Link.
/// </param>
/// <param name="BytesReceived">
/// Bytes delivered from this Link into the Arena. Not derivable: no other figure attributes received
/// bytes to one Link, and the Arena high-water is a per-Frame peak rather than a running total.
/// </param>
public readonly record struct LinkCounters(
    ulong BytesSent,
    ulong BytesReceived);
