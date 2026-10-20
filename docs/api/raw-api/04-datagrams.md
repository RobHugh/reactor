# Datagrams

## Sending

```csharp
Source to = Source.FromIPEndPoint(new IPEndPoint(address, 9000));
Result result = reactor.SendTo(udp, to, payload);
if (!result.IsSuccess)
{
    Log(result.Error);
}
```

`SendTo` is one datagram, sent immediately, all or nothing. It never queues and never splits.

It fails without throwing when:

- the `LinkId` names no open Link,
- the Link is not a datagram Link,
- the socket refused the send, for example a payload above the path or UDP size limit.

`Error` is prose for logging. There is no error code to switch on.

## Receiving

```csharp
case RawEventKind.DatagramReceived:
    ReadOnlySpan<byte> body = evt.Payload;   // one whole datagram
    Source from = evt.Source;
    Process(evt.Link, from, body);
    break;
```

One event is one whole datagram. `Payload` is valid until the next `Pump`. Copy it if you keep it.

Each datagram Link delivers at most one datagram per Pump. See [The loop](02-the-loop.md).

## Source

`Source` is who the datagram claims to be from. It is copied from the IP header and is **not
authenticated**. Never treat it as an identity.

It is a value type: family, address bytes, IPv6 scope id and port. Equality compares all four, so two
link-local IPv6 addresses on different interfaces are different Sources.

Reply to the sender by passing it straight back:

```csharp
reactor.SendTo(evt.Link, evt.Source, reply);
```

Convert with `Source.FromIPEndPoint` and `source.ToIPEndPoint()`. `FromIPEndPoint` throws
`ArgumentException` for anything other than IPv4 or IPv6.

## Oversized datagrams

A datagram larger than `scratchBufferSize` is discarded whole. No event is raised. It is logged as
`DatagramTruncated` (102) with the claimed `Source` and the buffer size.

The whole datagram is dropped, not delivered cut short, so a `DatagramReceived` is always a complete
datagram on every platform. With the default `scratchBufferSize` of 65 507 this cannot happen.

## ICMP errors

A send to a closed port can come back as an ICMP port-unreachable. The OS reports it on a later
receive, not on the send. Raw logs it as `DestinationUnreachable` (101) with the `SocketError` and
keeps the Link open. No event is raised, so a remote cannot make you tear a Link down.

Any other receive failure closes the Link with `Failed` and logs `LinkFailed` (100).

## What Raw does not do for datagrams

No retransmission, ordering, deduplication, fragmentation or congestion control. Loss is loss. Use
`DatagramSessionReactor` if you want those.
