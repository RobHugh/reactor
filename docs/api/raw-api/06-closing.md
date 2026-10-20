# Closing

## Closing a Link yourself

```csharp
reactor.Close(link);
```

`Close` releases the socket immediately and queues one `Closed` with `LocalClose`. The Slot is free
for the next `Bind`, `Listen`, `Connect` or accept.

The event is added to the current Frame, not the next one. Inside a `foreach` over `Drain()` you
reach it later in the same loop. After the loop, only another `Drain()` before the next `Pump` sees
it.

`Close` is idempotent. A stale `LinkId` or an already closed Link is a no-op.

`Close` works on every kind of Link:

- A datagram Link stops receiving.
- A listening Link stops accepting. Connections it already accepted are separate Links and stay open.
- A stream Link closes both directions at once. The OS sends a FIN, or a reset if unread data was
  still in the receive buffer. Use `Shutdown` and wait for `EndOfStream` first if both sides must
  finish cleanly.
- A dial still in flight is abandoned.

## When the remote closes

| What the remote did | What you get |
|---|---|
| Sent FIN | `EndOfStream`. The Link stays open. See [Streams](05-streams.md#half-close). |
| Reset the connection | `Closed` with `ResetByRemote`. |

## Every CloseReason

| Reason | Meaning |
|---|---|
| `LocalClose` | You called `Close`. |
| `ResetByRemote` | The remote reset the connection. |
| `Refused` | A dial was refused. Nothing was listening. |
| `Unreachable` | A dial's host or network was reported unreachable. |
| `Failed` | Anything else: a dial or read failure not covered above. |

Every reason except `LocalClose` arrives with a `LinkFailed` (100) diagnostic in the same Pump, carrying
the platform `SocketError`. They are linked only by the Link and the Pump. Install a Sink if you need
the exact error.

Treat `Unreachable` versus `Failed` as advice on whether a retry is worthwhile, not as a portable
classification. The same address can map differently per platform. Dialling the limited broadcast
address is `Failed` on Windows and `Unreachable` on Linux.

`Closed` is final. The `LinkId` is dead from that event onwards.

## Shutting down

```csharp
foreach (LinkId link in myStreamLinks)
{
    reactor.Shutdown(link);
}

// pump until each remote's EndOfStream, or your own deadline
reactor.Dispose();
```

`Dispose` closes every Link the same way `Close` does, but queues no events. Do the `Shutdown` step
first if the far ends need to see your data end before the connection does.
