# Links

A Link is one registered transport. There are three kinds, and you always know which one you hold
because you chose the member that made it.

| Made by | Kind | What it is |
|---|---|---|
| `Bind` | datagram | A bound UDP socket. Answers many senders. |
| `Listen` | listening | A bound, listening TCP socket. Produces stream Links. |
| `Connect`, or an `Accepted` event | stream | One TCP connection. |

## Binding a UDP socket

```csharp
LinkId udp = reactor.Bind(new IPEndPoint(IPAddress.Any, 9000));
```

The socket is bound and registered before `Bind` returns. Datagrams arrive as `DatagramReceived`
from the next Pump.

## Listening for TCP

```csharp
LinkId listener = reactor.Listen(new IPEndPoint(IPAddress.Any, 9000));
```

Each accepted connection arrives as `Accepted`, already registered as a new stream Link:

```csharp
case RawEventKind.Accepted:
    LinkId connection = evt.Link;          // the new stream Link
    LinkId via        = evt.ListeningLink; // which listener fired
    IPEndPoint remote = reactor.RemoteOf(connection);
    break;
```

`Accepted` carries no address. A connection's addresses are fixed for its lifetime, so ask
`RemoteOf` and `LocalOf`.

### Admission

A Pump accepts until one of these stops it:

1. The backlog is empty.
2. `AdmissionCap` accepts have happened this Pump. Logged as `AdmissionCapReached`.
3. Every Slot is taken. Logged as `CapacityExhausted`.

A connection that is not accepted stays in the OS backlog for a later Pump. Raw sends nothing to the
remote while it waits. If the backlog itself overflows, the OS decides what the remote sees.

A failed accept is logged as `LinkFailed` against the listening Link. The listening Link stays open.

## Dialling

```csharp
LinkId link = reactor.Connect(new IPEndPoint(address, 9000));
```

`Connect` never blocks. It returns the `LinkId` at once, and the Link holds a Slot from that moment.
The dial resolves on a later Pump as one of:

- `Connected`. The Link is ready to send and receive.
- `Closed` with `Refused`, `Unreachable` or `Failed`, plus a `LinkFailed` diagnostic carrying the
  `SocketError`.

Wait for `Connected` before you `Send`.

There is no connect timeout. A dial that never resolves stays pending until you `Close` it. Run your
own deadline if you need one.

`LocalOf` answers for a dialled Link immediately, because TCP binds the local port when the connect
begins.

## Binding rules

These apply to `Bind` and `Listen`:

- Binds are exclusive. A port already in use throws `InvalidOperationException` naming the
  endpoint and the `SocketError`.
- No dual-stack. `IPAddress.Any` binds IPv4 only and `IPAddress.IPv6Any` binds IPv6 only. Bind twice
  for both.
- Port 0 picks a free port. Read it back with `LocalOf(link).Port`.
- No hostnames. Resolve them yourself.

`Bind`, `Listen` and `Connect` all throw `InvalidOperationException` when every Slot is taken. No
socket is created in that case.

## Which call fits which Link

| Call | Datagram | Stream | Listening |
|---|---|---|---|
| `SendTo` | yes | fails | fails |
| `Send` | fails | yes | fails |
| `Shutdown` | fails | yes | fails |
| `RemoteOf` | throws | yes | throws |
| `LocalOf`, `CountersOf`, `Close` | yes | yes | yes |

The wrong call is reported, never silently ignored:

- `SendTo`, `Send` and `Shutdown` return a failed `Result` or `SendResult` whose `Error` names the
  problem, for example `Send is illegal on a datagram Link — use SendTo.`
- `RemoteOf` returns data, so it has no failure channel and throws `ArgumentException`.

## Stale LinkIds

A `LinkId` holds the Reactor it came from, a Slot index and a generation. Closing a Link advances
the Slot's generation, so a stale `LinkId` never addresses whichever Link takes the Slot next. A
`LinkId` from one Reactor never addresses a Link in another. A `default` `LinkId` addresses nothing.

| Member | Given a closed or foreign `LinkId` |
|---|---|
| `SendTo`, `Send`, `Shutdown` | Failed result: `Link … is not open.` |
| `LocalOf`, `RemoteOf`, `CountersOf` | Throws `ArgumentException`. |
| `Close` | No-op. |

A Link closing under you is an ordinary race, which is why the sending members report it rather than
throw.
