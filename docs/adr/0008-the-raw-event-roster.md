# The Raw event roster

A `RawReactor` emits seven event kinds. The kind determines exactly which fields are present, so no
field in the struct is ever conditionally meaningless:

```
Accepted         { Listener, Link }
Connected        { Link }
SegmentReceived  { Link, Payload }
DatagramReceived { Link, Source, Payload }
Writable         { Link }
EndOfStream      { Link }
Closed           { Link, Reason }
```

This is `RawEvent`, its own `ref struct` beside a now Session-shaped `SessionEvent`. A single shared
struct would carry `Listener`, `Source` and `Payload` fields dead in one profile or the other, which is
the argument ADR 0007 used to split the Reactors.

**A Link is any registered transport.** A listening socket is a Link, a connection is a Link, a bound
UDP socket is a Link — one identity type, one lifecycle, one `Closed` event. `Accepted` therefore names
two Links, the listener that fired and the connection it produced, which is what a consumer running
several listeners needs. The alternative was a third identity type in a library that had just spent an
ADR arguing identities should be few. **`Listener` does not become a term**: it is a role a Link plays,
not a kind of thing, which closes half of the open multiple-Listeners gap without a glossary entry.

The consequence is that some operations are legal on some Links and not others. `Send` and `Shutdown`
are stream-only, `SendTo` is datagram-only, and neither is legal on a listener. The wrong call returns
a failed `Result` rather than throwing. This is runtime checking where the Reactor split was
compile-time checking, and the inconsistency is accepted deliberately: typed link handles would have
made events carry a bare `LinkId` the consumer must map back to a wrapper before replying — a
dictionary the library made them keep, paid on every event, to prevent a mistake an escape-hatch
consumer makes with a wrong variable rather than a wrong model.

`Send` on a stream takes what the socket will take and reports the count; `SendTo` on a datagram is
all-or-nothing. `RemoteOf` answers a stream Link's remote address, which is Link state — constant for
its lifetime — and therefore not event data. That is why `Accepted` carries no address: `Source`
remains exclusively the per-datagram claim, and the glossary's warning that a Source is never an
identity survives intact, which it could not do if the field were verified half the time.

## The Reactor holds no outbound bytes

`Send` never blocks, never copies and never queues. A send that takes fewer bytes than offered leaves
the remainder with the consumer, marks the Link as wanting `Writable`, and the next Pump that finds it
writable emits one `Writable` event and clears the mark. A Link the consumer never over-sent on never
produces one.

The arming rule is what makes the event affordable: a socket is writable nearly always, so a
level-triggered `Writable` would produce up to sixty-four non-events per Pump for the consumer to skip
past. Armed by a short send, it is a reply to something the consumer did.

Queuing was the obvious alternative and was rejected on the same ground as everything else in Raw. An
unbounded queue is a memory leak with a network cause; a bounded one requires a limit we would have to
justify and a way to learn when to retry, which reintroduces the event under another name. Either way
the Reactor would be inventing a per-Link FIFO policy — machinery a Session has and Raw was said not to
lend.

**`EndOfStream` is separate from `Closed`.** TCP's close is one-directional, and a peer that has sent
FIN can still receive. Collapsing the two would decide on the consumer's behalf that their protocol
cannot use half-close, in the one profile that exists so they can decide such things themselves —
request-response protocols that signal end-of-request by shutting down the write half are exactly what
someone reaches for Raw to build. `Shutdown(link)` sends our own FIN. The cost is a half-open state the
consumer must handle or leak.

## What the Sink absorbs, and why

Two rules divide the event stream from the Diagnostics, and they are worth stating as rules because
each was reached separately and they agree:

**Anything a remote can generate at will goes to the Sink, never the stream.** An ICMP port-unreachable
arriving after an earlier send surfaces on Windows as a `WSAECONNRESET` on the *next receive* and says
nothing about the socket's health; it is dropped, the Link lives, and a Diagnostic records it. A
datagram larger than the receive buffer is consumed to clear the head of the queue — leaving it unread
would block every datagram behind it forever — and reported the same way. Both would otherwise hand a
hostile or misconfigured remote a way to generate one event per packet.

**Anything platform-specific stays out of permanent public API.** `Closed` carries a `CloseReason` we
define and can keep stable — `LocalClose`, `ResetByPeer`, `Refused`, `Unreachable`, `Failed` — while the
exact `SocketError` travels in the Diagnostic emitted at the same moment, in the `Value` slots that
already exist. Sharing Session's `DisconnectReason` was rejected for the reason that split the enum in
the first place: most of its members are protocol vocabulary Raw can never produce.

The truncation rule discards bytes Unix would have delivered, since Windows fails an oversized
`recvfrom` with `WSAEMSGSIZE` and discards the datagram before we could see it. Identical behaviour on
both platforms is worth more here than the partial payload, and a `Truncated` flag would make the
glossary's definition of a Datagram — one whole unit — conditional on the one event whose entire
promise is that it is whole.

## Consequences

- **`DiagnosticRecord`'s subject becomes profile-neutral.** It carried a `PeerId`, a word ADR 0007 made
  Session-only. It is now an index-plus-generation subject that is a `PeerId` from a `SessionReactor`
  and a `LinkId` from a `RawReactor`, empty when the record concerns the Reactor itself.
  `IReactorDiagnostics`, `DiagnosticFormatter` and `ReactorSockets.Logging` all stay single, so a
  consumer running one Reactor of each kind gets one log stream — which is the entire point of logging.
  The field's meaning depends on its emitter, which is the price.
- **A consumer who wants the OS error code must install a Sink and correlate it themselves.** The
  `Closed` event and its Diagnostic arrive in the same Pump but are not linked by anything but the
  Link and the moment.
- **The glossary's Link entry widens** from "a TCP connection or a bound UDP socket" to "one registered
  transport", covering listeners. `Handle`'s note that a Raw consumer holds a Link instead is
  unaffected and now covers more.
- **The half-open state is real and leakable.** A consumer who treats `EndOfStream` as the end and never
  calls `Close` holds the Link and its slot until the Reactor is disposed. There is no Linger in Raw to
  reclaim it, because Raw has no timer policy of its own.
- **Raw's construction-time knobs now include the receive buffer size**, which determines what counts as
  an oversized datagram. `MaxDatagramSize` and receive-buffer sizing stay with the open wire-format
  gap rather than being settled here.
- **The multiple-Listeners gap narrows to binding semantics** — IPv6, dual-stack, and multiple binds to
  one port. Listener identity is settled: it is a Link.
- The `DiagnosticId` members this ADR implies — an unreachable, an oversized datagram, a link failure
  carrying a `SocketError` — join the open diagnostic-roster gap rather than being enumerated here.
