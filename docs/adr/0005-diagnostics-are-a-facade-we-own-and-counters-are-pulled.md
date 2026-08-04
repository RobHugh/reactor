# Diagnostics are a facade we own, and counters are pulled

The library writes no logging library and takes no dependency that forces one on a consumer.
Diagnostics are two separate mechanisms, because counters and records have opposite requirements:
counters are cheap, continuous and only meaningful in aggregate, while records are rare,
individually meaningful, and expensive to render.

**Counters are pull-only.** A snapshot struct — datagrams sent and received, packets retransmitted,
datagrams discarded, arena and retention high-water marks, `SinkFaulted` — read by the consumer
whenever it wants one. On the hot path a counter costs one add and nothing else. Routing counts
through the record facade was rejected: it would put a level check and a call frame on every packet,
or force us to buffer counts internally and flush them, which is the pull mechanism rebuilt and
hidden.

**Records go through a facade we own**, on the order of `bool IsEnabled(DiagnosticLevel)` and
`void Log(in DiagnosticRecord)`, with four levels (`Error`, `Warn`, `Info`, `Trace`). A
`DiagnosticRecord` is a readonly struct of typed fields — a closed `DiagnosticId` enum, a level, a
`PeerId`, and two numeric slots. No strings, no `object[]`, no closures, nothing to allocate. A
static formatter renders a record to text on demand, so structured sinks never format and text sinks
get our wording rather than inventing their own. An optional, separately shipped
`Reactor.Extensions.Logging` package adapts the interface to `ILogger`.

Accepting `Microsoft.Extensions.Logging.Abstractions` directly was rejected on two grounds: it is
still a forced dependency, and it would put a type we do not own into the L2 contract that ADR 0003
commits to keeping stable. `ILogger` also cannot express the one thing this sink contract most needs
to say — that it is called on the hot path, inside the caller's frame.

Much of what a networking library would log is already an Event the consumer drains. What remains,
and what Diagnostics exist for, is what happens *below* the Event level and never reaches a Peer the
consumer knows about: a retransmit, a malformed datagram discarded from an unknown Source, a
rejected cookie, a stalled send window.

## The sink contract

A Sink is consumer code invoked synchronously, on the consumer's thread, inside `Pump`, possibly
between two mutations of internal state. Therefore:

- Calling back into the Reactor from a Sink is a programmer error and throws.
- Whatever a Sink does costs the caller's frame directly and non-preemptively, so a synchronous file
  or console write inside a frame is the stall ADR 0002 warns about. This must be stated on the
  interface, not buried in documentation.
- **A Sink that throws is caught, counted as `SinkFaulted`, and disabled for the life of that
  Reactor.** This is a deliberate exception to bugs-always-throw: `Pump` never abandoning a frame
  part-way is the invariant the whole event model exists to protect, and it would be perverse for
  the diagnostic channel — the thing you switch on when something is already wrong — to be the one
  component that can violate it. The observer must not be able to break the observed. Disabling
  rather than retrying also prevents a persistently broken Sink from costing one exception per
  record, which at `Trace` level could be thousands per frame.

## Consequences

- **Every new diagnostic is a public API addition.** `DiagnosticId` only grows, and a mistake in it
  is permanent.
- **`Value0` and `Value1` mean different things per `DiagnosticId`.** That is the price of
  fixed-width records, and it is the sort of positional convention that gets miscopied, so the
  meanings belong in XML docs on each enum member rather than in a table elsewhere.
- **A consumer's broken Sink goes quiet exactly when they need it**, discoverable only through a
  counter they must think to check.
- Consumers not using the adapter package write a small adapter of their own — friction imposed on
  all of them to avoid a dependency most would have accepted.
- Two things to document and two places for a consumer to look; anyone wanting counters in their log
  stream samples the snapshot and emits it themselves.
- `IsEnabled` plus a `MinimumLevel` enum compare gate construction of the record itself, so a
  disabled level costs a comparison and never reaches the interface call.
