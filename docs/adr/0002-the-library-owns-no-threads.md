# The library owns no threads

Every socket operation — `Select`, receive, send, handshake, retransmit — happens synchronously on
the caller's thread inside `Pump`, and nowhere else. A Reactor is owned by exactly one thread and
shares nothing with any other; a process may run several independent Reactors on several threads,
each internally serialized, as Schmidt's §8.5 permits. Pumping from the wrong thread is a
programmer error and throws.

This buys the property the library is for: no locks anywhere in the internals, no memory barriers on
the hot path, and behaviour that is reproducible because identical inputs produce identical work.
It is also what makes the frame arena and the `ref struct` event model safe without any
synchronization.

## Consequences

- **A long frame does not drain the OS receive buffer.** `SO_RCVBUF` sizing becomes a documented
  tuning concern, and a caller that stalls will drop UDP datagrams.
- **Handlers are non-preemptive** (Schmidt §7.2): slow work inside the library costs the caller's
  frame directly.
- **Nothing ticks unless the caller calls.** Heartbeats, Linger expiry and retransmits only advance
  during a `Pump`, so a caller that stops pumping stops the protocol.
- **No singleton.** Schmidt's `Initiation_Dispatcher::instance()` cannot be used; handlers bind to an
  explicit Reactor instance.
- Time comes from an injected `TimeProvider` and must be monotonic (`GetTimestamp()` /
  `GetElapsedTime()`); `GetUtcNow()` is never consulted for a protocol decision, since a wall-clock
  step could expire every Session in the process at once.
