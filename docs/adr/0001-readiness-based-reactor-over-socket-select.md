# A readiness-based Reactor on Socket.Select, not .NET's completion-based I/O

.NET's public socket API is Proactor-shaped: `Socket.xxxAsync`, `SocketAsyncEventArgs` and everything
built on `Task`/`await` deliver handlers on *completion* of an operation the caller already started,
scheduled onto arbitrary `ThreadPool` threads. The readiness-based demultiplexer Schmidt describes
exists in .NET only as `Socket.Select` or hidden inside the runtime where it cannot be reached.
We deliberately build on `Socket.Select` anyway, because this library exists to serve a caller that
already owns a loop and must decide for itself when networking happens — completion-based delivery
would hand results back on threads and at moments the caller does not control, which is precisely
what consumers of this library are trying to avoid.

## Considered options

- **Completion-based (`Socket.xxxAsync` + `async`/`await`).** What Kestrel does, and the runtime's
  own recommendation. Rejected: it inverts control back onto the platform, and results arrive
  asynchronously on pool threads.
- **A background I/O thread feeding a queue the caller drains.** Rejected: this is the Proactor
  shape, and it is already what `System.Net.Sockets` does internally via epoll/kqueue on Unix and
  IOCP on Windows. Reimplementing it would produce a worse copy of machinery we get for free.
- **`Socket.Select` on the caller's thread.** Chosen.

## Consequences

- `Socket.Select` is O(n) in the watched set and takes non-generic `IList`s that it mutates in place.
  Acceptable only because capacity is fixed and small (default 64); the list churn must be designed
  around, not used naively, or it allocates every frame.
- We forgo the runtime's IOCP/epoll machinery entirely. This design does not scale to thousands of
  connections and is not intended to.
- The event loop's ownership is inverted relative to Schmidt's paper: there, the dispatcher owns the
  process (`for(;;) handle_events()`); here it is a subordinate step inside the caller's loop.
