# The Reactor Pattern in C#: Mapping Schmidt's Model onto .NET

## Purpose and scope

Douglas Schmidt's PLoP-94 paper (`source-docs/PLoP-94.md`) defines the Reactor pattern in terms of five participants —
Handle, Synchronous Event Demultiplexer, Initiation Dispatcher, Event Handler, and Concrete Event Handler — built
around a blocking, readiness-based `select()` call and a single-threaded, non-preemptive dispatch loop. This document
investigates how (and whether) that model maps onto .NET's networking and concurrency primitives, using only primary
sources: official Microsoft Learn API docs, the `dotnet/runtime` and `dotnet/aspnetcore` source trees, and the
DotNetty repository. Every claim below is inline-cited to its source. Section 5 is explicitly labeled as this
document's own synthesis rather than a sourced finding.

---

## 1. Is Reactor the right mental model for C#, or is .NET actually Proactor-shaped?

Schmidt's own paper answers part of this question directly: "The Reactor pattern can be considered a synchronous
variant of the asynchronous **Proactor** pattern... The Proactor supports the demultiplexing and dispatching of
multiple event handlers that are triggered by the completion of asynchronous events. In contrast, the Reactor pattern
is responsible for demultiplexing and dispatching of multiple event handlers that are triggered when it is possible
to initiate an operation synchronously without blocking" (`source-docs/PLoP-94.md`, Section 10). The question for a
C# library is which side of that split the platform's I/O primitives actually sit on.

**.NET's high-performance socket API is completion-based, not readiness-based.** The
[`SocketAsyncEventArgs` class documentation](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socketasynceventargs)
states it is "part of a set of enhancements to the `System.Net.Sockets.Socket` class that provide an alternative
asynchronous pattern... specifically designed for network server applications that require high performance," and
describes the operating pattern as: allocate a context object, set its properties, call an `xxxAsync` method, and
"if the asynchronous socket method (`xxxAsync`) returns true, in the callback, query the context properties for
completion status." The `Completed` event fires when the *operation itself* finishes — not when a handle becomes
"ready" for an operation the caller must then perform. This is the Proactor shape as defined in Schmidt's own
taxonomy: dispatch triggered by completion of an asynchronous operation, not by readiness to begin a synchronous one.

**On Windows, this completion model is implemented directly on top of I/O Completion Ports (IOCP).** The
[`ThreadPoolBoundHandle` class](https://learn.microsoft.com/en-us/dotnet/api/system.threading.threadpoolboundhandle)
is documented as representing "an I/O handle that is bound to the system thread pool and enables low-level components
to receive notifications for asynchronous I/O operations," via `AllocateNativeOverlapped`, which returns a pointer to
a `NativeOverlapped` structure and an `IOCompletionCallback` delegate invoked when the operation completes — the
managed surface over Win32 overlapped I/O and IOCP.

**On Linux/Unix, the runtime does not expose an epoll-based readiness API to managed code; it hides an epoll loop
inside the implementation and re-presents it as completions.** The source file
[`SocketAsyncEngine.Unix.cs`](https://github.com/dotnet/runtime/blob/main/src/libraries/System.Net.Sockets/src/System/Net/Sockets/SocketAsyncEngine.Unix.cs)
contains the comment: "The responsibility of `SocketAsyncEngine` is to get notifications from epoll|kqueue and
schedule corresponding work items to ThreadPool (socket reads and writes)." `SocketAsyncEngine` runs a dedicated
background thread that loops on `WaitForSocketEvents()` (the epoll/kqueue wait) and then queues completions to the
`ThreadPool` (or, when `DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS` is set, invokes them inline on the event
thread). This is architecturally a **Reactor internally** (an epoll-based demultiplexer with dispatched handlers) —
but it is fully encapsulated inside `System.Net.Sockets`; it is not an API surface an application or a library author
can register handlers with directly.

**Conclusion for this section:** the *public* API surface .NET exposes for networking (`Socket.xxxAsync`,
`SocketAsyncEventArgs`, and everything built on `Task`/`async`/`await`) is Proactor-shaped: handlers/continuations are
invoked on completion of an operation the caller already initiated, not on readiness to begin one. The
Reactor-shaped mechanism (readiness demultiplexing via epoll/kqueue, or IOCP's completion queue) exists only at the
runtime's internal implementation layer, which is not exposed publicly. A library that wants to *say* "Reactor
pattern" while sitting on idiomatic .NET I/O is, at the public-API level, actually building a **Proactor** in
Schmidt's terminology, using the runtime's IOCP/epoll machinery as its underlying asynchronous operation provider.
The one place a true readiness-based, Schmidt-style Reactor could be built in C# is directly on
[`Socket.Select`](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socket.select), which is a thin,
literal wrapper over BSD `select()` (see Section 2).

---

## 2. Candidate C# primitives for each Reactor role

### Handle → `Socket` / `SafeHandle`

Schmidt defines Handles as identifying "resources that are managed by an OS... commonly network connections, open
files, timers, and synchronization objects" (`source-docs/PLoP-94.md`, Section 5). In .NET, the direct analogue is
[`System.Net.Sockets.Socket`](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socket) for the
socket-specific case, and more generally
[`System.Runtime.InteropServices.SafeHandle`](https://learn.microsoft.com/en-us/dotnet/api/system.runtime.interopservices.safehandle),
documented as an abstract "wrapper class for operating system handles" that provides "critical finalization of
handle resources, preventing handles from being reclaimed prematurely by garbage collection and from being recycled
by the operating system to reference unintended unmanaged objects." `SafeHandle` is the general OS-resource-handle
abstraction the runtime itself uses for files, sockets, pipes, wait handles, etc. (concrete derived types such as
`SafeFileHandle`, `SafePipeHandle`, `SafeWaitHandle` are listed on the same page). For a Reactor-style library,
`Socket` (which wraps a `SafeSocketHandle` internally) is the natural Handle for network events specifically.

### Synchronous Event Demultiplexer / event loop

Schmidt's demultiplexer "blocks awaiting events to occur on a set of Handles" and returns when an operation can
proceed "without blocking," with `select()` as the canonical implementation (`source-docs/PLoP-94.md`, Section 5).
.NET offers three non-equivalent candidates:

- **`Socket.Select`** — the literal analogue. The
  [API documentation](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socket.select) describes it as
  a static method taking `checkRead`/`checkWrite`/`checkError` `IList`s of sockets and a timeout: "`Select` returns
  when at least one of the sockets of interest... meets its specified criteria, or the [timeout] is exceeded... After
  calling `Select`, the `IList` will be filled with only those sockets that satisfy the conditions." This is a true,
  readiness-based, blocking demultiplexer, structurally identical to BSD `select()` — but it is a thin platform
  wrapper with the same historical limitations as native `select` (fixed socket-count ceilings on some platforms,
  O(n) rescans), and building a hand-rolled event loop on it forgoes the runtime's IOCP/epoll machinery entirely.
- **No public readiness-based multiplexer exists for the runtime's actual async I/O path.** As shown in Section 1,
  the runtime's internal epoll/kqueue loop (`SocketAsyncEngine.Unix.cs`) and Windows IOCP loop
  (`ThreadPoolBoundHandle`) are not exposed as a "demultiplexer you register handles with" API; they are wired
  directly into `Socket.xxxAsync`/`SocketAsyncEventArgs` completion delivery.
- **`System.IO.Pipelines`** does not act as an event demultiplexer at all — it is a buffer-management layer sitting
  *above* the I/O source. Per the
  [official docs](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines), `System.IO.Pipelines` "is a
  library designed to make high-performance I/O in .NET easier," built around a `Pipe` producing a
  `PipeReader`/`PipeWriter` pair; a caller still drives it explicitly, e.g. calling `socket.ReceiveAsync(memory,...)`
  and writing the result into a `PipeWriter` (per the docs' own `FillPipeAsync` example). It composes with whatever
  event/completion source feeds it; it is not itself a Synchronous Event Demultiplexer.

The practical conclusion: an idiomatic .NET library gets its "wait for events across many handles" behavior for free
from `async`/`await` plus the runtime's internal (non-public) IOCP/epoll loop — it does not need to, and generally
should not, implement its own `select()`-style multiplexer, unless it deliberately wants Schmidt's literal
readiness-based model via `Socket.Select`.

### Initiation Dispatcher → custom event-loop type vs. delegating to Task infrastructure

Two designs are viable:

1. **Delegate to the existing Task-based Asynchronous Pattern (TAP) infrastructure.** Per
   [Microsoft's TAP overview](https://learn.microsoft.com/en-us/dotnet/standard/asynchronous-programming-patterns/task-based-asynchronous-pattern-tap),
   "the task-based asynchronous pattern is the recommended asynchronous design pattern for new development," and "a
   TAP method might even have nothing to execute, and might just return a `Task` that represents the occurrence of a
   condition elsewhere in the system." Here, `Task`/`Task<T>` scheduling (backed by the CLR `ThreadPool`, itself fed
   by the internal IOCP/epoll completion loop) plays the role of the Initiation Dispatcher's `handle_events` loop;
   `register_handler`/`remove_handler` have no direct analogue because "registration" is implicit in calling an
   `xxxAsync` method and awaiting its `Task`.
2. **A custom dispatcher type**, explicitly modeled on Schmidt's `Initiation_Dispatcher`
   (`register_handler`/`remove_handler`/`handle_events`), that owns the handle table and drives dispatch itself. This
   is architecturally closer to what DotNetty does (see Section 3): a library-owned event-loop abstraction sits
   between raw sockets and application handler code, rather than exposing `Task` completions directly.

### Event Handler / Concrete Event Handler → delegates vs. `IEventHandler` interface, single- vs. multi-method

Schmidt's own Implementation section (`source-docs/PLoP-94.md`, Section 8.3–8.4) already frames this as an open
design choice in C++: "Event Handler objects" vs. "Event Handler functions" for the dispatch target, and
"single-method" (`handle_event(Event_Type et)`) vs. "multi-method" (`handle_input`/`handle_output`/`handle_timeout`/…)
interfaces, with the tradeoff that single-method invites switch statements while multi-method requires the framework
author to anticipate the method set in advance. C# offers direct equivalents for both axes:

- **Delegates as the dispatch target** — analogous to Schmidt's "Event Handler functions." A C# design would use
  `Action<T>`/`Func<T, Task>`/a custom delegate type as the callback registered per event kind. This is the shape
  TAP itself uses via continuations (`ContinueWith`, or implicitly via `await`), per the
  [TAP docs](https://learn.microsoft.com/en-us/dotnet/standard/asynchronous-programming-patterns/task-based-asynchronous-pattern-tap):
  "The creator of the continuation code has control over where that code executes. You can create the continuation
  code either explicitly, through methods on the `Task` class (for example, `ContinueWith`), or implicitly, by using
  language support built on top of continuations (for example, `await` in C#)."
- **An `IEventHandler`-style interface as the dispatch target** — analogous to Schmidt's "Event Handler objects,"
  giving Concrete Event Handlers state plus behavior and making them subclassable/poolable, at the cost of requiring
  a type per handler role (mirroring `Logging_Acceptor`/`Logging_Handler` in the paper's own example).
- **Single-method vs. multi-method** maps unchanged into C#: a single `HandleEvent(EventType type)` method with a
  discriminated `EventType` enum (idiomatic C# would likely use a `switch` expression over a closed
  discriminated-union-like type, or pattern matching on an event record) vs. separate methods/events per event kind
  (`OnConnected`, `OnDataReceived`, `OnClosed`, …), which is closer to how `SocketAsyncEventArgs.Completed` plus
  `LastOperation`/`SocketAsyncOperation` is actually consumed in Microsoft's own reference implementation (see the
  `IO_Completed` → `switch (e.LastOperation)` pattern in the
  [`SocketAsyncEventArgs` example code](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socketasynceventargs)).
  Notably, Microsoft's own canonical `SocketAsyncEventArgs` sample uses the single-method-with-switch style Schmidt
  warns limits extensibility — evidence that the tradeoff Schmidt identified in 1994 for C++ still applies verbatim
  to a completion-based C# design today.

---

## 3. Prior art: how production .NET networking libraries actually structure this

### `System.IO.Pipelines`

As described in Section 2, `System.IO.Pipelines` is not itself a Reactor or Proactor — it is a buffer-management
abstraction over a stream of bytes, motivated by the problems the
[docs](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines) enumerate with naive `Stream.ReadAsync` loops:
partial reads, ignored return values, multiple messages per read, and unbounded/uncontrolled buffer growth. It
introduces `PipeReader`/`PipeWriter`, `GetMemory`/`Advance`/`FlushAsync`, and — notably for concurrency-model
purposes — a pluggable [`PipeScheduler`](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines): "By
default: The current `SynchronizationContext` is used. If there's no `SynchronizationContext`, it uses the thread
pool to run callbacks," with `PipeScheduler.ThreadPool` "the default and generally the best choice" and
`PipeScheduler.Inline` flagged as able to "cause unintended consequences such as deadlocks." This scheduler hook is
functionally where a library could plug in Reactor-style serialized dispatch (see Section 4).

### Kestrel's Sockets transport (`dotnet/aspnetcore`)

Kestrel's socket transport is built entirely on the completion-based (Proactor-shaped, per Section 1) public API, not
on a hand-rolled readiness loop. In
[`SocketConnectionListener.cs`](https://github.com/dotnet/aspnetcore/blob/main/src/Servers/Kestrel/Transport.Sockets/src/SocketConnectionListener.cs),
the accept path is a `while (true)` loop awaiting `_listenSocket.AcceptAsync(cancellationToken)` and handing the
resulting `Socket` off to a connection factory (`_factory.Create(acceptSocket)`), which wraps it in a
`SocketConnectionContext`. There is no dedicated accept thread and no manual `select`/epoll call in this code — it
is plain `async`/`await` over `Socket.AcceptAsync`, i.e., the library rides directly on the runtime's internal
completion machinery rather than reimplementing demultiplexing. The per-connection type
[`SocketConnection.cs`](https://github.com/dotnet/aspnetcore/blob/main/src/Servers/Kestrel/Transport.Sockets/src/Internal/SocketConnection.cs)
composes a `Socket`, a `SocketReceiver`, a `SocketSender`, and an `IDuplexPipe` — i.e., Kestrel layers
`System.IO.Pipelines` directly on top of `Socket`'s completion-based async methods per connection, confirming that
"Pipelines on top of `Socket.xxxAsync`" is the production pattern Microsoft itself uses, not a custom Reactor
dispatcher.

### DotNetty

[DotNetty](https://github.com/Azure/DotNetty) is described in its own README as "DotNetty project – a port of netty,
event-driven asynchronous network application framework," directly porting Java Netty's architecture — which is
explicitly Reactor/event-loop-shaped, using a `SingleThreadEventExecutor`/`IEventLoop` abstraction (confirmed by
project issue history referencing `SingleThreadEventExecutor` shutdown and task-polling behavior, e.g.
[Issue #7](https://github.com/Azure/DotNetty/issues/7) and
[Issue #239](https://github.com/Azure/DotNetty/issues/239) in the Azure/DotNetty repository) that owns a task queue
and runs registered work serially on a dedicated thread — the closest thing among these three libraries to a literal
C# reimplementation of Schmidt's Initiation Dispatcher, complete with an explicit single-threaded execution
guarantee per event loop (multiple event loops can run in parallel, each internally serialized) — mirroring
Schmidt's own note that "Event Handlers are only serialized within an instance of the Reactor pattern... multiple
Event Handlers in multiple threads can run in parallel" (`source-docs/PLoP-94.md`, Section 8.5).

**Summary across the three:** Kestrel — the highest-traffic, most scrutinized production .NET networking code —
does *not* implement a Reactor-style dispatcher; it composes `async`/`await` over `Socket.xxxAsync` with
`System.IO.Pipelines` for buffering. DotNetty, as a direct port of a JVM framework whose design predates
widespread `async`/`await`, *does* implement an explicit Netty-style single-threaded event-loop dispatcher. This is
a real fork in the design space, not just an implementation detail.

---

## 4. Concurrency model implications: reproducing (or diverging from) single-threaded serialization

Schmidt's Reactor guarantees serialized handler dispatch: "the Reactor pattern serializes the invocation of event
handlers at the level of event demultiplexing and dispatching within a process or thread. Serialization at the
Initiation Dispatcher level often eliminates the need for more complicated synchronization or locking within an
application process" (`source-docs/PLoP-94.md`, Section 7.1), and separately warns that "Event Handlers are not
preempted while they are executing," so long-running handlers block the whole loop (Section 7.2). .NET has several
primitives that can reproduce or deliberately break this guarantee:

- **`SynchronizationContext`.** The [`Channel<T>` docs](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels.channel-1)
  and [`System.IO.Pipelines` docs](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines) both reference it
  as the default target for posting continuations; the Pipelines doc states explicitly that "asynchronous code
  resumes on either a `TaskScheduler` or the current `SynchronizationContext`," and that `PipeScheduler` exists so a
  library "can have fine-grained control over where the I/O is performed" instead of accepting the ambient context. A
  custom `SynchronizationContext` (or a `PipeScheduler` backed by one) that posts all continuations to a single
  dedicated thread is a direct, faithful reproduction of Schmidt's single-threaded, non-preemptive dispatch
  guarantee in C# — every "handler" (continuation) runs to completion, one at a time, on one thread, exactly as in
  the original Reactor.
- **The default ThreadPool-backed continuation model deliberately diverges from Schmidt's guarantee.** Per the TAP
  docs, "you can determine where asynchronous execution occurs... execute the workload on the thread pool,
  implement it by using asynchronous I/O... run it on a specific thread... or use any number of potential contexts,"
  and by default (no captured `SynchronizationContext`, e.g. in a console/server app) continuations run on arbitrary
  `ThreadPool` threads — i.e., handlers for *different* connections/events can run concurrently, which is precisely
  the coarse-grained-concurrency-control property Schmidt attributes to Reactor's serialization, given up in exchange
  for using all CPU cores. This is consistent with what Section 3 found for `PipeScheduler.ThreadPool` being
  "the default and generally the best choice" in Pipelines.
- **`System.Threading.Channels.Channel<T>`** provides ordered, single-writer/single-reader-optimizable
  producer/consumer queues rather than a scheduling primitive per se. Per Microsoft's own guidance (surfaced via the
  [`Channel<T>` API reference](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels.channel-1) and
  its `ChannelOptions.SingleReader`/`SingleWriter` settings), setting `SingleReader: true` lets the implementation
  "avoid locks when reading... significantly reducing the overheads involved in consuming from the channel." A
  `Channel<T>` with a single dedicated consumer loop is a natural way to *serialize* delivery of events from many
  concurrent producers (e.g., many sockets' completions) into one ordered, single-threaded processing loop — i.e., a
  `Channel<T>` plus one consumer `Task` is a practical, idiomatic way to build Schmidt's serialized Initiation
  Dispatcher without hand-rolling a thread or a custom `SynchronizationContext`.

In short: .NET does not remove the ability to reproduce Reactor's single-threaded serialization guarantee — it moves
it from "the only option" (as it effectively was in Schmidt's synchronous, single-process C++ world) to "one
configurable choice among several," with the platform default (`ThreadPool`-scheduled continuations) actively opting
out of it for throughput.

---

## 5. Recommendation (synthesis / opinion — not sourced findings)

*Everything in this section is this document's own synthesis, not a claim traceable to a primary source. It should
be read as a proposal to evaluate, not a citation.*

Given the findings above, I would not market a library built on idiomatic .NET I/O as "the Reactor pattern in C#" —
the public API surface (`Socket.xxxAsync`, `Task`, `await`) is Proactor-shaped by construction, and Kestrel, the
most battle-tested prior art, confirms that fighting this by building a readiness-based dispatcher on top of
`Socket.Select` would mean reimplementing (and very likely under-performing) machinery the runtime already provides
internally via IOCP/epoll. The Reactor *vocabulary* is still useful for organizing the library's internals and its
docs — Handle, Event Handler, dispatcher — but the *mechanism* underneath should be Proactor/completion-based,
matching what the platform actually gives you.

Concretely, I'd aim for a public surface with three pieces:

1. **A connection/listener type** (`Handle` role) wrapping `Socket`, exposing `IDuplexPipe` (or something built on
   `PipeReader`/`PipeWriter`) rather than raw buffers — following Kestrel's own choice to layer Pipelines over
   `Socket`, which already solves the partial-read/buffer-management problems the Pipelines docs describe.
2. **A single `IEventHandler`-style interface with a small, closed set of methods** (e.g. `OnConnected`,
   `OnDataReceived(ReadOnlySequence<byte>)`, `OnClosed(Exception?)`) rather than Schmidt's single-method
   `handle_event(type)` switch style — Section 2 found that even Microsoft's own canonical `SocketAsyncEventArgs`
   sample uses the switch style Schmidt flagged as limiting extensibility, so there's no reason to repeat that
   mistake in a from-scratch design. A parallel delegate-based registration API (`OnDataReceived(Func<...>)`) can sit
   on top of the interface for consumers who don't want to define a type per handler.
3. **An explicit, swappable dispatch/serialization policy**, not a hidden one — surfaced as roughly "run handlers for
   this connection serially" (a `Channel<T>` + single consumer loop, or a dedicated `SynchronizationContext`, per
   connection or per some sharding key) versus "let the `ThreadPool` schedule freely." Section 4 shows both are
   legitimate, well-supported .NET patterns with opposite tradeoffs (Schmidt's simplicity-via-serialization vs.
   throughput-via-parallelism), and the repo's own goal — "a simple interface that abstracts away networking
   complexity" — argues for defaulting to the serialized-per-connection mode (so consumer callback code never has to
   reason about concurrent invocation for the same connection, matching people's mental model of "one handler,
   one connection") while allowing an opt-out for advanced/high-throughput consumers.

The internal complexity — wiring `Socket.xxxAsync` completions into `PipeWriter`, running the dispatch-policy
plumbing, pooling `SocketAsyncEventArgs`/buffers — should live entirely behind that interface and never leak into
the public method signatures, consistent with the CLAUDE.md goal that "internal design (event loop,
reactor/dispatcher, connection handling)" is expected to be more complex than what's exposed.

---

## Sources

- Douglas C. Schmidt, "Reactor: An Object Behavioral Pattern for Demultiplexing and Dispatching Handles for
  Synchronous Events" — `source-docs/PLoP-94.md` (local copy, this repo)
- [`SocketAsyncEventArgs` Class](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socketasynceventargs) — Microsoft Learn
- [`Socket` Class](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socket) — Microsoft Learn
- [`Socket.Select` Method](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.socket.select) — Microsoft Learn
- [`ThreadPoolBoundHandle` Class](https://learn.microsoft.com/en-us/dotnet/api/system.threading.threadpoolboundhandle) — Microsoft Learn
- [`SafeHandle` Class](https://learn.microsoft.com/en-us/dotnet/api/system.runtime.interopservices.safehandle) — Microsoft Learn
- [System.IO.Pipelines](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines) — Microsoft Learn
- [`Channel<T>` Class](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels.channel-1) — Microsoft Learn
- [Task-based Asynchronous Pattern (TAP): Introduction and overview](https://learn.microsoft.com/en-us/dotnet/standard/asynchronous-programming-patterns/task-based-asynchronous-pattern-tap) — Microsoft Learn
- [`SocketAsyncEngine.Unix.cs`](https://github.com/dotnet/runtime/blob/main/src/libraries/System.Net.Sockets/src/System/Net/Sockets/SocketAsyncEngine.Unix.cs) — dotnet/runtime source
- [`SocketConnectionListener.cs`](https://github.com/dotnet/aspnetcore/blob/main/src/Servers/Kestrel/Transport.Sockets/src/SocketConnectionListener.cs) — dotnet/aspnetcore source
- [`SocketConnection.cs`](https://github.com/dotnet/aspnetcore/blob/main/src/Servers/Kestrel/Transport.Sockets/src/Internal/SocketConnection.cs) — dotnet/aspnetcore source
- [Azure/DotNetty](https://github.com/Azure/DotNetty) — repository README
- [Azure/DotNetty Issue #7](https://github.com/Azure/DotNetty/issues/7), [Issue #239](https://github.com/Azure/DotNetty/issues/239) — `SingleThreadEventExecutor`/event-loop behavior
