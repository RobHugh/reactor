# ReactorSockets

A single-threaded, pull-driven networking library for applications that already own a loop. The
owning loop decides when networking happens; nothing is ever delivered to the consumer except in
response to a call the consumer made.

## Language

### The loop

**Pump**:
A single, synchronous advance of a Reactor: check which Handles are ready, move whatever bytes are
available, and make the resulting Events available to be drained.
_Avoid_: Poll, tick, update, run, service

**Frame**:
The span between the start of one Pump and the start of the next. The unit of lifetime for anything
the Reactor hands to the consumer.
_Avoid_: Cycle, iteration, step

**Reactor**:
One instance of the dispatcher and everything registered with it. A Reactor is owned by exactly one
thread and is never shared; a process may run several independent Reactors.

The word names the role, not a type. There are three — a `DatagramSessionReactor`, a
`StreamSessionReactor` and a `RawReactor` — and a given instance is one of them for its whole lifetime. A
Session Reactor's addresses are fixed at construction and never rebound; Raw registers its transports as
it goes. Say "a Reactor" when the profile does not matter and the type name when it does. The library is **ReactorSockets**, written in full every
time; "the Reactor" never means the package, and "Reactor" alone is never the product. The design
pattern is Schmidt's Reactor, named as the pattern when it is meant.
_Avoid_: Dispatcher, initiation dispatcher, event loop, engine, and "Reactor" as shorthand for the
package

**Drain**:
The consumer's act of reading the Events produced by a Pump, before the next Pump invalidates them.
_Avoid_: Consume, dequeue, flush

### What moves

**Segment**:
An arbitrary run of bytes that arrived from a Handle, carrying no guarantee of being a whole
anything. What a stream transport can honestly report, and therefore a Raw word only: a stream
Session frames the bytes itself and delivers Messages.
_Avoid_: Chunk, packet, buffer

**Message**:
A complete, self-delimited unit of application data. The thing a consumer is entitled to act on.
_Avoid_: Packet, frame, record, payload

**Datagram**:
A single unit as delivered by a message-oriented transport, where one receive yields exactly one
whole unit. In Raw a Datagram is a Segment and a Message simultaneously. In a datagram Session it is
a container: one Datagram carries a header and however many packets a Pump had ready, so the wire
unit and the unit of delivery are no longer the same thing.
_Avoid_: Packet, message

**Space**:
One sequence-numbered stream of packets within a Session, one per delivery mode. Packets in
different Spaces cannot block each other, because a hole in a Space is a hole only in that Space —
which is what makes a promise about one mode independent of traffic in another.
_Avoid_: Channel, stream, lane, queue

**Retention**:
The bounded store of data a sender holds after a Send, kept until the receiver's acknowledgement
says it can be released. Its exhaustion is the only backpressure the library has: a full Retention
fails a Send and never blocks, drops, or grows. Bounded in bytes and never shared between Peers, so
a failure always names the party that caused it; in a datagram Session it is per Space as well, so
one mode cannot exhaust another.
_Avoid_: Send buffer, outbox, backlog, queue

**Fragment**:
One of the pieces a Message is cut into because it exceeds what a single Datagram can carry. Unlike
a Segment, a Fragment is a deliberate division of a known whole — numbered, counted, and meaningless
until its set is complete.
_Avoid_: Chunk, piece, part, packet, split

### How a Peer is spoken to

**Raw**:
The profile in which a Reactor makes no claims about the bytes and invents nothing on the wire, so it
can address anything in the world. Segments in, Segments out. Raw is an escape hatch for building
your own protocol layer, not a toolkit for it — it defines no part of the Session protocol and lends
none of its machinery. A `RawReactor` carries both transports and deals in Links.
_Avoid_: Plain, passthrough, unmanaged, raw mode

**Session**:
The profile in which both ends run a Reactor and therefore share a protocol, buying Messages,
liveness, and survival across the loss of the underlying transport. Both Session Reactors deal in
Peers.

There are two, and they are two protocols rather than one protocol over two transports. A
`DatagramSessionReactor` builds reliability, ordering and delivery modes over UDP. A
`StreamSessionReactor` inherits those from TCP and builds framing, liveness and resumption on top —
the things a stream does not provide and half-open connections make necessary. Say "a Session" when
the transport does not matter and the type name when it does.
_Avoid_: Connection, channel, link, managed mode

**Heartbeat**:
What a Session sends when it has sent nothing else, so the other end learns the transport still
works. Distinct from a keepalive, which only holds a NAT mapping open and proves nothing about the
party behind it. A Session carrying traffic proves itself continuously and sends none.
_Avoid_: Keepalive, ping, poll, liveness check

**Interrupted**:
The state of a Session whose transport has been lost but whose Peer identity and undelivered
Messages are still being held, pending a Resume.
_Avoid_: Disconnected, dropped, reconnecting, stale

**Closing**:
The state of a Peer that will accept no further Messages but is still delivering the ones it already
holds. A Peer in this state is leaving deliberately, not failing.
_Avoid_: Draining, shutting down, half-closed

**Goodbye**:
The announcement that a Session is ending on purpose. Its presence forbids a Resume; its absence is
what makes one worth attempting.
_Avoid_: Bye, close message, FIN, shutdown

**Linger**:
The bounded period a Reactor will hold an Interrupted Session before abandoning it. Its expiry, not
the loss of the transport, is what closes a Peer. Both ends announce their own and the shorter one
governs, because the option to resume only exists for as long as both parties are still holding it.
_Avoid_: Timeout, grace period, TTL

**Resume**:
The re-establishment of a Session over a replacement transport, such that the Peer's identity and
its undelivered data survive the loss of the old one.
_Avoid_: Reconnect, retry, restore, failover

### Who it moves between

**Peer**:
The remote party a Session exchanges data with, identified stably by the consumer across the
lifetime of its association — not to be confused with the transport currently carrying it. A
Session-only word: Raw has no Peers, which leaves the term free for whatever a consumer's own
protocol layer calls its remote party.
_Avoid_: Client, connection, remote, endpoint, socket

**Link**:
One registered transport in a Raw Reactor — a listening socket, a TCP connection and a bound UDP
socket alike. Unlike a Peer, a Link is the transport: it has no identity beyond the thing carrying it,
and when it closes there is nothing left to resume. Listening is a role a Link plays, not a kind of
thing, which is why there is no such term as Listener.
_Avoid_: Connection, socket, stream, channel, session, listener

**Source**:
The address a datagram claims to have come from. Unauthenticated by construction — a hint about
origin, never an identity. A Link says which registered thing an event concerns; a Source says who
claims to have sent this particular datagram. Raw deals in Sources throughout; a datagram Session
names one only when the sender is not a Peer, which is the case its cookie exists to handle. It carries
its family, its address, its scope and its port, and because no socket is ever dual-mode there is exactly
one spelling of any address — an IPv4 Source never arrives wearing an IPv6 form.
_Avoid_: Sender, from, remote, origin

**Handle**:
The operating-system resource a Reactor watches for readiness. Schmidt's term, kept deliberately.
Internal in both profiles: what a Raw consumer holds is a Link, which names a registered transport
without granting access to the resource underneath it.
_Avoid_: Socket, descriptor, fd

### What comes out

**Event**:
Something that happened to a Peer or a Link during a Pump and is worth telling the consumer about.
Anything a remote party can generate at will is a Diagnostic instead, so that no one on the network
can decide how much of the consumer's Drain they occupy.
_Avoid_: Notification, callback, signal, message

**Writable**:
The Event answering a Send that could not take everything offered. It is a reply, not a status: a Link
the consumer never over-sent on never produces one, and a Reactor that reported writability whenever
it was true would say so about almost every Link on almost every Pump.
_Avoid_: Ready, drain, flush, sendable, unblocked

**EndOfStream**:
The Event saying a Link's remote party will send nothing further, while the Link remains open for
sending. A statement about one direction only — unlike Closing, which is a Peer's declared intent, and
unlike Closed, which ends the Link.
_Avoid_: Half-close, FIN, EOF, shutdown, finished

**Handler**:
An internal participant registered with a Reactor that is invoked when its Handle is ready. Handlers
are the Reactor's own machinery; consumers drain Events instead.
_Avoid_: Listener, subscriber, observer, callback

### What it says about itself

**Diagnostic**:
A record of something a Reactor did internally that never became an Event, offered to a Sink at the
moment it happens. Concerns the library's own workings rather than a Peer's data. It carries one
subject — a Peer, a Link, a Source, or none when the record is about the Reactor itself. A Diagnostic
says why this one happened; anything interesting only in aggregate is a Counter instead.
_Avoid_: Log, log entry, message, trace

**Sink**:
Consumer-supplied code a Reactor hands Diagnostics to. Called on the consumer's thread inside a
Pump, so whatever it costs is the consumer's cost.
_Avoid_: Logger, appender, writer, listener

**Counter**:
A running total a Reactor maintains and never reports on its own. Read when the consumer wants a
number — unlike a Diagnostic, which is offered when it happens. Counters only ever rise and are never
reset, so an interval is the consumer's own subtraction. Those kept per subject run for the life of
the Peer rather than the transport, which is what lets a count survive a Resume and show a path
flapping.
_Avoid_: Metric, stat, gauge, telemetry
