# Changelog

## Unreleased

### Fixed

- Two Datagram Session Reactors with different `FragmentSize` values could not exchange large
  reliable Messages (#479, ADR 0024). A receiver acknowledged and then dropped a Fragment wider than
  its own `FragmentSize`. A `ReliableOrdered` Space then stopped delivering for the rest of the
  Session, a `ReliableUnordered` Message was lost, and the sender's `Send` returned `Sent`.
  Hello-with-cookie and Accept now carry the announcing end's `FragmentSize` and
  `ReceiveWindowFragments`. A reliable Message is cut at the smaller of the two `FragmentSize`
  values, fixed when the Peer reaches Established and unchanged across a Resume. `Send` returns
  `RemoteMessageTooLarge` when the Message needs more Fragments at that size than the peer's
  `ReceiveWindowFragments`. A reliable Fragment wider than the receiver's own `FragmentSize`, which
  only a non-conforming peer sends, closes the Peer with `ProtocolViolation` before its datagram is
  acknowledged, and logs the new `OversizedFragment` (210) Diagnostic. An announced `FragmentSize` outside 576 to 65 507 or a `ReceiveWindowFragments` of 0
  is refused like an illegal announced interval. The smaller `FragmentSize` is also the
  single-Datagram cap of the unreliable modes, so an unreliable `Send` to a peer with a smaller
  `FragmentSize` returns `MessageTooLarge` sooner.

### Breaking changes

- `DatagramSessionProtocol.Version` goes from 1 to 2, and a version 1 and a version 2 Datagram
  Session Reactor cannot connect. A version 2 Reactor refuses a version 1 dialler with
  `VersionMismatch`. A version 1 Reactor discards a version 2 Hello-with-cookie as malformed, so a
  version 2 dialler's dial goes unanswered.
- A smaller peer `FragmentSize` means more retention slots per Message, so `RetentionExhausted`
  arrives sooner. Receive memory is unchanged.

This log covers the closed pull requests up to the 1.0.0 release on `main` (2026-11-10). It describes
the code as it stands now. Where a later change replaced earlier behaviour, only the current
behaviour is listed. The risks section lists what the pull requests left open and the current code
still shows.

## Features

### Common to every profile

- One synchronous `Pump()` on the consumer's own thread drives everything. A Pump runs one
  `Socket.Select` scan from a rotating offset, and `Drain()` yields that Frame's Events. Payloads
  alias a preallocated frame arena and stay valid until the next Pump.
- `ReactorOptions`: `Capacity` (`required`), `AdmissionCap` (defaults to `Capacity`), `ArenaSize`
  (defaults to the profile floor), `TimeProvider`, and a Sink with its `MinimumLevel`. A combination
  that cannot work throws at construction, and the message states the arithmetic.
- The frame arena gives each Share-holder `arena ÷ capacity` bytes. One whole Message or Payload may
  always be delivered.
- `Source` is a 48-byte value with family, address, scope id and port. No surface takes a string
  host.
- `LinkId` and `PeerId` carry the issuing Reactor's Origin, a Slot index and a `ulong` generation. An
  id from another Reactor, or a stale id, addresses nothing.
- Every public member refuses a call from a foreign thread or from inside a Pump. Reactors are
  `IDisposable` and release every socket.
- Binds are exclusive and never `DualMode`.
- Linux and Windows are both supported. ICMP errors are reported on Linux through `IP_RECVERR` and
  `Select`'s error set.
- Diagnostics: a level-gated Sink that is disabled if it throws, and a `DiagnosticId` roster in
  per-profile blocks (Common, Raw 100s, datagram 200s, stream 300s) with every member's number,
  level and producing test pinned.
- `DiagnosticFormatter.Format` renders a record as text in the invariant culture. A Source renders
  as an unauthenticated claim.
- Counters are pull-only, monotonic and never reset, at Reactor and per-Link or per-Peer scope.
  Every counter documents why it cannot be derived from the others.
- `ReactorSockets.Logging` package: `LoggerSink(ILogger)`, one per Reactor, attributed by logger
  category. Both packages version together (1.0.0).
- `docs/options.md` lists every knob with its default, its provenance and the memory formula per
  profile. User guides cover the Raw, stream Session and datagram Session profiles under
  `docs/api/`.

### Raw profile (`RawReactor`)

- UDP: `Bind`, all-or-nothing `SendTo`, `DatagramReceived`. An oversized datagram is consumed and
  reported as `DatagramTruncated`. An ICMP port-unreachable is logged as `DestinationUnreachable`
  and never closes the Link.
- TCP: `Listen`, `Connect`, `Send`, `Shutdown` and `RemoteOf`. Accepts happen inside a Pump, bounded
  by the admission cap. `Send` never blocks, copies or queues, and arms `Writable` only after a short
  send. `EndOfStream` is separate from `Closed`, so half-close is usable.
- `CloseReason`: `LocalClose`, `ResetByRemote`, `Refused`, `Unreachable`, `Failed`.
- One legality table covers every (operation, Link kind) pair. Send-shaped calls return a `Result`.
  Queries throw.
- `AdmissionCapReached` and `CapacityExhausted` tell the two admission causes apart.
  `AdmissionDeferrals` counts Links that left a connection in the backlog.

### Stream Session profile (`StreamSessionReactor`)

- `StreamSessionOptions` composes `ReactorOptions`. Listening endpoints are bound once at
  construction and read back through `BoundEndpoints`. No public member exposes a socket.
- Handshake: fixed 41-byte Hello, Refusal and Resume records. The accepting end reads the dialler's
  first record and then decides in order: resume match, capacity, per-address limit. A version
  mismatch or a full Reactor gets an in-protocol Refusal. Linger is negotiated to the minimum of the
  two ends.
- Messages: `u32` length-prefixed framing, delivered as `MessageReceived` within the per-Peer read
  share. Malformed framing closes with `ProtocolViolation`.
- `Send` appends into a preallocated per-Peer retention ring and returns `Sent`,
  `RetentionExhausted`, `Closed` or `NotEstablished`. An empty Message throws `ArgumentException`.
- Writes are batched once per Pump, and Nagle is off. `SendBufferSize` sets `SO_SNDBUF`.
- Delivery counter: a control record that releases retention through the count the peer confirmed.
  It is sent once per Pump in which the count advanced, and on a two-way Peer it rides the same
  gather write as retention. It waits for a Message boundary.
- Two independent idle timers. A Peer that has written nothing for `SendIdleInterval` sends a
  delivery counter. A Peer that hears nothing for the peer's interval × `MissedHeartbeats` moves to
  `Interrupted`.
- `Interrupted`: the Peer keeps its `PeerId`, counters, retention, undelivered Messages and capacity
  Slot for Linger. `Send` still succeeds until retention fills. Linger expiry closes with
  `LingerExpired`.
- Resume: the dialling side re-dials on a jittered schedule within Linger and sends a Resume. The
  secret is compared in constant time, and the matched Peer rewinds to the peer's reported count and
  raises `PeerResumed`. A resume point below the ring's head closes with `RetentionGap`.
- `Close` sends a Goodbye. A Peer still holding deliverable Messages raises `PeerClosing` first.

### Datagram Session profile (`DatagramSessionReactor`)

- Cookie-guarded handshake over fixed 256-byte control datagrams: Hello, Cookie, HelloWithCookie,
  Accept, Refusal. Cookies are stateless HMAC-SHA256 on a 30-second epoch. One reply answers one
  datagram, and malformed input gets no reply.
- A pending dial resends its current handshake flight on a doubling, jittered schedule, restarts
  from a fresh Hello when its cookie goes stale, and closes with `TransportFailed` one Linger after
  `Connect`.
- HelloWithCookie and Accept announce `MaxMessageSize`, send-idle interval and Linger. Each end uses
  the minimum Linger and logs `LingerNegotiated`.
- Wire format: a 20-byte header with two ack blocks and 7-byte packet records. Session demux is by a
  random 32-bit token, so a NAT rebind keeps the Session.
- Four delivery modes, each with its own `u32` sequence Space and receive window: `ReliableOrdered`,
  `ReliableUnordered`, `UnreliableSequenced` and `Unreliable` (duplicates suppressed in a 64-entry
  window).
- One coalesced datagram per Peer per Pump. A record that does not fit waits for the next Pump.
- Reliability: ack blocks with a horizon rule, an RFC 6298 adaptive RTO with Karn's algorithm,
  backoff that resets on delivery, and bounds derived from the observed Pump interval and Linger.
  Retransmission is per record.
- Fragmentation for the reliable modes. A wide Message is cut into Fragments on contiguous
  sequences, with no message ID on the wire, and reassembled inside the receive window with no
  separate buffer. Each end announces its Fragment size, and a reliable Message is cut at the
  smaller of the two (see Unreleased).
- `Send` returns `MessageTooLarge` above one datagram in an unreliable mode, and
  `RemoteMessageTooLarge` above the peer's announced limit in any mode. A refused Send burns no
  sequence and takes no retention.
- A quiet Session sends one reliable Ping per `SendIdleInterval`. A Peer that hears nothing for the
  peer's interval × `MissedHeartbeats` moves to `Interrupted`, and Linger expiry closes it.
- Resume over a new path, authorised by the handshake secret. A Resume from the Peer's current IP
  moves at once. From a new IP it must first answer a `PathChallenge` with a matching
  `PathResponse`.
- Goodbye is an ordered reliable record. `Close` waits for the cumulative ack or Linger. A closed
  Peer keeps a small record for one Linger to re-ack a resent Goodbye.
- A Fragment-size stall (unfragmented records acked, no Fragment acked over four rounds) closes the
  Peer with `FragmentSizeUnviable`. Nothing is probed or resized.
- Diagnostics: `FragmentSetAbandoned` for a set that can never complete, `OversizedFragmentSet`,
  `OversizedFragment`, `UnreliableFragment`, `OversizedMessage`, `RetransmissionTimerBackedOff`.
- Memory is preallocated at construction and states its bill exactly (`ReceiveBudgetBytes`,
  `SendBudgetBytes`). `HeldRecordBudgetBytes` states the ceiling for records held behind an ordered
  gap.

### Abuse limits

- `MaxPeersPerAddress` on both Session profiles. It defaults to `Capacity - 1`, at least 1, and
  ignores the port. IPv6 addresses are grouped by `PeerLimitIPv6PrefixLength` (default 64). A
  refusal logs `AddressPeerLimitReached`.
- `MaxDatagramsPerPump` limits reads per bound socket per Pump on the datagram profile, logged as
  `DatagramReadLimitReached`.
- A refused stream connection is read for at most one handshake record. Past that it gets a
  `ProtocolViolation` Refusal.
- Announced intervals outside 1 ms to 2^32 − 1 ms are rejected by both handshake codecs. Seeded
  fuzz tests show that no handshake input throws out of `Pump`.

### Breaking changes for 1.0 consumers

- `SendIdleInterval` or `Linger` above 2^32 − 1 ms throws at construction on both Session profiles.
  An end that was on an earlier version with a value above that cannot handshake with an upgraded
  end.
- `ReactorOptions.Capacity` is `required`.
- An unreliable datagram `Send` above the peer's limit now returns `RemoteMessageTooLarge` instead
  of `Sent`.

### Build and tooling

- CI runs the suite on Linux and Windows, plus a `format` job.
- Every project builds with `EnforceCodeStyleInBuild` and `TreatWarningsAsErrors`.
- No `InternalsVisibleTo`. Tests reach only the public surface, and a type is public only because
  a consumer uses it (ADR 0015, ADR 0022).
- Test instruments: `HostilePeer`, `WireObserver`, `WireTap` and `LossyProxy` (scheduled and
  standing faults, captures, path moves). Harness waits use a wall-clock `PumpDeadline`.

## Bug fixes

### Identity and lifecycle

- An id from one Reactor could close a live Peer or Link of another, first within one profile and
  then across the two Session profiles.
- `default(LinkId)` addressed the first Link in Slot 0.
- `Connect` on a full stream Reactor leaked the connecting socket.
- A Send to a Peer still handshaking closed it with `TransportFailed` (stream) or was misreported as
  `RemoteMessageTooLarge` (datagram).
- An empty stream Message was sent and made the far end close with `ProtocolViolation`.

### Transport and platform

- On Unix, a refused connect was lost by reading `LocalEndPoint` before the error.
- ICMP unreachable was never seen on Linux.
- A queued ICMP error stopped the datagram read loop and starved the Peers behind it.
- A Raw listener at capacity accepted and then dropped connections instead of leaving them in the
  backlog. Slot exhaustion logged nothing.

### Stream retention and the delivery counter

- A remote that confirmed a Message not yet sent could make the next `Send` overwrite queued bytes.
- A retention flush could start behind a half-sent handshake or counter record. A stale remainder
  blocked retention for good after a dialling-side Resume.
- A delivery counter could land inside a half-sent Message frame.
- The boundary ring was missing from the documented memory bill.

### Datagram reliability

- Records past a datagram's capacity were dropped silently at flush.
- Retransmitted Fragments lost their index and count.
- The ack horizon never slid.
- Backoff doubled twice per loss event, never reset, and could overflow its deadline into the past.
- Acks never freed retention, so Pings stopped and reliable Sends leaked slots.
- A reliable Message the frame arena could not hold was acked and then lost.
- A sender closed on a selective ack of its Goodbye while an earlier Message was still missing.
- A zero observed Pump interval delayed the first stream re-dial to Linger.

### Fragments

- Forged Fragment sets passed the overlap check: observed instead of declared ranges, modes pooled
  together, and an anchor the attacker could move.
- A Fragment in an unreliable mode was accepted on receive.
- A selectively retransmitted subset of a set was rejected as malformed.
- Overlapping groups could each complete from the other's pieces, and `Pump` could throw out of
  range.
- A lost Fragment, or a Message as wide as the window, could stop a `ReliableOrdered` Space for good
  while the Session stayed open.
- An ordered Space held behind a stalled head grew without limit.
- `MaxMessageSize` was announced but not enforced on the datagram receive side.
- An unreliable Message over the peer's limit was reported `Sent` and then discarded.

### Resume

- A matched datagram Resume was interrupted again in the same Pump.
- A Resume naming a dialled Peer could move its send address.
- A Resume with a forged source address could redirect a Peer's traffic to a victim.

### Hostile input and abuse

- An extreme announced interval threw `OverflowException` out of `Pump`.
- One address could take every capacity Slot, on both profiles and through an IPv6 prefix.
- A datagram flood could keep one `Pump` from returning.
- A refused stream connection was drained without limit.
- The stream codec accepted a zero `MaxMessageSize`, an unmapped refusal reason and non-zero
  padding.
- A lost handshake datagram left a datagram dial pending forever.

### Counters and documentation

- A malformed datagram naming a live Session was counted as unknown-source.
- `SendBudgetBytes` understated the allocation.
- `ReadShareExhaustions` on Raw Links could never move and was removed. It now exists per Peer.
- `CODEMAP-AGENT-GUIDE.md` did not load on case-sensitive checkouts.
- Intermittent Windows test failures caused by bounding harness waits by Pump count.

## Risks

Each risk was stated in a closed pull request and is still present in the code.

### Security and abuse

- **R1 A stream connection that never sends holds a socket indefinitely.** A just-accepted
  connection is held until its first record arrives, with no timeout. The admission cap limits
  accepts per Frame, not how long a held connection lives.
- **R2 Path challenge limits.** Where an IP is shared (carrier-grade NAT, office NAT, shared host), a
  Peer can still be redirected to another port on that IP. A captured Resume replayed from the
  Peer's own IP still moves it. A forged `PathChallenge` that guesses a dialler's 32-bit session id
  is charged to that Peer's counters. Responses on the wrong Link, and Peers that start closing
  after a challenge, are untested.
- **R3 Per-address limit gaps.** The default of `Capacity - 1` only stops one address taking the
  last Slot. A Peer stays charged to the address it was admitted from, so other hosts behind that
  address (such as a NAT) have less room for the life of the Session. Interrupted and Closing Peers
  stay charged, so a client that restarts with a fresh handshake can be refused for up to Linger.
  IPv4 is not grouped. On the stream profile the charge starts before the handshake completes.
- **R4 The Pump read limit's default can be large.** Capacity 64 with a 1 MiB `MaxMessageSize`
  allows 223,808 reads per socket per Pump. A live Peer's datagrams wait behind a flood on the same
  socket and could reach the receive-idle deadline (not tested). Datagrams the OS drops when the
  socket buffer is full are not counted.
- **R5 A hostile peer can stop its own `ReliableOrdered` Space** by never filling a hole or never
  completing a held Fragment set. Other Peers are unaffected.
- **R6 A refused stream sender over the drain limit may see a reset before it reads its Refusal.**
  The two short-send paths of the drain are untested.

### Delivery and timing

- **R7 The accepting end is not RTT-seeded.** Until an RTT sample exists, its first reliable record
  is resent only at Linger. A dial whose HelloWithCookie was resent also starts with an RTO equal to
  Linger (ADR 0018).
- **R8 Fragment delivery retries can lose the arena race repeatedly.** The delay is unbounded but
  not permanent. A record refused because it would overwrite a held Fragment raises no counter or
  Diagnostic.
- **R9 Side effects of the ordered hold limit.** A Goodbye past the limit behind a stalled head
  goes unacked, so the sender waits out its Linger. `OutOfWindowPackets` also counts in-window records the Space will not hold.
  `HeldRecordBudgetBytes` excludes the short-lived arrays left when the ring grows.
- **R10 A discarded oversized `UnreliableSequenced` record still advances the high-water mark**, so
  an older record arriving after it is dropped.

### Observability

- **R11 A Fragment set still held when a Session closes is never reported** as abandoned.
- **R12 Duplicate Cookie or Accept datagrams** answering an earlier handshake copy are counted as
  `UnknownSourceDatagrams`.
- **R13 `HorizonDeferrals` is reconciled only when a datagram arrives**, so a Peer that sends and
  never receives reports zero.
- **R14 `BytesSent` on the datagram profile counts a datagram even when the transport send
  fails**, unlike `DatagramsSent`.
- **R15 Diagnostic values that depend on convention.** `PeerRefused` tells its two shapes apart by
  `Value1 == 0`, which holds only because no protocol version is 0. A `MalformedFraming` length
  prefix of 0 can mean a zero prefix or a stream that ended early.

### Test coverage

- **R16 Paths with no test that would fail on regression:** a stream write failure producing
  `Interrupted`, Raw `CloseReason.Unreachable` and `Failed`, a fuzzed stream Resume reaching the
  retention rewind, the `fragmentCount` boundary at 65,535, and the send-side `u32` sequence wrap.
  `DiagnosticId` reachability is checked by review, not by asserting each member.
- **R17 The partial-set flood memory test is sensitive to the ack path.** If ack sending starts to
  allocate, the test fails with no change to reassembly.
- **R18 Some tests depend on the environment.** Path-challenge tests bind `127.0.0.2`, which stock
  macOS does not route. The Fragment-size diagnosis fixture spends bounded wall-clock time in its
  handshake.

### Documentation and process

- **R19 The committed DocFX snapshot in `docs/api/docfx` is stale**, including types that are now
  internal.
- **R20 CI hardening is incomplete.** A failing check does not block a merge, because branch
  protection is not available on the repository's current GitHub plan. Actions are pinned by tag
  (`@v4`), not by SHA, and there is no `permissions:` block and no CodeQL.
- **R21 About 80 inline `//` comments in `src/` still cite tracker numbers.** `DatagramAssembler`
  has no callers in `src/`.
