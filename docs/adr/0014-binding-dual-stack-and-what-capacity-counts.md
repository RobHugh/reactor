# Binding, dual-stack, and what capacity counts

ADR 0008 narrowed the multiple-Listeners gap to binding semantics and settled the identity half of it: a
listening socket is a Link, and `Listener` is a role rather than a term. ADR 0012 settled every knob in
the library except where a socket's address comes from. ADR 0013 put a full endpoint inside
`DiagnosticRecord` and named this gap as the first thing that would have to justify it. This ADR closes
what is left — the address vocabulary, dual-stack, several binds to one port, where a bind lives on each
of the three surfaces, and what capacity counts once a Reactor's Handles stop being its Peers.

The last of those turned up a rule in ADR 0012 that is wrong in one profile. It is corrected here rather
than in a footnote, because this is the ADR that makes the error visible.

## Two address types, and no resolver

**The cold surface speaks `IPEndPoint`; the record path carries a `Source` value.**

Binding and dialling happen a handful of times in a process, so an allocation there is free and the type
should be the one the consumer already holds — `IPEndPoint` is what configuration parsers produce and
what `Dns.GetHostAddresses` feeds. The record path is the one place where a value type is load-bearing:
ADR 0013 sized `DiagnosticRecord` at roughly 56 bytes with the endpoint inline, and an `IPEndPoint` there
would make every Diagnostic allocate and hand the consumer a mutable object whose lifetime differs from
the record's. Two spellings of one idea, with a documented conversion, is the price of getting both paths
right; one type everywhere would have had to be wrong on one of them.

**The library never resolves a name.** No surface takes a hostname. ADR 0002 says the library owns no
threads, so a resolve could only happen inside a Pump — a blocking call of unbounded duration on the
consumer's loop thread, in a library whose entire proposition is that the loop stays theirs. A name is
also the wrong input for the next decision: one that has both an A and an AAAA record chooses the address
family on the consumer's behalf, silently, which is exactly what the next section refuses to do.
Resolution belongs before construction, where the consumer picks the address and pays the latency
somewhere they control.

## The family follows the endpoint

**`DualMode` is never set.** `IPAddress.Any` binds IPv4, `IPv6Any` binds IPv6, and a consumer who wants
both binds twice.

.NET creates an `InterNetworkV6` socket with `IPV6_V6ONLY` on every platform, so v6-only is already the
uniform starting point regardless of the host's `bindv6only` setting; dual-stack is an opt-in we decline.
Declining costs an extra bind and buys **one spelling per address**. Under `DualMode` an IPv4 peer
arrives as `::ffff:203.0.113.7`, so every IPv4 address exists in two forms and the library must either
carry both or normalise. Normalising is not free where it matters most: ADR 0009 keys the cookie HMAC
over the source address, so the value hashed would no longer be the value the socket reported, on the one
path that must reproduce exactly on the next datagram. The datagram profile's resume matching compares
the same field.

Two binds is not an extension of the design but a shape it has supported since before this gap opened.
ADR 0008 made a listener a Link and gave `Accepted` two Link fields specifically for *"a consumer running
several listeners"*.

A `DualStack` convenience that opens both sockets under one registration was rejected: it is permanent
public surface wrapping two calls the consumer can already make, and it owes an answer for the case where
one family binds and the other fails.

## Binds are exclusive, and reuse is not a knob

**No address-reuse option is exposed, and the Reactor binds exclusively.** A bind to a port already in use
fails at registration on every platform for the same reason. On Windows the library sets
`ExclusiveAddressUse` explicitly, so that uniformity is a fact rather than a hope: Windows is the one
platform where `SO_REUSEADDR` lets a second bind take over a live socket, and it would be the one platform
where the consumer is told nothing.

The rule being applied is ADR 0008's — **anything platform-specific stays out of permanent public API**. A
single `ReuseAddress` flag would mean "rebind a port in `TIME_WAIT`" on Unix, "take over a live socket" on
Windows, and never "share this port", since the option that actually shares is `SO_REUSEPORT`, which .NET
exposes no property for and which is reachable only through a raw numeric option name that differs
between Linux and BSD. A consumer who sets one flag would have configured three different libraries.

A configure callback handed the underlying `Socket` was rejected outright: it hands an L2 consumer an L0
object, which is the thing ADR 0003 exists to prevent. With it they can set blocking mode, resize buffers
underneath the arena arithmetic, or close the socket while the Reactor still holds the Link.

**What this costs and what it buys, on the same machine.** Ephemeral binds are unaffected, which is the
normal case for a test: bind port zero, and the OS does not hand out a port that would conflict. The one
idiom lost is a test that pins a port, closes from the listening side, and reruns inside the platform's
`TIME_WAIT` window — 60 seconds on Linux, 120 on Windows by default. It is narrower than it looks, because
`TIME_WAIT` is held by whichever side performed the active close, and in this library the consumer decides
who closes: a harness that tears down the dialling side first leaves the listening port clean. UDP has no
`TIME_WAIT` at all.

Against that, exclusivity removes a flake class that is expensive to diagnose. On Linux a `SO_REUSEPORT`
pair *splits* arriving datagrams between the two sockets by hash, so a fixture that failed to dispose, or
a Reactor constructed in both a class fixture and a test, would lose half its traffic to a socket nobody
is pumping — with no error anywhere. Under exclusive binding the second bind fails at registration and
names the port.

## Raw registers; a Session declares

**A Raw bind is a runtime call returning a Link. A Session Reactor takes its listening endpoints at
construction and never binds again.**

Each profile gets the shape its own vocabulary implies. ADR 0007 split the identities: Raw has Links,
Sessions have Peers. A Raw bind registers a transport the consumer then addresses, so it must hand back a
Link. A Session Reactor's socket is protocol machinery the consumer never names, sends on, or closes —
there is no identity to return, and inventing one would put a Link into the profile ADR 0007 removed the
word from. ADR 0013 already agrees from the other direction: `LinkFailed` and `DestinationUnreachable`
sit in the Raw block, and no Session-block member has a Link subject.

The Session endpoints are a **collection, possibly empty**. Several binds is therefore the ordinary case
rather than an extension, which is what the family decision above requires. The alternative — one endpoint
per Reactor, two families meaning two Reactors — was rejected on ADR 0011's own arithmetic: the
preallocation is `capacity × (retention + receive buffer)` whether the Peers exist or not, so serving one
logical set of Peers over two families would pay the entire bill twice, and split them across two Peer id
spaces that cannot resume into each other. Empty means dial-only, and is meaningful rather than a hole.

Construction validates the set as it already validates the arena floor — endpoints distinct, each bound
exclusively — and a failure throws naming the endpoint, beside the allocation failure ADR 0011 already
puts there.

**There is no rebinding.** A Reactor cannot begin listening later, stop, or move; that is a new Reactor.
This removes every question about Peers established through an endpoint that is going away, and keeps
binding in the same phase as every other resource commitment the design makes.

**The dial-only datagram case.** The stream profile's dial needs no bind, since TCP binds implicitly on
connect. UDP does not work that way: there must be a bound socket for replies to arrive on. So a datagram
Reactor with no endpoints binds a wildcard, port-zero socket in the family of the destination the first
time it dials. This is derived rather than guessed — the family comes from the destination, and the port
comes from the OS because we have nothing to say about it — and it is visible through the accessor below.

## A Source carries its scope

**A `Source` is `{ family, 16 address bytes, scope id, port }`, and the scope id participates in
equality.**

An IPv6 link-local address is only meaningful with its zone index, because the same `fe80::/10` address
can legitimately exist on two interfaces of one host. Dropping the scope would make a link-local Source
unusable as a reply address, and would do something worse to the cookie: ADR 0009 keys the HMAC over the
source address and port, so two hosts reachable at the same link-local address on different interfaces
would be issued and validated against the *same* cookie — the exact confusion a cookie exists to prevent,
in the addressing mode where it is easiest to arrange. Carrying the scope but excluding it from equality
was rejected for the same reason: it buys stability against an inconsistently reported zone index by
making two different hosts equal.

The family is an **explicit tag** rather than inferred from the bytes. With mapped addresses gone there is
nothing in a 16-byte field that distinguishes a zero-padded IPv4 address from an IPv6 one.

This lands the struct at the roughly 56 bytes ADR 0013 budgeted, so nothing there is revised.

## Capacity counts share-holders, and ADR 0012's read cap is corrected

**Capacity counts the things the consumer names: Links in Raw, Peers in a Session.** One meaning, the one
already documented, and the same number that multiplies the preallocation.

ADR 0012 set the read cap as *"no Handle may consume more than `arena ÷ capacity` bytes in one Pump."*
That is right in Raw and in the stream profile, where a Handle and the subject of its bytes are the same
thing. **It is wrong in the datagram Session profile**, where ADR 0009's session id demultiplexes every
Peer off one bound socket — the property that lets a Peer resume from a different address. That socket is
the Reactor's only reading Handle, so the rule as written caps the whole Reactor at one sixty-fourth of
its arena per Pump at capacity 64, leaving 63/64ths of the memory the consumer paid for unreachable by
construction.

**The share belongs to the share-holder, not to the Handle**: a Peer in either Session profile, a Link in
Raw. The datagram socket is then bounded by the sum of the shares of the Peers whose packets it is
delivering, which is the arena. A listening Link holds no share, because accepting reads nothing into the
arena; a bound Session socket is not a share-holder either, it is where shares arrive through.

This is a **correction, not a supersession**. ADR 0012 states the intent two paragraphs above the defect —
*a fair share of the arena* — and fails to achieve it only in the profile where the socket is not the
subject. The escape clause is unchanged and still fires: a share-holder may always take one whole Message.

A Raw listener does consume a capacity slot, because it is a registered Link that `Select` scans and that
the consumer named. It simply never draws arena.

Counting Handles everywhere instead was rejected because it conflates the scanning budget with the
preallocation multiplier — the two things ADR 0011 separated when it made capacity required — and would
make the memory bill depend on how many addresses you listen on. A second knob for a Handle budget was
rejected because its value is arrived at by counting binds the constructor can already see.

**Amended (issue #427): one address may not hold every Slot.** Capacity and the admission cap bound
the total, but each fresh nonce from one address could still take a Slot. The datagram profile gains a
required `MaxPeersPerAddress`, on its own options because ADR 0012 keeps protocol knobs out of the shared
value. It is required under ADR 0011's rule: how many Peers legitimately share an address depends on the
NATs and proxies in front of the Reactor, and defaulting it to `Capacity` would leave the limit off for
anyone who never read about it. `Capacity` or above is the explicit way to set no limit.

- **The key is the address, not the `Source`.** `Source` equality includes the port, and a host can
  answer a Cookie from as many ports as it can bind. Family and scope stay in the key, for the
  reasons under *A Source carries its scope*. A host that can bind a whole IPv6 prefix still gets the
  limit once per address, and grouping by prefix is left open.
- **A Peer is charged to the address of its handshake until its Slot is released.** It does not move on
  a Resume, or one address could fill its limit, move those Peers to a second address and fill it again.
  The cost falls on other hosts behind the old address, such as a NAT, which have that much less room
  for as long as the moved Session lasts.
- **The refusal on the wire is ADR 0007's capacity refusal, unchanged.** The remote's next step is the
  same, no wire format changes, and it cannot use the reason to learn whether Slots are free.
- **A full Reactor is not a per-address refusal.** It reports `CapacityExhausted` as before, so a limit
  of `Capacity` never fires.

The stream profile has no such limit: this amendment covers the datagram profile only.

**Amended (issue #452): the stream profile takes the same limit.** One address could open connections
and complete the Hello exchange until it held every Slot. `StreamSessionOptions` gains
`MaxPeersPerAddress`, and the key, the charge and the wire refusal follow the datagram rules above. A Peer is charged to the address its connection was accepted
from, and a Resume on a new connection does not move it. Two points are specific to the stream profile:

- **A Resume never meets the limit.** The resume match runs before the capacity check (ADR 0011), and the
  limit runs after it, so a reconnecting Peer whose address is at the limit still resumes.
- **The refusal reuses `AddressPeerLimitReached` (208).** It is one condition on both profiles, so under
  ADR 0020 the stream profile shares the datagram member. A full Reactor still reports `PeerRefused`
  (300), so a limit of `Capacity` never fires. Neither admission counter moves, as for every stream
  refusal.

**Both profiles default the limit to `Capacity - 1`, at least 1.** This supersedes the #427 amendment's
decision to make the datagram knob required. ADR 0011 allows a default derived from another knob, and
`Capacity - 1` is derived from the failure itself: it is the largest limit under which one address can
never hold every Slot. Any lower default would be a guess about how many Peers share a NAT, which is the
guess ADR 0011 forbids. The #427 objection was to a default of `Capacity`, which leaves the limit off.
This one is on, but weak: it stops one address taking the last Slot, not one address taking all but one.

- **Existing code keeps compiling.** An options value that never sets the knob gets the default.
- **A Reactor whose Peers all share one address loses one Slot** until it sets the limit to `Capacity`.
  Nothing reports this except `AddressPeerLimitReached` when that last Slot is refused.
- **At a capacity of 1 the default is 1, which is no limit.** One Slot cannot be split between addresses.

**Amended (issue #453): IPv6 addresses are grouped by prefix, and IPv4 addresses are not.** Both
profiles gain `PeerLimitIPv6PrefixLength`, which sets how many leading bits of an IPv6 address form the
key for `MaxPeersPerAddress`. A host that can bind a whole prefix no longer gets the limit once per
address.

- **It is a knob, not a constant.** ADR 0012 makes a value a constant when the consumer has no
  information to choose it with, as for the cookie epoch. Here the consumer knows what the Reactor cannot
  see: whether its Peers share one LAN, or arrive from ISPs that delegate a /56 to each customer. The
  right length describes the consumer's network, so it is theirs to set.
- **It defaults to 64, from a citable standard.** RFC 4291 §2.5.1 fixes the interface identifier of
  unicast addresses at 64 bits, so a /64 is one link. With no NAT in IPv6, the devices behind one router
  share one /64, which is how they share one address behind an IPv4 NAT. The default therefore gives an
  IPv6 deployment the limit an IPv4 one already has.
- **It accepts 1 to 128, and 128 is per address.** 128 restores the counting this ADR had before #453.
  Construction rejects any other value, naming the knob.
- **Family and scope stay in the key.** A link-local Peer is grouped with others on the same interface
  only, for the reasons under *A Source carries its scope*. IPv4 addresses are never grouped, for the
  reasons below.
- **Only the limit groups.** The charge, the wire refusal and the rule that a Resume does not move a
  Peer apply to the prefix as they did to the address. A Resume still compares the full address to decide
  whether a path challenge is needed (ADR 0021).
- **`AddressPeerLimitReached` keeps its subject.** The record names the refused `Source` and its limit
  as before, and carries the prefix length its address was counted by in its second value, which was
  unused: 32 for IPv4, and the configured length for IPv6. A reader can then tell a refusal of one
  address from a refusal of its prefix. No `DiagnosticId` member is added.

The cost falls on IPv6 Peers that legitimately share a /64. Every on-link host on an IPv6 LAN is in one
/64, so at the default those Peers share one limit. A consumer whose Peers are on-link sets the length
to 128. The default also narrows the bypass without closing it. RFC 6177 recommends a /48 to /56 for each
end site, so one customer can hold 256 to 65 536 /64s. A shorter length closes more of that, at the cost
of grouping unrelated customers. IPv6 is grouped and IPv4 is not because a /64 comes free with one host,
while each IPv4 address costs its holder address space.

The limit keys on the full IPv4 address. Grouping was considered for a host that holds several addresses
in one subnet, and declined for four reasons:

- **The cheap bypass cannot be told apart from honest Peers.** Holding many public IPv4 addresses costs
  the holder address space, so the per-address limit already makes that bypass expensive. The cheap case
  is a host on the Reactor's own subnet that claims unused addresses through ARP replies or extra DHCP
  leases. Those addresses share a subnet with every honest on-link Peer, so a subnet limit would refuse
  the honest Peers with the attacker.
- **A group would merge unrelated hosts.** Carrier-grade and cloud NAT pools send many users from
  addresses in one block, and one user can appear from several of them. Containers and virtual machines
  on one host each take an address on a shared bridge subnet. One limit across the block puts all of
  them behind it.
- **The prefix length would be a guess.** The Reactor cannot see a remote network's mask. A fixed length
  is the guess ADR 0011 forbids, and a knob owes a derived default that does not exist. A local
  interface's mask is known only for on-link Peers, and those are the Peers the first point says must not
  be grouped.
- **Broadcast and multicast do not change the answer.** They are destinations, and the limit counts the
  sources of completed handshakes, which are unicast. A broadcast or multicast send reaches Peers on the
  local subnet, so subnet grouping would cap the audience that kind of send exists to reach.

The cost is that a host on the Reactor's subnet that can claim N unused addresses can hold
N × `MaxPeersPerAddress` Slots, bounded by `Capacity`.

## Reading back what was bound

Exclusive binding makes port zero the normal way to bind in a test, so the assigned address must be
readable. **Raw gets `LocalOf(Link)`**, beside ADR 0008's `RemoteOf(Link)` and for the same reason: it is
Link state, constant for the Link's lifetime, and therefore not event data. **A Session Reactor exposes a
snapshot of its bound endpoints** in the order supplied, with any socket the datagram profile opened on a
first dial appended.

**Both return `IPEndPoint`, not `Source`.** A Source is defined as what a datagram *claims* — unauthenticated
by construction, a hint about origin and never an identity. Our own bound address is the one address in
the library that is a fact, and returning it as a `Source` would put the library's own socket into the type
whose entire warning is that it must not be trusted.

## Consequences

- **ADR 0012's per-Handle read cap is corrected to per-share-holder.** Same rule, right denominator. Its
  observation that the default arena yields "one Message per Handle per Pump" now reads *per share-holder*,
  and the datagram profile — where it was never true — is the reason.
- **The construction surfaces gain their last member.** A Session Reactor's options carry a possibly-empty
  set of endpoints; Raw's do not, because Raw binds at runtime. ADR 0012's claim that every default has a
  stated provenance survives: the set has no default, and the dial-only socket's family and port are both
  derived.
- **ADR 0008's multiple-Listeners gap is closed entirely.** Identity was settled there; binding semantics,
  IPv6 and multiple binds to one port are settled here.
- **`Source` is now fully specified** — family tag, 16 bytes, scope, port — which is what ADR 0013 said this
  gap owed it. No mapped form can ever appear in one, so `Source` equality is total and the cookie input is
  exactly what the socket reported.
- **The address vocabulary is two types on purpose.** `IPEndPoint` on cold paths, `Source` on the record
  path, one conversion, and a documented reason so that the next surface added does not pick by taste.
- **No `DiagnosticId` member is added.** A bind failure is a construction throw, which is precisely the
  signal ADR 0013 predicted for anything proposed as `Error`: `Error` stays empty.
- **A test suite binds port zero.** Pinned ports still work, but the library has no reuse escape, so the
  one documented cost is a pinned-port rerun inside `TIME_WAIT` after closing from the listening side.
- **No design gaps remain open.** Every deferral made since ADR 0001 has now been settled, and what is left
  before 1.0 is the rename ADR 0006 requires and the first line of L0. `ISessionReactor` remains recorded
  as post-1.0, with ADR 0011's note that the options-genericity question returns with it.
