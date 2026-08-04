# Raw and Session are separate Reactors, and Raw has Links not Peers

Raw exists as an escape hatch. It is there so a consumer can implement their own protocol layer over a
bare transport, and it therefore defines none of the Session protocol — no framing, no delivery modes,
no liveness, no Resume. A Reactor is Raw or Session for its entire lifetime; the two never coexist in
one instance. A consumer who wants both constructs two Reactors and pumps them separately.

Because the profiles never mix, the profile is fixed by the type rather than by a constructor flag.
The library exposes **`SessionReactor`** and **`RawReactor`** as two public faces over one internal
engine. Session-only operations — delivery modes, Goodbye, Linger, Resume — exist only on the first;
Links, Sources and Segments only on the second. Nothing unreachable is callable, and the failure that
would otherwise be a runtime throw on a call the compiler accepted does not exist.

**`Peer` becomes a Session-only word.** This is the argument that decided the split, and it is not
about unreachable enum members. A consumer reaching for Raw is about to write a protocol layer with
its own notion of a remote party — its own identity, its own reconnection, its own liveness. Handing
them a `PeerId` squats on the exact word that layer needs, at the exact seam we are offering them, in
the one place we have said we define nothing.

A `RawReactor` carries both Raw TCP and Raw UDP. The principle that separated Raw from Session is not
applied a second time here: a protocol layer using TCP for control and UDP for bulk is an ordinary
shape, and splitting it would cost that consumer a shared clock for no gain the type system could
express.

**Raw identity is a `Link`.** One `LinkId` covers both a TCP connection and a bound UDP socket, and it
is index-plus-generation like `PeerId` — generation is stale-handle safety on slot reuse, which is the
one part of `PeerId`'s design that was never about Sessions. Every raw event carries a valid `LinkId`;
datagram events additionally carry a `Source`. A Link says *which registered thing this concerns*; a
Source says *who claims to have sent this datagram*. They are not rival identities occupying one slot,
so no event field is conditionally meaningless except the one that genuinely applies to datagrams
alone.

`Socket` was the more honest word and was rejected: `ReactorSockets.Socket` beside
`System.Net.Sockets.Socket` is an ambiguity in precisely the files most likely to import both, since
an escape-hatch consumer is the one person guaranteed to be using the BCL socket types directly.
`Endpoint` collides with `IPEndPoint`, `Channel` with `System.Threading.Channels`, `Transport` with the
L1 folder, `Port` with the number. `Link` sits on Session's avoid-list only because a Session is more
than a link, which leaves it free for the thing that is nothing more than one.

**A Pump accepts; the consumer may close.** Inbound connections are accepted by the Pump that finds
them ready, and arrive as an event with the identity already created. This is one code path for both
profiles, and it is what a Session must do regardless in order to run its handshake.

At capacity the two profiles diverge, because one of them owns a protocol and the other does not. A
`RawReactor` stops accepting and leaves connections in the OS backlog to be picked up by a later Pump;
we invent nothing on the wire there, so we say nothing. A `SessionReactor` refuses a validated
handshake in-protocol, with a reason, so the remote can back off rather than retry blindly at the
moment we are already saturated. The stateless cookie has already confirmed the sender's address by
that point, so the amplification exposure is bounded.

## Consequences

- **ADR 0006's "the primary type stays `Reactor`" is superseded.** No type is named `Reactor`, so that
  paragraph's argument — that `ReactorSockets.Reactor` resolves cleanly because the namespace segment
  and the type name differ — no longer applies to anything. The package name `ReactorSockets` is
  unaffected; nothing about it depended on the type.
- **`Reactor` names a role and no longer names a type.** CONTEXT.md pins it to the dispatcher instance
  in either profile, with `SessionReactor` and `RawReactor` as the two things that fill it. This is
  the third revision of that entry, and it is now the one revision that cannot be invalidated by a
  later type rename.
- **Two public entry points to document, test and keep stable**, against ADR 0003's rule that every
  public type is permanent. The internal cost is small — readiness dispatch, the frame arena, the
  event model and Drain all live in L0/L1 and are shared — but the documentation and test surface for
  construction, registration and teardown is genuinely doubled.
- **A consumer building on Raw gets no help.** L0/L1 are internal and will stay internal, so the
  length-prefixing, sequencing, retransmission and heartbeat work we have already designed is
  unavailable to them. Anyone wanting "reliability over UDP with my own handshake" is writing it
  themselves. This is the accepted meaning of an escape hatch rather than a toolkit.
- **The Session refusal adds to the wire format** — a rejection message and a `DisconnectReason` for
  it. Both land in the still-open wire-format gap rather than being settled here.
- **The accept cap is a construction-time knob, not a per-frame choice.** Because a Pump accepts
  unasked, the only place to express admission policy is at construction, and the OS backlog length
  becomes load-bearing behaviour we must document and cannot fully control.
- The roster of raw event kinds is not settled here, and whether **Listener** becomes a glossary term
  belongs with the open multiple-Listeners gap.
