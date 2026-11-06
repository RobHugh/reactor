# A Resume from a new IP address is proven by a path challenge

ADR 0009 authorises a Resume by a secret exchanged during the handshake, and the accepting end then
sends to whatever address the Resume came from. Issue #426 found the gap: the secret proves who sent the
Resume, not that its source address will receive.

That matters more than "trusted peer" suggests. Any host that completes the cookie handshake with a
listening Reactor holds a resume secret. It can send one Resume with a forged source address naming a
victim, then keep the Session alive and acknowledge from its own address, because data datagrams are
accepted from any source. Every datagram the Reactor sends that Peer then goes to the victim. That is
the reflection the cookie and "Refusal answers only a cookie-validated Hello" exist to close.

Resume still has to survive a dialler whose address changes. A NAT rebinding usually changes only the
port. A network change, an address-family change, or a carrier-grade NAT that rebinds into another pool
address changes the IP too.

## Decision

**A matched Resume from the IP address the Peer is already sent to moves the Peer at once, whatever the
port.** Address, family and scope id must match. This is the common rebinding case, and it costs nothing
extra. A forged Resume on this path can only name a port on an address the handshake already proved
receives, which is the attacker's own.

**A matched Resume from any other IP address moves nothing until that address answers a challenge.**
The accepting end sends a `PathChallenge` control datagram to the Resume's source, from the socket the
Resume arrived on, carrying a 16-byte random nonce. The dialling end echoes it in a `PathResponse` sent
to the address it already sends its Peer to, from the socket it already sends from. The accepting end
moves the Peer only when the echo arrives from exactly the challenged source, on exactly the challenging
socket, with the nonce matching in constant time. Until then the Peer keeps being sent to its old address.

The two new control types are 7 and 8 on the pre-Session roster. Both bodies are the receiver's own
session id (4 bytes, which routes) then the nonce (16 bytes). Like every control datagram they are 256
bytes, so a challenge answers a Resume one-for-one and the exchange cannot amplify.

**The nonce repeats while the challenged source stays the same.** The dialler resends Resume every
send-idle interval. A fresh nonce each time would void any answer still in flight when the round trip
exceeds that interval. A Resume from a different source replaces the pending challenge with a fresh
nonce. Any move of the send address clears a pending challenge, so a late answer cannot move the Peer
a second time.

**The response goes to the dialler's Peer address, never to the challenge's source.** A forged challenge
therefore cannot aim the dialler's traffic anywhere but the Peer it already talks to.

The stream profile needs none of this. A stream Resume arrives on a new TCP connection, and TCP's own
handshake has already proved that address receives.

## Alternatives rejected

- **Accept trusted-peer redirection in the threat model.** The "trusted peer" is anyone who can complete
  a handshake, so this would accept an open reflector.
- **Pin the IP address and refuse a Resume from any other.** This closes the reflection with no wire
  change, but drops network change, address-family change and pool rebinding, all of which the Resume
  design names as cases it exists for.
- **Challenge every Resume, including a port-only change.** This costs a round trip on the common case and
  proves nothing the same-IP rule does not already bound.

## Cost

- Two new wire types and a pending-challenge field per Peer.
- Resuming after a change of IP address takes one extra round trip.
- The challenge itself goes to an unproven address. A forger can still make the Reactor send one 256-byte
  datagram to a victim per Resume it sends, which is the same one-for-one exposure the Cookie already
  accepts for a Hello.
- On-path attackers remain out of scope. One who sees the Accept holds the secret, and one who sees the
  challenge can answer it.
