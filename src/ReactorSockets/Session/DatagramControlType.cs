namespace ReactorSockets.Session;

/// <summary>
/// The pre-Session control datagram roster (ADR 0009): the types carried by a datagram whose session
/// id is <see cref="DatagramSessionProtocol.ControlSession"/>. Explicit values because these are wire
/// values a remote sends us, not an in-process enumeration — a renumbering here is a protocol change.
/// </summary>
/// <remarks>
/// In-Session control — acknowledgements, Ping, Goodbye — is deliberately not here. Those are
/// ordinary packet records inside a Session's data header, told apart by a bit in the record's flags,
/// so they ride the Session's own sequencing and acknowledgement machinery. Only the exchanges that
/// happen before a Session exists, and therefore have nowhere else to live, are control datagrams.
/// </remarks>
internal enum DatagramControlType : byte
{
    /// <summary>A dialler's opening bid, carrying a version and a nonce and nothing worth allocating for.</summary>
    Hello = 1,

    /// <summary>The answer to a <see cref="Hello"/>: a stateless authenticator the dialler must echo back.</summary>
    Cookie = 2,

    /// <summary>A <see cref="Hello"/> repeated with the <see cref="Cookie"/> echoed, plus everything a Session needs to exist.</summary>
    HelloWithCookie = 3,

    /// <summary>The accepting end's agreement, carrying the session id it wants stamped on datagrams sent to it.</summary>
    Accept = 4,

    /// <summary>A refusal of a cookie-validated <see cref="HelloWithCookie"/>, naming which of the two reasons applies.</summary>
    Refusal = 5,

    /// <summary>
    /// A request to re-establish a previous Session from a new address, proving entitlement with the
    /// secret that Session established. Defined here because the roster is the wire format's, not this
    /// slice's; handled in the slice that implements Resume.
    /// </summary>
    Resume = 6,
}
