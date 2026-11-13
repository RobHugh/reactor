# <a id="ReactorSockets_RawReactor__ctor_ReactorSockets_ReactorOptions_System_Nullable_System_Int32__"></a> Constructor RawReactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor__ctor_ReactorSockets_ReactorOptions_System_Nullable_System_Int32__"></a> RawReactor\(ReactorOptions, int?\)

Constructs a Reactor bound to the calling thread (ADR 0002) — every future mutating call must
come from this same thread or it throws.

```csharp
public RawReactor(ReactorOptions options, int? scratchBufferSize = null)
```

### Parameters

`options` [ReactorOptions](ReactorSockets.ReactorOptions.md)

The shared options value. Every knob on it is validated here, and
[`ReactorOptions.ArenaSize`](ReactorSockets.ReactorOptions.ArenaSize.md#ReactorSockets_ReactorOptions_ArenaSize) is resolved against this profile's floor; a
combination that cannot work throws at construction and nowhere else.

`scratchBufferSize` [int](https://learn.microsoft.com/dotnet/api/system.int32)?

The single scratch buffer every Link's receive lands in, reused across Links since only one is
read at any instant inside a single-threaded Pump (ADR 0012). Defaults to
[`RawReactor.MaxUdpDatagramSize`](ReactorSockets.RawReactor.MaxUdpDatagramSize.md). Provenance: the largest payload a UDP datagram can carry over
IPv4, at which an IPv4 datagram cannot be truncated. An IPv6 datagram of 65 508 to 65 527 bytes
exceeds it and is reported as truncated. This is the profile-specific
knob a consumer lowers to opt into truncation, kept off [`ReactorOptions`](ReactorSockets.ReactorOptions.md) because it
has no meaning outside Raw's datagram surface. It must be at least 1 and at most
[`MaxLength`](https://learn.microsoft.com/dotnet/api/system.array.maxlength) minus 1, because the scratch allocation adds one truncation byte;
a value outside that range throws [`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception).

### Remarks

<b>The memory bill</b> is one scratch buffer plus one arena — <code>scratchBufferSize + 1 + ArenaSize</code>
(the extra byte is how a truncated datagram is recognised) — neither multiplied by capacity,
because Raw holds nothing for a Link across a Pump. That is what "invents nothing on the wire"
looks like from the constructor (ADR 0012).

