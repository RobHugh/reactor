# <a id="ReactorSockets_StreamSessionOptions_Entropy"></a> Property Entropy

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_Entropy"></a> Entropy

The entropy the resume secret is drawn from. A construction knob in ADR 0011's sense — the
consumer's own resource, and a consumer with a hardware source or a policy about which
generator may be used is entitled to supply it. It carries a default only because that default
is derived rather than guessed. Provenance: the platform's own CSPRNG.

```csharp
public RandomNumberGenerator Entropy { get; init; }
```

### Property Value

 [RandomNumberGenerator](https://learn.microsoft.com/dotnet/api/system.security.cryptography.randomnumbergenerator)

### Remarks

Nothing disposes this, and nothing should. A supplied generator belongs to the consumer, who
may hand the same one to several Reactors; a Reactor that disposed it would be disposing a
resource it does not own. The default instance is not a leak either — on .NET the generator
[`Create`](https://learn.microsoft.com/dotnet/api/system.security.cryptography.randomnumbergenerator.create) returns holds no handle, drawing from the OS on
each call, so its <code>Dispose</code> has nothing to release.

