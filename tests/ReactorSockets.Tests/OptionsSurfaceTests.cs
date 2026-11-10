using System.Reflection;
using System.Runtime.CompilerServices;
using System.Xml.Linq;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The default-provenance audit ADR 0012 asked for at 1.0. A knob may carry a default only
/// when that default is derived — from machinery the library owns, from another knob, or from a
/// citable standard — and otherwise it is required. A constant is not configurable at all. These tests
/// walk the public surface mechanically, so the claim stays true as knobs are added rather than being
/// re-audited by hand.
/// </summary>
public class OptionsSurfaceTests
{
    /// <summary>
    /// The sentence opener a defaulted knob's XML doc uses to state where its default comes from. The
    /// convention exists so this test can find it; <c>docs/options.md</c> lists every one.
    /// </summary>
    private const string ProvenanceMarker = "Provenance:";

    private static Assembly ReactorSocketsAssembly => typeof(ReactorOptions).Assembly;

    /// <summary>
    /// Every value a consumer can set when constructing a Reactor, by property or constructor parameter.
    /// This is the audited list: a new entry is a new knob, and must be required or state its provenance.
    /// </summary>
    private static readonly string[] _auditedKnobs =
    [
        "ReactorOptions.Capacity",
        "ReactorOptions.AdmissionCap",
        "ReactorOptions.ArenaSize",
        "ReactorOptions.TimeProvider",
        "ReactorOptions.Sink",
        "ReactorOptions.MinimumLevel",
        "DatagramSessionOptions.Reactor",
        "DatagramSessionOptions.MaxMessageSize",
        "DatagramSessionOptions.RetentionBytes",
        "DatagramSessionOptions.MaxPeersPerAddress",
        "DatagramSessionOptions.MaxDatagramsPerPump",
        "DatagramSessionOptions.SendIdleInterval",
        "DatagramSessionOptions.Linger",
        "DatagramSessionOptions.Endpoints",
        "DatagramSessionOptions.FragmentSize",
        "DatagramSessionOptions.ReceiveWindowFragments",
        "DatagramSessionOptions.MissedHeartbeats",
        "DatagramSessionOptions.Entropy",
        "StreamSessionOptions.Reactor",
        "StreamSessionOptions.MaxMessageSize",
        "StreamSessionOptions.RetentionBytes",
        "StreamSessionOptions.MaxPeersPerAddress",
        "StreamSessionOptions.SendIdleInterval",
        "StreamSessionOptions.Linger",
        "StreamSessionOptions.Endpoints",
        "StreamSessionOptions.Entropy",
        "StreamSessionOptions.ReceiveBufferSize",
        "StreamSessionOptions.SendBufferSize",
        "StreamSessionOptions.MissedHeartbeats",
        "RawReactor(options)",
        "RawReactor(scratchBufferSize)",
        "DatagramSessionReactor(options)",
        "StreamSessionReactor(options)",
    ];

    [Fact]
    public void PublicSurface_EverySettableValue_IsAnAuditedKnob()
    {
        // Act
        IEnumerable<string> settable = SettableProperties().Select(Describe)
            .Concat(ConstructorParameters().Select(Describe));

        // Assert
        settable.Should().BeEquivalentTo(_auditedKnobs);
    }

    [Fact]
    public void PublicSurface_EveryDefaultedKnob_StatesItsProvenanceInItsXmlDoc()
    {
        // Arrange
        Dictionary<string, XElement> docs = LoadXmlDocs();

        // Act
        IEnumerable<string> undocumented = SettableProperties()
            .Where(property => !property.IsDefined(typeof(RequiredMemberAttribute)))
            .Where(property => !DocOf(docs, property).Contains(ProvenanceMarker, StringComparison.Ordinal))
            .Select(Describe)
            .Concat(ConstructorParameters()
                .Where(parameter => parameter.IsOptional)
                .Where(parameter => !DocOf(docs, parameter).Contains(ProvenanceMarker, StringComparison.Ordinal))
                .Select(Describe));

        // Assert
        undocumented.Should().BeEmpty("a knob without a derivation is required, and one with a derivation says so (ADR 0012)");
    }

    [Theory]
    [InlineData("Ack")]
    [InlineData("Duplicate")]
    [InlineData("Space")]
    [InlineData("SessionId")]
    [InlineData("Header")]
    [InlineData("Cookie")]
    [InlineData("Epoch")]
    [InlineData("Deadline")]
    [InlineData("Redial")]
    [InlineData("ReadCap")]
    [InlineData("ReadShare")]
    [InlineData("Rto")]
    [InlineData("Retransmission")]
    [InlineData("Family")]
    [InlineData("Port")]
    public void PublicSurface_SettableValues_ExposeNoConstantOrDerivedValue(string name)
    {
        // Arrange — the protocol constants (the 32-bit ack width, the 64-entry duplicate window, the four
        // sequence Spaces, zero as the reserved session value, the 20-byte header, the 30-second cookie
        // epoch) and the derived values (the receive-idle deadline, the re-dial schedule, the
        // per-share-holder read cap, the RTO floor and ceiling, the dial-only socket's family and port).

        // Act
        IEnumerable<string> settable = SettableProperties().Select(Describe)
            .Concat(ConstructorParameters().Select(Describe));

        // Assert
        settable.Should().NotContain(
            knob => knob.Contains(name, StringComparison.OrdinalIgnoreCase),
            "a number both ends must agree on is a constant by construction, and a derived value is not a knob (ADR 0012)");
    }

    /// <summary>
    /// The Reactors a consumer constructs: every exported type whose public constructor takes an
    /// options type. Their constructor parameters are where the options surface starts.
    /// </summary>
    private static IEnumerable<Type> Reactors() =>
        ReactorSocketsAssembly.GetExportedTypes()
            .Where(type => type.GetConstructors().Any(constructor =>
                constructor.GetParameters().Any(parameter => IsOptionsType(parameter.ParameterType))));

    /// <summary>
    /// Every settable property of every options type reachable from a Reactor's constructor, following
    /// options types composed into others (<see cref="DatagramSessionOptions.Reactor"/>). Counters,
    /// identities and events have <c>init</c> members too, but they are values a Reactor hands out, not
    /// configuration it takes, so they are not reached.
    /// </summary>
    private static IEnumerable<PropertyInfo> SettableProperties()
    {
        HashSet<Type> visited = [];
        Queue<Type> pending = new(ConstructorParameters()
            .Select(parameter => parameter.ParameterType)
            .Where(IsOptionsType));

        while (pending.TryDequeue(out Type? optionsType))
        {
            if (!visited.Add(optionsType))
            {
                continue;
            }

            foreach (PropertyInfo property in optionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
            {
                if (property.SetMethod is not { IsPublic: true })
                {
                    continue;
                }

                if (IsOptionsType(property.PropertyType))
                {
                    pending.Enqueue(property.PropertyType);
                }

                yield return property;
            }
        }
    }

    private static IEnumerable<ParameterInfo> ConstructorParameters() =>
        from type in Reactors()
        from constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
        from parameter in constructor.GetParameters()
        select parameter;

    private static bool IsOptionsType(Type type) =>
        type.Assembly == ReactorSocketsAssembly && type.IsClass && type.Name.EndsWith("Options", StringComparison.Ordinal);

    private static string Describe(PropertyInfo property) => $"{property.DeclaringType!.Name}.{property.Name}";

    // `!` works around a framework limitation: Member.DeclaringType and ParameterInfo.Name are declared
    // nullable for metadata shapes a public constructor compiled from C# source never has.
    private static string Describe(ParameterInfo parameter) => $"{parameter.Member.DeclaringType!.Name}({parameter.Name!})";

    /// <summary>The generated XML documentation file, keyed by doc-comment id.</summary>
    private static Dictionary<string, XElement> LoadXmlDocs()
    {
        string path = Path.ChangeExtension(ReactorSocketsAssembly.Location, ".xml");
        return XDocument.Load(path).Descendants("member")
            .ToDictionary(member => (string)member.Attribute("name")!, member => member);
    }

    private static string DocOf(Dictionary<string, XElement> docs, PropertyInfo property) =>
        docs.TryGetValue($"P:{property.DeclaringType!.FullName}.{property.Name}", out XElement? member)
            ? member.Value
            : string.Empty;

    private static string DocOf(Dictionary<string, XElement> docs, ParameterInfo parameter) =>
        docs.Where(pair => pair.Key.StartsWith($"M:{parameter.Member.DeclaringType!.FullName}.#ctor", StringComparison.Ordinal))
            .SelectMany(pair => pair.Value.Elements("param"))
            .Where(param => (string?)param.Attribute("name") == parameter.Name)
            .Select(param => param.Value)
            .FirstOrDefault() ?? string.Empty;
}
