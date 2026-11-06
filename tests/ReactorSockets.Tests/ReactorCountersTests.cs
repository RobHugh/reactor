using System.Collections;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The shape of the Counter surface, asserted over the public assembly rather than any one
/// profile. Covers <see cref="ReactorCounters"/> and its two per-subject siblings,
/// <see cref="PeerCounters"/> and <see cref="LinkCounters"/>, because the three rules here — no reset,
/// no enumeration of subjects, and a non-derivability line per field — are one decision (ADR 0013).
/// </summary>
public class ReactorCountersTests
{
    private static readonly Type[] _snapshotTypes = [typeof(ReactorCounters), typeof(PeerCounters), typeof(LinkCounters)];

    private static readonly Type[] _subjectTypes = [typeof(PeerId), typeof(LinkId), typeof(PeerCounters), typeof(LinkCounters)];

    public static TheoryData<Type> SnapshotTypes => [.. _snapshotTypes];

    [Fact]
    public void PublicSurface_EveryExportedType_HasNoResetMethod()
    {
        // Arrange — a reset is a write to observation state that another reader's baseline depends on.
        IEnumerable<MethodInfo> methods = PublicMethods();

        // Act
        IEnumerable<string> resets = methods
            .Where(method => method.Name.Contains("Reset", StringComparison.OrdinalIgnoreCase))
            .Select(method => $"{method.DeclaringType?.Name}.{method.Name}");

        // Assert
        resets.Should().BeEmpty();
    }

    [Fact]
    public void PublicSurface_NoMember_ReturnsACollectionOfSubjectsOrTheirCounters()
    {
        // Arrange — the consumer already holds the identities; a roster walk invites sampling them all.
        IEnumerable<(string Member, Type ReturnType)> returns = PublicMethods()
            .Select(method => ($"{method.DeclaringType?.Name}.{method.Name}", method.ReturnType));

        // Act
        IEnumerable<string> enumerators = returns
            .Where(entry => IsCollection(entry.ReturnType) && ElementTypes(entry.ReturnType).Any(_subjectTypes.Contains))
            .Select(entry => entry.Member);

        // Assert
        enumerators.Should().BeEmpty();
    }

    [Fact]
    public void PublicSurface_EveryPerSubjectCounterAccessor_IsKeyedByOneSubject()
    {
        // Arrange
        Dictionary<Type, Type> keyFor = new()
        {
            [typeof(PeerCounters)] = typeof(PeerId),
            [typeof(LinkCounters)] = typeof(LinkId),
        };
        MethodInfo[] accessors = [.. PublicMethods().Where(method => keyFor.ContainsKey(method.ReturnType) && !method.IsSpecialName)];

        // Act
        IEnumerable<string> unkeyed = accessors
            .Where(method => method.GetParameters() is not [ParameterInfo only] || only.ParameterType != keyFor[method.ReturnType])
            .Select(method => $"{method.DeclaringType?.Name}.{method.Name}");

        // Assert
        accessors.Should().NotBeEmpty("CountersOf exists on every profile");
        unkeyed.Should().BeEmpty();
    }

    [Theory]
    [MemberData(nameof(SnapshotTypes))]
    public void Documentation_EveryCounterField_SaysWhatItAloneTellsTheConsumer(Type snapshotType)
    {
        // Arrange — a field that cannot be given this line should not exist (ADR 0013).
        XElement member = LoadDocumentation().Descendants("member")
            .Single(element => element.Attribute("name")?.Value == $"T:{snapshotType.FullName}");
        Dictionary<string, string> paramDocs = member.Elements("param")
            .ToDictionary(param => param.Attribute("name")!.Value, param => Regex.Replace(param.Value, @"\s+", " ")); // every <param> carries a name

        // Act
        IEnumerable<string> undocumented = snapshotType
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(field => field.Name)
            .Where(name => !paramDocs.TryGetValue(name, out string? doc) || !doc.Contains("Not derivable:", StringComparison.Ordinal));

        // Assert
        undocumented.Should().BeEmpty();
    }

    [Theory]
    [MemberData(nameof(SnapshotTypes))]
    public void Shape_EveryCounterField_IsAMonotonicUlong(Type snapshotType)
    {
        // Arrange
        PropertyInfo[] fields = snapshotType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // Act
        IEnumerable<string> notUlong = fields.Where(field => field.PropertyType != typeof(ulong)).Select(field => field.Name);

        // Assert
        fields.Should().NotBeEmpty();
        notUlong.Should().BeEmpty();
    }

    private static IEnumerable<MethodInfo> PublicMethods() =>
        typeof(ReactorCounters).Assembly.GetExportedTypes()
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly));

    private static bool IsCollection(Type type) =>
        type != typeof(string)
        && (type.IsArray
            || typeof(IEnumerable).IsAssignableFrom(type)
            || type.GetMethod(nameof(IEnumerable.GetEnumerator)) is not null
            || (type.IsGenericType && type.GetGenericTypeDefinition() is var definition
                && (definition == typeof(Span<>) || definition == typeof(ReadOnlySpan<>)
                    || definition == typeof(Memory<>) || definition == typeof(ReadOnlyMemory<>))));

    /// <summary>Every type nested inside <paramref name="type"/>'s element or generic arguments, so a pair or tuple of subjects is caught too.</summary>
    private static IEnumerable<Type> ElementTypes(Type type)
    {
        Type[] direct = type.IsArray ? [type.GetElementType()!] : type.GetGenericArguments(); // an array always has an element type
        return direct.Concat(direct.SelectMany(ElementTypes));
    }

    private static XDocument LoadDocumentation()
    {
        string assemblyPath = typeof(ReactorCounters).Assembly.Location;
        return XDocument.Load(Path.ChangeExtension(assemblyPath, ".xml"));
    }
}
