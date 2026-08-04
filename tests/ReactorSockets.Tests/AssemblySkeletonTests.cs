using System.Reflection;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class AssemblySkeletonTests
{
    [Fact]
    public void ReactorSocketsAssembly_Loaded_HasExpectedIdentity()
    {
        // Arrange
        Assembly assembly = Assembly.Load(new AssemblyName("ReactorSockets"));

        // Act
        string? assemblyName = assembly.GetName().Name;

        // Assert
        assemblyName.Should().Be("ReactorSockets");
    }

    [Fact]
    public void ReactorSocketsAssembly_PublicTypes_ContainsOnlyTheSharedOptionsAndDiagnosticsSurface()
    {
        // Arrange
        Assembly assembly = Assembly.Load(new AssemblyName("ReactorSockets"));

        // Act
        Type[] publicTypes = assembly.GetExportedTypes();

        // Assert
        publicTypes.Select(type => type.Name).Should().BeEquivalentTo(
            nameof(ReactorOptions),
            nameof(IReactorDiagnostics),
            nameof(DiagnosticRecord),
            nameof(DiagnosticLevel),
            nameof(DiagnosticId),
            nameof(DiagnosticSubjectKind),
            nameof(DiagnosticGateway),
            nameof(ReactorCounters),
            nameof(ReactorCounterSet),
            nameof(LinkCounters),
            nameof(LinkCounterSet),
            nameof(LinkId),
            nameof(Source),
            nameof(SourceFamily),
            nameof(FrameArena),
            nameof(ArenaReservation),
            nameof(ArenaReservationOutcome),
            nameof(ReadinessDispatcher),
            nameof(RawReactor),
            nameof(RawEvent),
            nameof(RawEventKind),
            nameof(RawEventEnumerator),
            nameof(CloseReason),
            nameof(Result));
    }

    [Fact]
    public void ReactorSocketsAssembly_PublicMembers_NoneAcceptAStringHostOrAddress()
    {
        // Arrange
        Assembly assembly = Assembly.Load(new AssemblyName("ReactorSockets"));
        Type[] publicTypes = assembly.GetExportedTypes();
        string[] addressLikeNames = ["host", "hostname", "address", "endpoint"];

        // Act
        IEnumerable<string> offendingMembers =
            from type in publicTypes
            from method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            from parameter in method.GetParameters()
            where parameter.ParameterType == typeof(string)
            where addressLikeNames.Any(name => parameter.Name!.Contains(name, StringComparison.OrdinalIgnoreCase))
            select $"{type.Name}.{method.Name}({parameter.Name})";

        // Assert
        offendingMembers.Should().BeEmpty("the library never resolves a hostname — every address surface takes IPEndPoint or Source, never a string");
    }
}
