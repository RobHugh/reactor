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
            nameof(ReadinessDispatcher));
    }
}
