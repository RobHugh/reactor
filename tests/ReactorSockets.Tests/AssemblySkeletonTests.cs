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
    public void ReactorSocketsAssembly_PublicTypes_IsEmpty()
    {
        // Arrange
        Assembly assembly = Assembly.Load(new AssemblyName("ReactorSockets"));

        // Act
        Type[] publicTypes = assembly.GetExportedTypes();

        // Assert
        publicTypes.Should().BeEmpty();
    }
}
