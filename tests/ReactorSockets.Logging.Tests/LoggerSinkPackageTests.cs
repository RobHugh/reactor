using System.Reflection;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Logging.Tests;

/// <summary>
/// The packaging decisions ADR 0005 and ADR 0006 rest on, checked against the build rather than
/// remembered: the core takes no logging dependency, and the adapter versions with the core.
/// </summary>
public class LoggerSinkPackageTests
{
    private static Assembly CoreAssembly => typeof(IReactorDiagnostics).Assembly;

    private static Assembly AdapterAssembly => typeof(LoggerSink).Assembly;

    [Fact]
    public void CoreAssembly_ReferencedAssemblies_IncludeNoLoggingAssembly()
    {
        // Arrange — this is the test that protects the whole decision to ship the adapter apart: the
        // moment the core references a logging assembly, every consumer inherits it (ADR 0005).
        Assembly core = CoreAssembly;

        // Act
        IEnumerable<string?> loggingReferences = core.GetReferencedAssemblies()
            .Select(reference => reference.Name)
            .Where(name => name is not null && name.Contains("Logging", StringComparison.OrdinalIgnoreCase));

        // Assert
        loggingReferences.Should().BeEmpty("the core keeps its zero-dependency claim; ILogger lives in ReactorSockets.Logging");
    }

    [Fact]
    public void CoreProject_DependencyGraph_HasNoPackageDependencies()
    {
        // Arrange — a PackageReference nothing in the core calls yet is invisible to
        // GetReferencedAssemblies, because the compiler never emits it, but it is still installed into
        // every consumer. The dependency graph the build resolved names it either way.
        string depsPath = Path.Combine(AppContext.BaseDirectory, "ReactorSockets.Logging.Tests.deps.json");
        using JsonDocument deps = JsonDocument.Parse(File.ReadAllText(depsPath));
        JsonElement target = deps.RootElement.GetProperty("targets").EnumerateObject().First().Value;

        // Act
        JsonElement core = target.EnumerateObject()
            .Single(library => library.Name.StartsWith("ReactorSockets/", StringComparison.Ordinal))
            .Value;
        IEnumerable<string> dependencies = core.TryGetProperty("dependencies", out JsonElement found)
            ? found.EnumerateObject().Select(dependency => dependency.Name).ToList()
            : [];

        // Assert
        dependencies.Should().BeEmpty("the core project takes no package dependency of any kind (ADR 0005)");
    }

    [Fact]
    public void AdapterAssembly_Version_MatchesTheCoreVersion()
    {
        // Arrange — the adapter versions in lockstep with the core (ADR 0006); both read one
        // VersionPrefix from src/Directory.Build.props.
        Version? core = CoreAssembly.GetName().Version;

        // Act
        Version? adapter = AdapterAssembly.GetName().Version;

        // Assert
        adapter.Should().Be(core);
    }

    [Fact]
    public void AdapterAssembly_Identity_IsReactorSocketsLogging()
    {
        // Arrange — named .Logging, not .Extensions.Logging: the Microsoft-shaped form asserts a
        // provenance this package does not have (ADR 0006).
        Assembly adapter = AdapterAssembly;

        // Act
        string? name = adapter.GetName().Name;

        // Assert
        name.Should().Be("ReactorSockets.Logging");
    }
}
