using System.Reflection;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class AssemblySkeletonTests
{
    private static Assembly ReactorSocketsAssembly => Assembly.Load(new AssemblyName("ReactorSockets"));

    [Fact]
    public void ReactorSocketsAssembly_Loaded_HasExpectedIdentity()
    {
        // Arrange
        Assembly assembly = ReactorSocketsAssembly;

        // Act
        string? assemblyName = assembly.GetName().Name;

        // Assert
        assemblyName.Should().Be("ReactorSockets");
    }

    [Fact]
    public void ReactorSocketsAssembly_ExportedTypes_AreOnlyTheL2SurfaceAConsumerDrives()
    {
        // Arrange
        Assembly assembly = ReactorSocketsAssembly;

        // Act
        Type[] publicTypes = assembly.GetExportedTypes();

        // Assert
        publicTypes.Select(type => type.Name).Should().BeEquivalentTo(
            nameof(RawReactor),
            nameof(RawEvent),
            nameof(RawEventKind),
            nameof(RawEventEnumerator),
            nameof(CloseReason),
            nameof(Result),
            nameof(SendResult),
            nameof(LinkId),
            nameof(Source),
            nameof(SourceFamily),
            nameof(ReactorOptions),
            nameof(IReactorDiagnostics),
            nameof(DiagnosticRecord),
            nameof(DiagnosticLevel),
            nameof(DiagnosticId),
            nameof(DiagnosticSubjectKind),
            nameof(ReactorCounters),
            nameof(LinkCounters));
    }

    [Fact]
    public void ReactorSocketsAssembly_ExportedTypes_ContainNothingFromL0OrL1()
    {
        // Arrange — L0 and L1 are internal so the dispatcher and the transports can be replaced
        // wholesale without a major version. This is the guard on that, written against namespaces
        // rather than a list of type names: a new type added to Core/ or Transports/ and left public
        // fails here without anyone having to remember to extend an allow-list.
        Assembly assembly = ReactorSocketsAssembly;

        // Act
        // `!` works around a framework limitation: Type.FullName is declared string? because it is
        // null for generic parameters and a few other constructed forms, none of which GetExportedTypes
        // can return.
        IEnumerable<string> leaked = assembly.GetExportedTypes()
            .Where(type => type.Namespace != "ReactorSockets")
            .Select(type => type.FullName!);

        // Assert
        leaked.Should().BeEmpty("only the root namespace is the consumer's surface — ReactorSockets.Core and ReactorSockets.Transports are implementation");
    }

    [Fact]
    public void ReactorSocketsAssembly_Attributes_GrantNoTestAssemblyAccessToInternals()
    {
        // Arrange — L0 and L1 are internal and untested-through: the single seam is the public L2
        // surface (ADR 0015). The namespace guard above cannot catch a breach of that, because
        // InternalsVisibleTo exports nothing and leaves every other test in this file green. This is
        // the guard on the policy itself, and it is the reason the policy is a fact rather than a
        // memory — ADR 0003 claimed the opposite for two slices before anyone noticed.
        Assembly assembly = ReactorSocketsAssembly;

        // Act
        IEnumerable<string> grants = assembly
            .GetCustomAttributes<InternalsVisibleToAttribute>()
            .Select(attribute => attribute.AssemblyName);

        // Assert
        grants.Should().BeEmpty("tests observe only what a consumer can observe — an invariant that cannot be reached through the seam is designed away, not reached past (ADR 0015)");
    }

    [Fact]
    public void ReactorSocketsAssembly_ExportedTypes_ExposeNoWayToResetACounter()
    {
        // Arrange — counters are monotonic and pull-only (ADR 0013): two readers sampling
        // independently must both see a rising figure, so no reset may exist for either to call.
        Assembly assembly = ReactorSocketsAssembly;

        // Act
        IEnumerable<string> resetters =
            from type in assembly.GetExportedTypes()
            from method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            where method.Name.Contains("Reset", StringComparison.Ordinal) || method.Name.Contains("Clear", StringComparison.Ordinal)
            select $"{type.Name}.{method.Name}";

        // Assert
        resetters.Should().BeEmpty();
    }

    [Fact]
    public void ReactorSocketsAssembly_PublicMembers_NoneAcceptAStringHostOrAddress()
    {
        // Arrange
        // The only string a public member may take is prose — Result.Failure's reason — so this
        // matches on parameters named like an address rather than banning string outright.
        Assembly assembly = ReactorSocketsAssembly;
        Type[] publicTypes = assembly.GetExportedTypes();
        string[] addressLikeNames = ["host", "hostname", "address", "endpoint"];

        // Act
        IEnumerable<string> offendingMembers =
            from type in publicTypes
            from method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            from parameter in method.GetParameters()
            where parameter.ParameterType == typeof(string)
            // `!` works around a framework limitation: ParameterInfo.Name is declared string? for
            // return parameters and metadata without names, neither of which GetParameters yields
            // for a method compiled from C# source.
            where addressLikeNames.Any(name => parameter.Name!.Contains(name, StringComparison.OrdinalIgnoreCase))
            select $"{type.Name}.{method.Name}({parameter.Name})";

        // Assert
        offendingMembers.Should().BeEmpty("the library never resolves a hostname — every address surface takes IPEndPoint or Source, never a string");
    }
}
