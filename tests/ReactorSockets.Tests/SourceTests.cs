using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class SourceTests
{
    public static TheoryData<IPEndPoint> RoundTrippableEndpoints => new()
    {
        new IPEndPoint(IPAddress.Parse("203.0.113.7"), 51_820),
        new IPEndPoint(IPAddress.Parse("2001:db8::1"), 51_820),
        new IPEndPoint(new IPAddress(IPAddress.Parse("fe80::1").GetAddressBytes(), scopeid: 3), 51_820),
    };

    [Theory]
    [MemberData(nameof(RoundTrippableEndpoints))]
    public void ToIPEndPoint_RoundTrippedFromFromIPEndPoint_PreservesFamilyAddressScopeAndPort(IPEndPoint original)
    {
        // Arrange
        Source source = Source.FromIPEndPoint(original);

        // Act
        IPEndPoint roundTripped = source.ToIPEndPoint();

        // Assert
        roundTripped.Should().Be(original);
        if (original.AddressFamily == AddressFamily.InterNetworkV6)
        {
            roundTripped.Address.ScopeId.Should().Be(original.Address.ScopeId);
        }
    }

    [Fact]
    public void Equality_SourcesDifferingOnlyInScopeId_AreNotEqual()
    {
        // Arrange
        IPAddress linkLocal = IPAddress.Parse("fe80::1");
        Source onInterfaceTwo = Source.FromIPEndPoint(new IPEndPoint(new IPAddress(linkLocal.GetAddressBytes(), scopeid: 2), 9000));
        Source onInterfaceThree = Source.FromIPEndPoint(new IPEndPoint(new IPAddress(linkLocal.GetAddressBytes(), scopeid: 3), 9000));

        // Act
        bool areEqual = onInterfaceTwo == onInterfaceThree;

        // Assert
        areEqual.Should().BeFalse();
    }

    [Fact]
    public void Equality_SourcesDifferingOnlyInScopeId_HaveDifferentHashCodes()
    {
        // Arrange
        IPAddress linkLocal = IPAddress.Parse("fe80::1");
        Source onInterfaceTwo = Source.FromIPEndPoint(new IPEndPoint(new IPAddress(linkLocal.GetAddressBytes(), scopeid: 2), 9000));
        Source onInterfaceThree = Source.FromIPEndPoint(new IPEndPoint(new IPAddress(linkLocal.GetAddressBytes(), scopeid: 3), 9000));

        // Act & Assert
        onInterfaceTwo.GetHashCode().Should().NotBe(onInterfaceThree.GetHashCode());
    }

    [Fact]
    public void Equality_IPv4AndIPv6SourcesHoldingSameBytes_AreNotEqual()
    {
        // Arrange
        UInt128 sharedBytes = 12_345;
        Source ipv4 = new(SourceFamily.IPv4, sharedBytes, ScopeId: 0, Port: 9000);
        Source ipv6 = new(SourceFamily.IPv6, sharedBytes, ScopeId: 0, Port: 9000);

        // Act
        bool areEqual = ipv4 == ipv6;

        // Assert
        areEqual.Should().BeFalse();
    }

    [Fact]
    public void Equality_IPv4AndIPv6SourcesHoldingSameBytes_HaveDifferentHashCodes()
    {
        // Arrange
        UInt128 sharedBytes = 12_345;
        Source ipv4 = new(SourceFamily.IPv4, sharedBytes, ScopeId: 0, Port: 9000);
        Source ipv6 = new(SourceFamily.IPv6, sharedBytes, ScopeId: 0, Port: 9000);

        // Act & Assert
        ipv4.GetHashCode().Should().NotBe(ipv6.GetHashCode());
    }

    [Fact]
    public void Equality_IdenticalSources_AreEqualWithMatchingHashCodes()
    {
        // Arrange
        Source first = new(SourceFamily.IPv6, AddressBytes: 42, ScopeId: 3, Port: 9000);
        Source second = new(SourceFamily.IPv6, AddressBytes: 42, ScopeId: 3, Port: 9000);

        // Act & Assert
        first.Should().Be(second);
        first.GetHashCode().Should().Be(second.GetHashCode());
    }

    [Fact]
    public void Source_SizeOf_MatchesTheDocumentedBudget()
    {
        // Arrange & Act
        int size = Unsafe.SizeOf<Source>();

        // Assert — pinned so a later field addition is a deliberate decision (ADR 0013's
        // roughly-56-byte budget), not a silent regression.
        size.Should().Be(48);
    }
}
