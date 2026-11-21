using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A <see cref="FactAttribute"/> for a test whose scenario only the Windows socket stack produces. Elsewhere
/// the test is skipped with the reason given, rather than failing on a platform that takes a different path.
/// </summary>
public sealed class WindowsFactAttribute : FactAttribute
{
    /// <summary>Skips the test unless it runs on Windows, giving <paramref name="reason"/> as the skip reason.</summary>
    public WindowsFactAttribute(string reason)
    {
        if (!OperatingSystem.IsWindows())
        {
            Skip = reason;
        }
    }
}
