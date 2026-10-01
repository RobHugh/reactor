using System.Reflection;

namespace ReactorSockets.Tests;

/// <summary>
/// Reads a counter snapshot's fields by reflection, so an audit asserting "this field moved and no
/// other did" covers a field added later without being edited (issue #61). Every field of a counter
/// snapshot is a <see cref="ulong"/>; a field of any other type throws rather than being skipped.
/// </summary>
internal static class CounterFields
{
    /// <summary>Every field of <typeparamref name="T"/> by name, with its value in <paramref name="snapshot"/>.</summary>
    public static IReadOnlyDictionary<string, ulong> Read<T>(T snapshot)
        where T : struct =>
        typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .ToDictionary(field => field.Name, field => field.GetValue(snapshot) is ulong value
                ? value
                : throw new InvalidOperationException($"{typeof(T).Name}.{field.Name} is not a ulong."));

    /// <summary>The names of the fields whose value differs between <paramref name="before"/> and <paramref name="after"/>.</summary>
    public static IReadOnlyList<string> Moved<T>(T before, T after)
        where T : struct
    {
        IReadOnlyDictionary<string, ulong> beforeValues = Read(before);
        IReadOnlyDictionary<string, ulong> afterValues = Read(after);
        return [.. beforeValues.Keys.Where(name => beforeValues[name] != afterValues[name])];
    }

    /// <summary>The names of the fields that fell between <paramref name="before"/> and <paramref name="after"/>.</summary>
    public static IReadOnlyList<string> Fell<T>(T before, T after)
        where T : struct
    {
        IReadOnlyDictionary<string, ulong> beforeValues = Read(before);
        IReadOnlyDictionary<string, ulong> afterValues = Read(after);
        return [.. beforeValues.Keys.Where(name => afterValues[name] < beforeValues[name])];
    }
}
