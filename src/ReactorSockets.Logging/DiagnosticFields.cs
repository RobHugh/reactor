using System.Collections;

namespace ReactorSockets.Logging;

/// <summary>
/// A <see cref="DiagnosticRecord"/>'s typed fields as the key-value list structured logging providers
/// read: the id, level and subject kind, the subject's own fields, then the two value slots. A Link or
/// Peer subject contributes <c>Reactor</c>, <c>Slot</c> and <c>Generation</c>; a Source contributes
/// <c>ClaimedSource</c>, named as a claim because whoever sent the datagram chose it (ADR 0013).
/// </summary>
internal readonly struct DiagnosticFields(DiagnosticRecord record) : IReadOnlyList<KeyValuePair<string, object?>>
{
    /// <summary>The record these fields are read from, kept so the message can be formatted from it.</summary>
    public DiagnosticRecord Record { get; } = record;

    /// <summary>Three header fields, the subject's fields, and two value slots.</summary>
    public int Count => 5 + SubjectFieldCount;

    private int SubjectFieldCount => Record.SubjectKind switch
    {
        DiagnosticSubjectKind.Link or DiagnosticSubjectKind.Peer => 3,
        DiagnosticSubjectKind.Source => 1,
        DiagnosticSubjectKind.None => 0,
        _ => throw UnrecognisedSubjectKind(),
    };

    /// <summary>The field at <paramref name="index"/>, in the order the type summary lists them.</summary>
    public KeyValuePair<string, object?> this[int index]
    {
        get
        {
            int subjectEnd = 3 + SubjectFieldCount;
            return index switch
            {
                0 => new("DiagnosticId", Record.Id),
                1 => new("DiagnosticLevel", Record.Level),
                2 => new("SubjectKind", Record.SubjectKind),
                _ when index < subjectEnd => SubjectField(index - 3),
                _ when index == subjectEnd => new("Value0", Record.Value0),
                _ when index == subjectEnd + 1 => new("Value1", Record.Value1),
                _ => throw new ArgumentOutOfRangeException(nameof(index), index, $"index must be below {Count}."),
            };
        }
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (int i = 0; i < Count; i++)
        {
            yield return this[i];
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>The subject's field at <paramref name="offset"/>, one switch over the tagged subject.</summary>
    private KeyValuePair<string, object?> SubjectField(int offset) => (Record.SubjectKind, offset) switch
    {
        (DiagnosticSubjectKind.Link, 0) => new("Reactor", Record.Link.Origin),
        (DiagnosticSubjectKind.Link, 1) => new("Slot", Record.Link.Index),
        (DiagnosticSubjectKind.Link, _) => new("Generation", Record.Link.Generation),
        (DiagnosticSubjectKind.Peer, 0) => new("Reactor", Record.Peer.Origin),
        (DiagnosticSubjectKind.Peer, 1) => new("Slot", Record.Peer.Index),
        (DiagnosticSubjectKind.Peer, _) => new("Generation", Record.Peer.Generation),
        (DiagnosticSubjectKind.Source, _) => new("ClaimedSource", Record.Source.ToIPEndPoint().ToString()),
        _ => throw UnrecognisedSubjectKind(),
    };

    /// <summary>
    /// The throw for a subject kind outside the roster, which only a hand-forged record can carry —
    /// worded as <see cref="DiagnosticFormatter.Format"/> words its own.
    /// </summary>
    private ArgumentOutOfRangeException UnrecognisedSubjectKind() => new(
        nameof(Record), Record.SubjectKind, $"{Record.SubjectKind} is not a recognised DiagnosticSubjectKind.");
}
