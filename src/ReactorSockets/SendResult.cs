namespace ReactorSockets;

/// <summary>
/// The outcome of one <see cref="RawReactor.Send"/> call. Unlike <see cref="Result"/>, a stream send
/// has a quantity worth reporting even on success: the Reactor holds no outbound bytes (ADR 0008), so
/// <c>Send</c> takes what the socket will take right now and hands back the count — the caller keeps
/// whatever was not taken, and <see cref="BytesSent"/> is how it learns how much that is.
/// </summary>
public readonly record struct SendResult
{
    private SendResult(bool isSuccess, int bytesSent, string? error)
    {
        IsSuccess = isSuccess;
        BytesSent = bytesSent;
        Error = error;
    }

    /// <summary>
    /// Whether the call itself succeeded — not whether every offered byte was taken. A short send
    /// (<see cref="BytesSent"/> less than what was offered) is still <see langword="true"/>: it arms
    /// exactly one <see cref="RawEventKind.Writable"/> for this Link and is otherwise ordinary. This is
    /// <see langword="false"/> only for an illegal call (wrong Link kind, a stale <see cref="LinkId"/>,
    /// a Link already shut down for writing) or a socket-level failure.
    /// </summary>
    public bool IsSuccess { get; }

    /// <summary>
    /// The number of bytes the socket actually took this call. Zero when <see cref="IsSuccess"/> is
    /// <see langword="false"/>. Never queued or copied by the Reactor — whatever this does not account
    /// for out of the offered payload is still the caller's to keep and retry.
    /// </summary>
    public int BytesSent { get; }

    /// <summary>The failure reason, or <see langword="null"/> when <see cref="IsSuccess"/> is <see langword="true"/>.</summary>
    public string? Error { get; }

    /// <summary>Builds a successful result reporting how many bytes the socket actually took.</summary>
    public static SendResult Success(int bytesSent) => new(true, bytesSent, null);

    /// <summary>Builds a failed result naming <paramref name="error"/> as the reason.</summary>
    public static SendResult Failure(string error) => new(false, 0, error);
}
