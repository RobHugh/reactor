namespace ReactorSockets;

/// <summary>
/// The outcome of an operation whose failures are the caller's to check rather than the runtime's to
/// throw — a wrong-kind call on a Link (<c>Send</c> on a datagram Link) or a send the peer or the
/// network refused. <see cref="Error"/> names the illegality or the platform failure in prose; there
/// is no error code enumeration here because, unlike <see cref="CloseReason"/>, nothing about this
/// failure needs to survive as permanent public API — a consumer branches on <see cref="IsSuccess"/>
/// and logs or surfaces <see cref="Error"/> as-is.
/// </summary>
public readonly record struct Result
{
    private Result(bool isSuccess, string? error)
    {
        IsSuccess = isSuccess;
        Error = error;
    }

    /// <summary>Whether the operation succeeded.</summary>
    public bool IsSuccess { get; }

    /// <summary>
    /// The failure reason, or <see langword="null"/> when <see cref="IsSuccess"/> is <see
    /// langword="true"/>.
    /// </summary>
    public string? Error { get; }

    /// <summary>Builds a successful result.</summary>
    public static Result Success() => new(true, null);

    /// <summary>Builds a failed result naming <paramref name="error"/> as the reason.</summary>
    public static Result Failure(string error) => new(false, error);
}
