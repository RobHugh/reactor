using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>The result of one <see cref="TcpLink.Send(ReadOnlySpan{byte})"/> call.</summary>
internal readonly struct TcpLinkSendResult
{
    private TcpLinkSendResult(bool isSuccess, int bytesSent, SocketError error)
    {
        IsSuccess = isSuccess;
        BytesSent = bytesSent;
        Error = error;
    }

    /// <summary>
    /// Whether the underlying socket call succeeded. A short send — <see cref="BytesSent"/> less than
    /// what was offered, including zero when the send buffer has no room at all right now — is still
    /// <see langword="true"/>: TCP has no notion of a send failing merely because it was partial.
    /// </summary>
    public bool IsSuccess { get; }

    /// <summary>The number of bytes actually accepted into the socket's send buffer this call.</summary>
    public int BytesSent { get; }

    /// <summary>The platform error, populated only when <see cref="IsSuccess"/> is <see langword="false"/>.</summary>
    public SocketError Error { get; }

    /// <summary>Builds a result reporting how many bytes the socket accepted.</summary>
    public static TcpLinkSendResult ForSent(int bytesSent) => new(true, bytesSent, default);

    /// <summary>Builds a failed result carrying the platform error.</summary>
    public static TcpLinkSendResult ForFailed(SocketError error) => new(false, 0, error);
}
