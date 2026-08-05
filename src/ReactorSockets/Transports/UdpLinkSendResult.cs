using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>The result of one <see cref="UdpLink.SendTo"/> call.</summary>
internal readonly struct UdpLinkSendResult
{
    private UdpLinkSendResult(bool isSuccess, int bytesSent, SocketError error)
    {
        IsSuccess = isSuccess;
        BytesSent = bytesSent;
        Error = error;
    }

    /// <summary>Whether every offered byte was sent in one datagram (ADR 0008: a datagram send is all-or-nothing).</summary>
    public bool IsSuccess { get; }

    /// <summary>The number of bytes actually sent.</summary>
    public int BytesSent { get; }

    /// <summary>The platform error, populated only when <see cref="IsSuccess"/> is <see langword="false"/> due to an exception.</summary>
    public SocketError Error { get; }

    public static UdpLinkSendResult ForSent(int bytesSent, int bytesOffered) => new(bytesSent == bytesOffered, bytesSent, default);

    public static UdpLinkSendResult ForFailed(SocketError error) => new(false, 0, error);
}
