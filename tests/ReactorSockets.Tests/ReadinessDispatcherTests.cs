using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

public class ReadinessDispatcherTests : IDisposable
{
    private readonly List<Socket> _sockets = [];

    public void Dispose()
    {
        foreach (Socket socket in _sockets)
        {
            socket.Dispose();
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public void Pump_NeverCalled_NoCallbackFiresAndNoIntervalObserved()
    {
        // Arrange
        (Socket receiver, Socket sender) = CreateReadyLoopbackPair();
        ReadinessDispatcher dispatcher = new();
        int callbackCount = 0;
        dispatcher.Register(receiver, () => callbackCount++);

        // Act — deliberately no Pump call.

        // Assert
        callbackCount.Should().Be(0);
        dispatcher.ObservedPumpInterval.Should().BeNull();
        sender.Should().NotBeNull();
    }

    [Fact]
    public void Pump_SocketWithDataWaiting_InvokesRegisteredCallback()
    {
        // Arrange
        (Socket receiver, _) = CreateReadyLoopbackPair();
        ReadinessDispatcher dispatcher = new();
        int callbackCount = 0;
        dispatcher.Register(receiver, () => callbackCount++);

        // Act
        dispatcher.Pump();

        // Assert
        callbackCount.Should().Be(1);
    }

    [Fact]
    public void Pump_SocketWithNoDataWaiting_DoesNotInvokeCallback()
    {
        // Arrange
        Socket idle = CreateUnboundLoopbackSocket();
        idle.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        ReadinessDispatcher dispatcher = new();
        int callbackCount = 0;
        dispatcher.Register(idle, () => callbackCount++);

        // Act
        dispatcher.Pump();

        // Assert
        callbackCount.Should().Be(0);
    }

    [Fact]
    public void Pump_CalledFromAnotherThread_ThrowsInvalidOperationException()
    {
        // Arrange
        ReadinessDispatcher dispatcher = new();

        // Act
        Exception? observed = null;
        Thread otherThread = new(() =>
        {
            try
            {
                dispatcher.Pump();
            }
            catch (Exception exception)
            {
                observed = exception;
            }
        });
        otherThread.Start();
        otherThread.Join();

        // Assert
        observed.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public void Pump_TwoSuccessivePumps_ObservesElapsedMonotonicInterval()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ReadinessDispatcher dispatcher = new(timeProvider);

        // Act
        dispatcher.Pump();
        timeProvider.Advance(TimeSpan.FromMilliseconds(16));
        dispatcher.Pump();

        // Assert
        dispatcher.ObservedPumpInterval.Should().Be(TimeSpan.FromMilliseconds(16));
    }

    [Fact]
    public void Pump_SeveralReadyHandlesOverManyPumps_DoesNotServeTheSameHandleFirstEveryTime()
    {
        // Arrange
        ReadinessDispatcher dispatcher = new();
        List<int> firedOrder = [];
        for (int i = 0; i < 3; i++)
        {
            (Socket receiver, _) = CreateReadyLoopbackPair();
            int handleId = i;
            dispatcher.Register(receiver, () => firedOrder.Add(handleId));
        }

        // Act
        HashSet<int> firstFiredPerPump = [];
        for (int pump = 0; pump < 3; pump++)
        {
            firedOrder.Clear();
            dispatcher.Pump();
            firstFiredPerPump.Add(firedOrder[0]);
        }

        // Assert
        firstFiredPerPump.Should().HaveCount(3);
    }

    [Fact]
    public void Pump_SteadyStateAfterWarmup_AllocatesNothingOnTheManagedHeap()
    {
        // Arrange
        ReadinessDispatcher dispatcher = new();
        (Socket receiver, _) = CreateReadyLoopbackPair();
        Socket idle = CreateUnboundLoopbackSocket();
        idle.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        dispatcher.Register(receiver, () => { });
        dispatcher.Register(idle, () => { });

        for (int warmup = 0; warmup < 10; warmup++)
        {
            dispatcher.Pump();
        }

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            dispatcher.Pump();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }

    private (Socket Receiver, Socket Sender) CreateReadyLoopbackPair()
    {
        Socket receiver = CreateUnboundLoopbackSocket();
        receiver.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        Socket sender = CreateUnboundLoopbackSocket();
        sender.Connect(receiver.LocalEndPoint!);
        sender.Send([1, 2, 3]);

        return (receiver, sender);
    }

    private Socket CreateUnboundLoopbackSocket()
    {
        Socket socket = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _sockets.Add(socket);
        return socket;
    }
}
