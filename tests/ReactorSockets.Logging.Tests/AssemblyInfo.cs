using Xunit;

// LoggerSinkStreamTests binds real sockets on loopback, as ReactorSockets.Tests does, so this assembly
// is serialised for the same reason that one is (see its AssemblyInfo.cs).
[assembly: CollectionBehavior(DisableTestParallelization = true)]
