using Xunit;

// Every test in this project binds real sockets on loopback at port zero and lets the OS assign an
// ephemeral port. xUnit's default is to run test classes in parallel, which makes that unsafe: a test
// that records an assigned port, releases it, and expects to rebind it races every other test binding
// port zero at that instant, because the OS is free to hand the just-freed port to whichever asks
// first. Dispose_WithAnOpenLink_ReleasesItsSocketSoThePortCanBeReboundElsewhere failed exactly this
// way with AddressAlreadyInUse, roughly one run in two.
//
// Serialising the assembly is the cheap fix rather than the careful one: the alternatives (retry
// loops, or a port allocator shared across test classes) add machinery to the tests to work around a
// property of the operating system. Stream Links bind considerably more sockets than datagram ones,
// which widens the window rather than narrowing it.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
