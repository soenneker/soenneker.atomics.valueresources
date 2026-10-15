namespace Soenneker.Atomics.ValueResources;

internal static class ValueAtomicResourceSentinel
{
    internal static readonly object Disposed = new();
}
