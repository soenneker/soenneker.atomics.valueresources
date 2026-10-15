using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Soenneker.Atomics.ValueResources.Tests;

internal sealed class CleanupSource : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };
    internal readonly TaskCompletionSource Registered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal int Consumptions;
    internal ValueTask Task => new(this, _core.Version);
    internal void Complete() => _core.SetResult(true);
    public void GetResult(short token) { _core.GetResult(token); Consumptions++; }
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        _core.OnCompleted(continuation, state, token, flags);
        Registered.TrySetResult();
    }
}
