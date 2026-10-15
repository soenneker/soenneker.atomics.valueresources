using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Atomics.ValueResources.Tests;

public sealed class ValueAtomicResourceTests
{
    [Test]
    public async Task Default_creation_reset_and_terminal_disposal()
    {
        ValueAtomicResource<object> resource = default;
        var cleaned = new ConcurrentBag<object>();
        ValueTask Cleanup(object value) { cleaned.Add(value); return default; }
        await Assert.That(resource.TryGet()).IsNull();
        await Assert.That(resource.IsDisposed).IsFalse();
        var first = new object();
        await Assert.That(ReferenceEquals(resource.GetOrCreate(first, static value => value, Cleanup), first)).IsTrue();
        await Assert.That(ReferenceEquals(resource.GetOrCreate(() => throw new Exception(), Cleanup), first)).IsTrue();
        var second = new object();
        await resource.Reset(second, static value => value, Cleanup);
        await Assert.That(ReferenceEquals(resource.TryGet(), second)).IsTrue();
        await Assert.That(cleaned.Single() == first).IsTrue();
        await resource.DisposeAsync(Cleanup);
        resource.Dispose(Cleanup);
        await resource.Reset(() => throw new Exception(), Cleanup);
        await Assert.That(resource.GetOrCreate(() => throw new Exception(), Cleanup)).IsNull();
        await Assert.That(resource.IsDisposed).IsTrue();
        await Assert.That(cleaned.Count).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Disposal_during_factory_cleans_candidate_once(bool reset)
    {
        ValueAtomicResource<object> resource = default;
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int cleanups = 0;
        object Factory()
        {
            entered.Set();
            if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
            return new object();
        }
        ValueTask Cleanup(object value) { Interlocked.Increment(ref cleanups); return default; }
        Task worker = Task.Run(async () =>
        {
            if (reset) await resource.Reset(Factory, Cleanup);
            else await Assert.That(resource.GetOrCreate(Factory, Cleanup)).IsNull();
        });
        try
        {
            await Assert.That(entered.Wait(TimeSpan.FromSeconds(10))).IsTrue();
            await resource.DisposeAsync(Cleanup);
        }
        finally { release.Set(); }
        await worker.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(cleanups).IsEqualTo(1);
        await Assert.That(resource.TryGet()).IsNull();
        await Assert.That(resource.IsDisposed).IsTrue();
    }

    [Test]
    public async Task Concurrent_creation_returns_one_winner_and_cleans_loser()
    {
        ValueAtomicResource<object> resource = default;
        using var barrier = new Barrier(2);
        var cleaned = new ConcurrentBag<object>();
        object Factory()
        {
            if (!barrier.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
            return new object();
        }
        ValueTask Cleanup(object value) { cleaned.Add(value); return default; }
        Task<object?> first = Task.Run(() => resource.GetOrCreate(Factory, Cleanup));
        Task<object?> second = Task.Run(() => resource.GetOrCreate(Factory, Cleanup));
        object?[] values = await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(ReferenceEquals(values[0], values[1])).IsTrue();
        await Assert.That(cleaned.Count).IsEqualTo(1);
        await Assert.That(ReferenceEquals(cleaned.Single(), values[0])).IsFalse();
        await resource.DisposeAsync(Cleanup);
        await Assert.That(cleaned.Distinct().Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Concurrent_resets_and_disposal_clean_every_candidate_once()
    {
        ValueAtomicResource<object> resource = default;
        var created = new ConcurrentBag<object>();
        var cleaned = new ConcurrentDictionary<object, int>();
        object Factory() { var value = new object(); created.Add(value); return value; }
        ValueTask Cleanup(object value) { cleaned.AddOrUpdate(value, 1, static (_, count) => count + 1); return default; }
        resource.GetOrCreate(Factory, Cleanup);
        Task[] workers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++) await resource.Reset(Factory, Cleanup);
        })).ToArray();
        await Task.Yield();
        await resource.DisposeAsync(Cleanup);
        await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(cleaned.Count).IsEqualTo(created.Count);
        await Assert.That(cleaned.Values.All(static count => count == 1)).IsTrue();
    }

    [Test]
    public async Task Factory_failures_preserve_state_and_allow_retry()
    {
        ValueAtomicResource<object> resource = default;
        static ValueTask Cleanup(object value) => default;
        bool rejected = false;
        try { resource.GetOrCreate(static () => null!, Cleanup); }
        catch (InvalidOperationException) { rejected = true; }
        await Assert.That(rejected).IsTrue();
        var value = resource.GetOrCreate(static () => new object(), Cleanup);
        rejected = false;
        try { await resource.Reset(static () => throw new InvalidOperationException(), Cleanup); }
        catch (InvalidOperationException) { rejected = true; }
        await Assert.That(rejected).IsTrue();
        await Assert.That(ReferenceEquals(resource.TryGet(), value)).IsTrue();
        await resource.DisposeAsync(Cleanup);
    }

    [Test]
    public async Task Async_teardown_is_awaited_and_failures_are_suppressed()
    {
        ValueAtomicResource<object> resource = default;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        resource.GetOrCreate(static () => new object(), static _ => default);
        ValueTask pending = resource.DisposeAsync(async _ =>
        {
            await release.Task.ConfigureAwait(false);
            throw new InvalidOperationException();
        });
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(resource.IsDisposed).IsTrue();
        release.SetResult();
        await pending;
        ValueAtomicResource<object> other = default;
        other.GetOrCreate(static () => new object(), static _ => default);
        await other.DisposeAsync(static _ => throw new InvalidOperationException());
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Source_backed_teardown_is_consumed_once(bool synchronous)
    {
        ValueAtomicResource<object> resource = default;
        var source = new CleanupSource();
        resource.GetOrCreate(static () => new object(), static _ => default);
        Task disposal = Task.Run(async () =>
        {
            if (synchronous) resource.Dispose(_ => source.Task);
            else await resource.DisposeAsync(_ => source.Task);
        });
        try
        {
            await source.Registered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(disposal.IsCompleted).IsFalse();
        }
        finally { source.Complete(); }
        await disposal.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(source.Consumptions).IsEqualTo(1);
        await Assert.That(resource.IsDisposed).IsTrue();
    }
}
