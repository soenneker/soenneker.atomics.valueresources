using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Atomics.ValueResources;

/// <summary>
/// Provides inline, thread-safe lazy creation, replacement, and teardown of a reference-type resource.
/// </summary>
/// <typeparam name="T">The reference type owned by this resource.</typeparam>
/// <remarks>
/// Store this mutable struct in a non-readonly field and do not copy it: copies have independent lifecycle
/// state and may tear down the same resource. The default value is ready to use and stores one reference.
/// Pass consistent teardown callbacks for every operation. Factories must return fresh, non-null instances;
/// concurrent factories may run, and losing candidates are torn down. Cleanup failures are suppressed.
/// Returned references are not leases: concurrent reset or disposal may tear them down while in use.
/// Disposal is terminal, but does not wait for other operations' in-flight factories or cleanup.
/// </remarks>
public struct ValueAtomicResource<T> where T : class
{
    // A single publication slot keeps value and terminal lifecycle state inseparable.
    private object? _value;

    /// <summary>Gets whether this resource has been permanently disposed.</summary>
    public bool IsDisposed
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ReferenceEquals(Volatile.Read(ref _value), ValueAtomicResourceSentinel.Disposed);
    }

    /// <summary>Returns the current resource, or null if uninitialized or disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T? TryGet() => Unwrap(Volatile.Read(ref _value));

    /// <summary>Returns the current resource, creating it if necessary, or null if disposed.</summary>
    /// <remarks>
    /// Factory exceptions propagate and allow a later retry. A null factory result is rejected.
    /// Losing candidates are cleaned up asynchronously; this method does not wait for their teardown.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T? GetOrCreate(Func<T> factory, Func<T, ValueTask> teardown)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(teardown);
        object? value = Volatile.Read(ref _value);
        return value is null ? Publish(RequireValue(factory()), teardown) : Unwrap(value);
    }

    /// <summary>Gets or creates the resource with caller-supplied state, allowing a static factory without a closure.</summary>
    /// <remarks>Has the same publication, failure, and teardown behavior as <see cref="GetOrCreate(Func{T}, Func{T, ValueTask})"/>.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T? GetOrCreate<TState>(TState state, Func<TState, T> factory, Func<T, ValueTask> teardown)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(teardown);
        object? value = Volatile.Read(ref _value);
        return value is null ? Publish(RequireValue(factory(state)), teardown) : Unwrap(value);
    }

    private T? Publish(T created, Func<T, ValueTask> teardown)
    {
        object? observed = Interlocked.CompareExchange(ref _value, created, null);
        if (observed is null)
            return created;

        _ = Teardown(created, teardown);
        return Unwrap(observed);
    }

    /// <summary>Publishes a fresh resource and returns a task for teardown of the replaced resource.</summary>
    /// <remarks>
    /// Does nothing after disposal. If disposal wins during creation, the candidate is torn down instead.
    /// Factory failures leave the current resource unchanged and are returned as a faulted task.
    /// A later concurrent reset or disposal may replace or remove this operation's published resource.
    /// </remarks>
    public ValueTask Reset(Func<T> factory, Func<T, ValueTask> teardown)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(teardown);
        if (IsDisposed)
            return default;

        T fresh;
        try { fresh = RequireValue(factory()); }
        catch (Exception exception) { return ValueTask.FromException(exception); }
        return Replace(fresh, teardown);
    }

    /// <summary>Resets the resource with caller-supplied state, allowing a static factory without a closure.</summary>
    /// <remarks>Has the same publication, failure, and teardown behavior as <see cref="Reset(Func{T}, Func{T, ValueTask})"/>.</remarks>
    public ValueTask Reset<TState>(TState state, Func<TState, T> factory, Func<T, ValueTask> teardown)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(teardown);
        if (IsDisposed)
            return default;

        T fresh;
        try { fresh = RequireValue(factory(state)); }
        catch (Exception exception) { return ValueTask.FromException(exception); }
        return Replace(fresh, teardown);
    }

    private ValueTask Replace(T fresh, Func<T, ValueTask> teardown)
    {
        object? value = Volatile.Read(ref _value);
        while (true)
        {
            if (ReferenceEquals(value, ValueAtomicResourceSentinel.Disposed))
                return Teardown(fresh, teardown);

            object? observed = Interlocked.CompareExchange(ref _value, fresh, value);
            if (ReferenceEquals(observed, value))
                return value is null ? default : Teardown(Unsafe.As<T>(value)!, teardown);
            value = observed;
        }
    }

    /// <summary>Permanently detaches the resource and returns a task for its best-effort teardown.</summary>
    /// <remarks>Repeated calls are no-ops and do not wait for cleanup initiated by earlier calls.</remarks>
    public ValueTask DisposeAsync(Func<T, ValueTask> teardown)
    {
        ArgumentNullException.ThrowIfNull(teardown);
        // Avoid a locked write (and cache-line invalidation) after terminal disposal.
        if (IsDisposed)
            return default;

        T? value = Unwrap(Interlocked.Exchange(ref _value, ValueAtomicResourceSentinel.Disposed));
        return value is null ? default : Teardown(value, teardown);
    }

    /// <summary>Permanently detaches the resource and waits synchronously for its best-effort teardown.</summary>
    /// <remarks>Prefer asynchronous disposal when teardown may capture the caller's synchronization context.</remarks>
    public void Dispose(Func<T, ValueTask> teardown)
    {
        ValueTask pending = DisposeAsync(teardown);
        if (pending.IsCompletedSuccessfully)
            pending.GetAwaiter().GetResult();
        else
            pending.AsTask().GetAwaiter().GetResult();
    }

    private static T RequireValue(T? value) => value ?? throw new InvalidOperationException("The resource factory returned null.");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T? Unwrap(object? value) =>
        // Only null, T, or the sentinel can enter the private slot; no runtime type check is needed.
        ReferenceEquals(value, ValueAtomicResourceSentinel.Disposed) ? null : Unsafe.As<T>(value);

    private static ValueTask Teardown(T value, Func<T, ValueTask> teardown)
    {
        try
        {
            ValueTask pending = teardown(value);
            if (!pending.IsCompletedSuccessfully)
                return AwaitTeardown(pending);
            pending.GetAwaiter().GetResult();
        }
        catch
        {
            // Cleanup is best effort, including synchronous callback failures.
        }
        return default;
    }

    private static async ValueTask AwaitTeardown(ValueTask pending)
    {
        try { await pending.ConfigureAwait(false); }
        catch { /* Cleanup is best effort, including asynchronous callback failures. */ }
    }
}
