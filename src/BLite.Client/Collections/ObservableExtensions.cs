// BLite.Client — ObservableExtensions
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Lightweight Subscribe overloads for IObservable<T> so callers do not need
// a dependency on System.Reactive just to attach a simple action callback.

namespace BLite.Client;

public static class ObservableExtensions
{
    /// <summary>
    /// Subscribes to the observable with a single <paramref name="onNext"/> action.
    /// Errors are swallowed silently; completion is a no-op.
    /// </summary>
    public static IDisposable Subscribe<T>(this IObservable<T> source, Action<T> onNext) =>
        source.Subscribe(new DelegateObserver<T>(onNext, null, null));

    /// <summary>
    /// Subscribes to the observable with separate <paramref name="onNext"/>,
    /// <paramref name="onError"/>, and <paramref name="onCompleted"/> callbacks.
    /// </summary>
    public static IDisposable Subscribe<T>(
        this IObservable<T> source,
        Action<T> onNext,
        Action<Exception>? onError,
        Action? onCompleted) =>
        source.Subscribe(new DelegateObserver<T>(onNext, onError, onCompleted));

    private sealed class DelegateObserver<T>(
        Action<T> onNext,
        Action<Exception>? onError,
        Action? onCompleted) : IObserver<T>
    {
        public void OnNext(T value)          => onNext(value);
        public void OnError(Exception error) => onError?.Invoke(error);
        public void OnCompleted()            => onCompleted?.Invoke();
    }
}
