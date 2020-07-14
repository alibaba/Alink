package com.alibaba.alink.common.lazy;

import io.reactivex.rxjava3.subjects.ReplaySubject;

import java.util.List;
import java.util.function.Consumer;

/**
 * Represents some values that will be set for multiple times at any time.
 * <p>
 * When a value is set using {@link #addValue}, callbacks are triggered with that value.
 * Note that whenever the value is added, all callbacks are called;
 * and whenever callbacks are added, it will be called with all added values.
 *
 * @param <T> result type of the computation
 */
public class LazyEvaluation<T> {
    ReplaySubject<T> replaySubject;
    private Throwable throwable = null;

    public LazyEvaluation() {
        replaySubject = ReplaySubject.create();
    }

    /**
     * Add a callback.
     *
     * @param fn callback
     */
    public void addCallback(Consumer<T> fn) {
        replaySubject.subscribe(fn::accept, d -> {
            if (throwable == null) {
                throwable = d;
            }
        });
    }

    /**
     * Add multiple callbacks.
     *
     * @param callbacks callbacks
     */
    public void addCallbacks(List<Consumer<T>> callbacks) {
        for (Consumer<T> callback : callbacks) {
            addCallback(callback);
        }
    }

    /**
     * Add a value.
     *
     * @param v value
     */
    public void addValue(T v) {
        replaySubject.onNext(v);
        if (throwable != null) {
            throw new RuntimeException(throwable);
        }
    }

    /**
     * Get the latest value if have any, otherwise throw an exception.
     *
     * @return the latest value
     */
    public T getLatestValue() {
        if (replaySubject.hasValue()) {
            return replaySubject.getValue();
        } else {
            throw new RuntimeException("Result not set");
        }
    }
}
