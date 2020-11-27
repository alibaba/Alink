package com.alibaba.alink.common.lazy;

import io.reactivex.rxjava3.subjects.ReplaySubject;

import java.util.List;
import java.util.function.Consumer;

/**
 * Represents some values that will be set at any time for multiple times.
 * <p>
 * When a values is added using {@link #addValue}, callbacks are called with the value.
 * Note that whenever the value is added, all callbacks are called;
 * and whenever callbacks are added, it will be called with all added values.
 *
 * @param <T> result type of the computation
 */
public class LazyEvaluation<T> {
	ReplaySubject <T> replaySubject;
	private Throwable throwable = null;

	public LazyEvaluation() {
		replaySubject = ReplaySubject.create();
	}

	public void addCallback(Consumer <T> fn) {
		replaySubject.subscribe(fn::accept, d -> {
			if (throwable == null) {
				throwable = d;
			}
		});
	}

	public void addCallbacks(List <Consumer <T>> callbacks) {
		for (Consumer <T> callback : callbacks) {
			addCallback(callback);
		}
	}

	public void addValue(T v) {
		replaySubject.onNext(v);
		if (throwable != null) {
			throw new RuntimeException(throwable);
		}
	}

	public T getLatestValue() {
		if (replaySubject.hasValue()) {
			return replaySubject.getValue();
		} else {
			throw new RuntimeException("Result not set");
		}
	}
}
