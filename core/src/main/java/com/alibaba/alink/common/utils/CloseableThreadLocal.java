package com.alibaba.alink.common.utils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Similar with {@link ThreadLocal}, but equipped with `close` method. All thread-local objects are cleared when `close`
 * called.
 */
public class CloseableThreadLocal<T> implements Closeable {

	private final ConcurrentHashMap <Thread, T> threadObjMap = new ConcurrentHashMap <>();
	private final Consumer <T> consumer;
	private final Supplier <? extends T> supplier;

	public CloseableThreadLocal(Supplier <? extends T> supplier) {
		this(supplier, null);
	}

	public CloseableThreadLocal(Supplier <? extends T> supplier, Consumer <T> consumer) {
		this.supplier = Objects.requireNonNull(supplier);
		this.consumer = consumer;
	}

	public T get() {
		return threadObjMap.computeIfAbsent(Thread.currentThread(), d -> supplier.get());
	}

	public void set(T obj) {
		threadObjMap.put(Thread.currentThread(), obj);
	}

	private void removeByThread(Thread thread) {
		if (threadObjMap.containsKey(thread)) {
			T obj = threadObjMap.get(thread);
			if (null != consumer) {
				consumer.accept(obj);
			}
			threadObjMap.remove(thread);
		}
	}

	public void remove() {
		removeByThread(Thread.currentThread());
	}

	@Override
	public void close() {
		for (Thread thread : threadObjMap.keySet()) {
			removeByThread(thread);
		}
		threadObjMap.clear();
	}
}
