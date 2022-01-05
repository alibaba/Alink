package com.alibaba.alink.common.concurrent;

import org.apache.flink.api.common.time.Time;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

public class FutureUtils {

	/**
	 * Forwards the value from the source future to the target future.
	 *
	 * @param source future to forward the value from
	 * @param target future to forward the value to
	 * @param <T> type of the value
	 */
	public static <T> void forward(CompletableFuture <T> source, CompletableFuture<T> target) {
		source.whenComplete(forwardTo(target));
	}

	/**
	 * Forwards the value from the source future to the target future using the provided executor.
	 *
	 * @param source future to forward the value from
	 * @param target future to forward the value to
	 * @param executor executor to forward the source value to the target future
	 * @param <T> type of the value
	 */
	public static <T> void forwardAsync(
		CompletableFuture<T> source, CompletableFuture<T> target, Executor executor) {
		source.whenCompleteAsync(forwardTo(target), executor);
	}

	/**
	 * Throws the causing exception if the given future is completed exceptionally, otherwise do
	 * nothing.
	 *
	 * @param future the future to check.
	 * @throws Exception when the future is completed exceptionally.
	 */
	public static void throwIfCompletedExceptionally(CompletableFuture<?> future) throws Exception {
		if (future.isCompletedExceptionally()) {
			future.get();
		}
	}

	private static <T> BiConsumer <T, Throwable> forwardTo(CompletableFuture<T> target) {
		return (value, throwable) -> doForward(value, throwable, target);
	}

	/**
	 * Completes the given future with either the given value or throwable, depending on which
	 * parameter is not null.
	 *
	 * @param value value with which the future should be completed
	 * @param throwable throwable with which the future should be completed exceptionally
	 * @param target future to complete
	 * @param <T> completed future
	 */
	public static <T> void doForward(
		@Nullable T value, @Nullable Throwable throwable, CompletableFuture<T> target) {
		if (throwable != null) {
			target.completeExceptionally(throwable);
		} else {
			target.complete(value);
		}
	}

	public static <T> ScheduledFuture <?> scheduleAtFixedRate(
		final Runnable command,
		final Time initialDelay,
		final Time period,
		final ScheduledExecutorService scheduledExecutor) {

		return scheduledExecutor.scheduleAtFixedRate(
			command,
			initialDelay.getSize(),
			period.getSize(),
			initialDelay.getUnit()
		);
	}
}
