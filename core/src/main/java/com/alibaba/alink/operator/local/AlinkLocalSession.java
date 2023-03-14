package com.alibaba.alink.operator.local;

import com.alibaba.alink.common.concurrent.ExecutorThreadFactory;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AlinkLocalSession {

	private static final Logger LOG = LoggerFactory.getLogger(AlinkLocalSession.class);

	private static int DEFAULT_NUM_THREADS = 1;		// Runtime.getRuntime().availableProcessors() * 4 / 8;

	private static final int GLOBAL_POOL_SIZE = Runtime.getRuntime().availableProcessors();

	public static final ExecutorService executor = new ThreadPoolExecutor(
		GLOBAL_POOL_SIZE, GLOBAL_POOL_SIZE, 0, TimeUnit.SECONDS,
		new LinkedBlockingQueue <>(),
		new ExecutorThreadFactory(),
		new ThreadPoolExecutor.AbortPolicy()
	);

	public static final ExecutorService ioExecutor = new ThreadPoolExecutor(
		GLOBAL_POOL_SIZE, GLOBAL_POOL_SIZE, 0, TimeUnit.SECONDS,
		new LinkedBlockingQueue <>(),
		new ExecutorThreadFactory(),
		new ThreadPoolExecutor.AbortPolicy()
	);


	public static final DistributedInfo DISTRIBUTOR = new DefaultDistributedInfo();

	static {
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						gracefulShutdown(5, TimeUnit.SECONDS, executor);
					} catch (Exception e) {
						LOG.info("Shutdown executor service error.", e);
					}
				}
			});
		} catch (Exception ex) {
			LOG.info("Recycle resource error.", ex);
		}

		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						gracefulShutdown(5, TimeUnit.SECONDS, ioExecutor);
					} catch (Exception e) {
						LOG.info("Shutdown executor service error.", e);
					}
				}
			});
		} catch (Exception ex) {
			LOG.info("Recycle resource error.", ex);
		}
	}

	static void setDefaultNumThreads(int numThreads) {
		DEFAULT_NUM_THREADS = numThreads;
	}

	static int getDefaultNumThreads() {
		return DEFAULT_NUM_THREADS;
	}

	/**
	 * Gracefully shutdown the given {@link ExecutorService}. The call waits the given timeout that
	 * all ExecutorServices terminate. If the ExecutorServices do not terminate in this time,
	 * they will be shut down hard.
	 *
	 * @param timeout          to wait for the termination of all ExecutorServices
	 * @param unit             of the timeout
	 * @param executorServices to shut down
	 */
	public static void gracefulShutdown(long timeout, TimeUnit unit, ExecutorService... executorServices) {
		for (ExecutorService executorService : executorServices) {
			executorService.shutdown();
		}

		boolean wasInterrupted = false;
		final long endTime = unit.toMillis(timeout) + System.currentTimeMillis();
		long timeLeft = unit.toMillis(timeout);
		boolean hasTimeLeft = timeLeft > 0L;

		for (ExecutorService executorService : executorServices) {
			if (wasInterrupted || !hasTimeLeft) {
				executorService.shutdownNow();
			} else {
				try {
					if (!executorService.awaitTermination(timeLeft, TimeUnit.MILLISECONDS)) {
						LOG.warn("ExecutorService did not terminate in time. Shutting it down now.");
						executorService.shutdownNow();
					}
				} catch (InterruptedException e) {
					LOG.warn("Interrupted while shutting down executor services. Shutting all " +
						"remaining ExecutorServices down now.", e);
					executorService.shutdownNow();

					wasInterrupted = true;

					Thread.currentThread().interrupt();
				}

				timeLeft = endTime - System.currentTimeMillis();
				hasTimeLeft = timeLeft > 0L;
			}
		}
	}

	/**
	 * Shuts the given {@link ExecutorService} down in a non-blocking fashion. The shut down will
	 * be executed by a thread from the common fork-join pool.
	 *
	 * <p>The executor services will be shut down gracefully for the given timeout period. Afterwards
	 * {@link ExecutorService#shutdownNow()} will be called.
	 *
	 * @param timeout          before {@link ExecutorService#shutdownNow()} is called
	 * @param unit             time unit of the timeout
	 * @param executorServices to shut down
	 * @return Future which is completed once the {@link ExecutorService} are shut down
	 */
	public static CompletableFuture <Void> nonBlockingShutdown(long timeout, TimeUnit unit,
															   ExecutorService... executorServices) {
		return CompletableFuture.supplyAsync(
			() -> {
				gracefulShutdown(timeout, unit, executorServices);
				return null;
			});
	}

	private static <T> void joinAllTasks(List <CompletableFuture<T>> tasks) {

		final AtomicReference<Throwable> exception = new AtomicReference <>();

		CompletableFuture
			.allOf(tasks.toArray(new CompletableFuture[0]))
			.exceptionally(
				throwable -> {
					exception.compareAndSet(null, throwable);
					LOG.error("Execute multiple tasks error.", throwable);
					return null;
				}
			)
			.join();

		tasks.clear();

		Throwable throwable = exception.get();

		if (throwable != null) {

			if (throwable instanceof ExceptionWithErrorCode) {
				throw (ExceptionWithErrorCode) throwable;
			}

			throw new AkUnclassifiedErrorException("Run task error.", throwable);
		}
	}

	public static class TaskRunner {

		private final ArrayList <CompletableFuture <Void>> tasks = new ArrayList <>();

		public void submit(Runnable runnable) {
			tasks.add(CompletableFuture.runAsync(runnable, executor));
		}

		public void join() {
			joinAllTasks(tasks);
		}
	}

	public static class IOTaskRunner {

		private final ArrayList <CompletableFuture <Void>> tasks = new ArrayList <>();

		public void submit(Runnable runnable) {
			tasks.add(CompletableFuture.runAsync(runnable, ioExecutor));
		}

		public void join() {
			joinAllTasks(tasks);
		}
	}
}
