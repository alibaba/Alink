package com.alibaba.alink.operator.common.stream.model;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExecutorUtils;

import com.alibaba.alink.common.concurrent.ExecutorThreadFactory;
import com.alibaba.alink.common.concurrent.FutureUtils;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ReadModelStreamModelRuntimeException;
import com.alibaba.alink.common.exceptions.ScanFailRuntimeException;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileForEachReaderIterable;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ModelStreamFileScanner {

	private static final Logger LOG = LoggerFactory.getLogger(ModelStreamFileScanner.class);

	private final int schedulerPoolSize;
	private final int executorPoolSize;

	private ScheduledExecutorService scheduler;
	private ExecutorService executor;

	public ModelStreamFileScanner(int schedulerPoolSize, int executorPoolSize) {
		this.schedulerPoolSize = schedulerPoolSize;
		this.executorPoolSize = executorPoolSize;
	}

	public void open() {

		scheduler = new ScheduledThreadPoolExecutor(
			schedulerPoolSize,
			new ExecutorThreadFactory()
		);

		executor = new ThreadPoolExecutor(
			executorPoolSize,
			executorPoolSize,
			0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue <>(executorPoolSize),
			new ExecutorThreadFactory(),
			new ThreadPoolExecutor.AbortPolicy()
		);
	}

	public void close() {

		if (scheduler != null) {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, scheduler);
		}

		if (executor != null) {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	public Iterator <Timestamp> scanToFile(final ScanTask scanTask, final Time period) {

		final BlockingQueue <Timestamp> queue = new LinkedBlockingQueue <>();

		commitTask(() -> {
			CompletableFuture
				.supplyAsync(() -> {
					try {
						return scanTask.doScanFile();
					} catch (IOException e) {
						throw new ScanFailRuntimeException("Scan file fail.", e);
					}
				}, executor)
				.thenApplyAsync(scanTask::doUpdateAndRemoveLatest, executor)
				.thenAcceptAsync(
					timestamps -> {
						for (Timestamp timestamp : timestamps) {
							try {
								queue.put(timestamp);
							} catch (InterruptedException e) {
								throw new AkUnclassifiedErrorException("Error. ",e);
							}
						}
					},
					executor
				)
				.exceptionally(
					throwable -> {
						if (throwable instanceof CancellationException) {
							return null;
						}

						LOG.warn("Failed to scan model stream.", throwable);

						return null;
					}
				);
		}, period, scheduler);

		return new Iterator <Timestamp>() {
			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public Timestamp next() {
				try {
					return queue.take();
				} catch (InterruptedException e) {
					throw new AkUnclassifiedErrorException("Error. ",e);
				}
			}
		};
	}

	public void scanToUpdateModel(
		final ScanTask scanTask,
		final Time period,
		final Function <List <Row>, ModelMapper> mapperCreator,
		final AtomicReference <ModelMapper> modelMapperToUpdate) {

		commitTask(() -> {
			CompletableFuture
				.supplyAsync(() -> {
					try {
						return scanTask.doScanFile();
					} catch (IOException e) {
						throw new ScanFailRuntimeException("Scan file fail.", e);
					}
				}, executor)
				.thenApplyAsync(scanTask::doUpdateAndRemoveLatest, executor)
				.thenApplyAsync(timestamps -> {
					try {
						return scanTask.doReadLatestModel(timestamps);
					} catch (IOException e) {
						throw new ReadModelStreamModelRuntimeException("Read model stream fail.", e);
					}
				}, executor)
				.thenAcceptAsync(rows -> scanTask.doUpdateModel(rows, mapperCreator, modelMapperToUpdate), executor)
				.exceptionally(
					throwable -> {
						if (throwable instanceof CancellationException) {
							return null;
						}

						LOG.warn("Failed to scan model stream.", throwable);

						return null;
					}
				);
		}, period, scheduler);

	}

	public static class ScanTask {
		private final FilePath filePath;
		private final Timestamp statTime;

		private Timestamp latest;

		public ScanTask(FilePath filePath, Timestamp startTime) {
			this.filePath = filePath;
			this.statTime = startTime;
		}

		public List <Timestamp> doScanFile() throws IOException {
			return ModelStreamUtils
				.listModels(filePath)
				.stream()
				.filter(timestamp -> timestamp.compareTo(statTime) >= 0)
				.collect(Collectors.toList());
		}

		public synchronized List <Timestamp> doUpdateAndRemoveLatest(List <Timestamp> files) {

			if (files.isEmpty()) {
				return new ArrayList <>();
			}

			final Timestamp localLatest = latest;

			latest = files
				.stream()
				.max(Comparator.naturalOrder())
				.get();

			if (localLatest == null) {
				return files;
			}

			return files
				.stream()
				.filter(timestamp -> timestamp.compareTo(localLatest) > 0)
				.collect(Collectors.toList());
		}

		public List <Row> doReadLatestModel(List <Timestamp> files) throws IOException {

			if (files.isEmpty()) {
				return null;
			}

			final Timestamp localLatest = files
				.stream()
				.max(Comparator.naturalOrder())
				.get();

			FileForEachReaderIterable iterable = new FileForEachReaderIterable();

			BaseFileSystem <?> fileSystem = filePath.getFileSystem();

			Path modelFolderPath = new Path(
				filePath.getPath(), ModelStreamUtils.toStringPresentation(localLatest)
			);

			if (!fileSystem.exists(modelFolderPath)) {
				throw new IllegalStateException("Model " + modelFolderPath.getPath() + " is not exists.");
			}

			AkUtils.getFromFolderForEach(new FilePath(modelFolderPath, fileSystem), iterable);

			List <Row> all = new ArrayList <>();

			for (Row row : iterable) {
				all.add(row);
			}

			return all;
		}

		public void doUpdateModel(
			List <Row> modelRows,
			Function <List <Row>, ModelMapper> mapperCreator,
			AtomicReference <ModelMapper> modelMapperToUpdate) {

			if (modelRows == null) {
				return;
			}

			ModelMapper modelMapper = mapperCreator.apply(modelRows);

			modelMapper.open();

			ModelMapper old = modelMapperToUpdate.getAndSet(modelMapper);

			old.close();
		}
	}

	private static ScheduledFuture <?> commitTask(
		Runnable task, Time period, ScheduledExecutorService scheduledExecutor) {

		return FutureUtils.scheduleAtFixedRate(
			task,
			Time.of(0, period.getUnit()),
			period,
			scheduledExecutor
		);
	}
}
