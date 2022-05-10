/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.reader;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.alibaba.alink.common.io.catalog.datahub.common.Constants;
import com.alibaba.alink.common.io.catalog.datahub.common.MetricUtils;
import com.alibaba.alink.common.io.catalog.datahub.common.metrics.SumAndCount;
import com.alibaba.alink.common.io.catalog.datahub.common.source.WatermarkProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by sleepy on 16/1/11.
 *
 * @param <OUT>    the type parameter
 * @param <CURSOR> the type parameter
 */
public class ParallelReader<OUT, CURSOR extends Serializable>
		implements WatermarkProvider {
	private static final Logger LOG = LoggerFactory.getLogger(ParallelReader.class);
	private static final String SPLIT_PIPE_LEN_CONFIG = Constants.CONFIG_PREFIX + "source.buffer-len";
	private static final String IDLE_INTERVAL_CONFIG = Constants.CONFIG_PREFIX + "source.idle-interval";
	private static final long STOP_WAITING = 5;
	private long idleInterval = 10;
	private int splitPipeLen = 10;

	private ExecutorService readerPool = Executors.newCachedThreadPool();
	private BlockingQueue<ReaderRunner<OUT, CURSOR>> readerRunners = new LinkedBlockingQueue<>();
	private WatermarkEmitter<OUT> watermarkEmitter = null;
	private volatile boolean stop = false;
	private volatile boolean exitAfterReadFinished = false;
	private long watermarkInterval;
	private RuntimeContext context;
	private Configuration config;
	private Counter outputCounter;
	private Meter tpsMetric;
	private boolean tracingMetricEnabled;
	private SumAndCount partitionLatency;
	private SumAndCount processLatency;
	private Gauge<Integer> partitionCount;
	private int sampleInterval;
	private long inputCount;

	private transient Map<InputSplit, CURSOR> exitedReadRunnerSplitCursor = new HashMap<>();

	public ParallelReader(
			RuntimeContext context, Configuration config,
			long watermarkInterval, boolean tracingMetricEnabled, int sampleInterval) {
		this.context = context;
		this.config = config;
		this.watermarkInterval = watermarkInterval;
		splitPipeLen = config.getInteger(SPLIT_PIPE_LEN_CONFIG, 10);
		idleInterval = config.getInteger(IDLE_INTERVAL_CONFIG, 10);
		LOG.info("idleInterval:" + idleInterval);
		LOG.info("splitPipeLen:" + splitPipeLen);

		context.getMetricGroup().gauge(MetricUtils.METRICS_DELAY, new DelayGauge(readerRunners, DelayKind.DELAY));
		context.getMetricGroup().gauge(MetricUtils.METRICS_FETCHED_DELAY, new
				DelayGauge(readerRunners, DelayKind.FETCHED_DELAY));
		context.getMetricGroup().gauge(MetricUtils.METRICS_NO_DATA_DELAY, new
				DelayGauge(readerRunners, DelayKind.NO_DATA_DELAY));
		outputCounter = context.getMetricGroup().counter(MetricUtils.METRICS_TPS + "_counter", new SimpleCounter());
		tpsMetric = context.getMetricGroup().meter(MetricUtils.METRICS_TPS, new MeterView(outputCounter, 60));

		this.tracingMetricEnabled = tracingMetricEnabled;
		this.sampleInterval = sampleInterval;
		if (this.tracingMetricEnabled) {
			partitionLatency = new SumAndCount(MetricUtils.METRICS_SOURCE_PARTITION_LATENCY, context.getMetricGroup());
			processLatency = new SumAndCount(MetricUtils.METRICS_SOURCE_PROCESS_LATENCY, context.getMetricGroup());
			partitionCount = context.getMetricGroup().gauge(MetricUtils.METRICS_SOURCE_PARTITION_COUNT, new Gauge<Integer>() {
				@Override
				public Integer getValue() {
					int count = 0;
					for (ReaderRunner runner : readerRunners) {
						if (!runner.finished) {
							count++;
						}
					}
					return count;
				}
			});
		}
	}

	public ParallelReader<OUT, CURSOR> setExitAfterReadFinished(boolean exitAfterReadFinished) {
		this.exitAfterReadFinished = exitAfterReadFinished;
		return this;
	}

	/**
	 * Add record reader.
	 *
	 * @param recordReader the record reader
	 * @param split        the split
	 * @throws IOException the io exception
	 */
	public void addRecordReader(
			RecordReader<OUT, CURSOR> recordReader,
			InputSplit split,
			CURSOR cursor)
			throws IOException {
		ReaderRunner<OUT, CURSOR> readerRunner = new ReaderRunner<>(recordReader, split, cursor,
																	context, splitPipeLen, idleInterval, tracingMetricEnabled, sampleInterval);
		readerRunners.add(readerRunner);
	}

	/**
	 * Run.
	 *
	 * @param ctx the ctx
	 * @throws Exception the exception
	 */
	public void run(SourceFunction.SourceContext<OUT> ctx) throws Exception {
		try {
			submitReaders();
			runWatermarkEmitter(ctx);
			runImpl(ctx);
		} catch (InterruptedException e) {
			LOG.error("ParallelReader was interrupted: ", e);
		} catch (Throwable e) {
			LOG.error("ParallelReader caught exception: ", e);
			throw new RuntimeException(e);
		} finally {
			close();
		}
	}

	private void runWatermarkEmitter(SourceFunction.SourceContext<OUT> ctx) {
		if (watermarkInterval > 0) {
			watermarkEmitter = new WatermarkEmitter<>(this, watermarkInterval, ctx);
			readerPool.submit(watermarkEmitter);
		}
	}

	/**
	 * Stop externally.
	 */
	public void stop() {
		stop = true;
	}

	public void close() {
		boolean released = false;
		try {
			for (ReaderRunner runner : readerRunners) {
				runner.stop();
			}
			if (watermarkEmitter != null) {
				watermarkEmitter.stop();
			}
			try {
				// Wait for reader stopping
				Thread.sleep(100 * idleInterval);
			} catch (InterruptedException e) {
				LOG.warn("Waiting for reader stopping is interrupted.", e);
			}

			readerPool.shutdownNow();
			released = readerPool.awaitTermination(STOP_WAITING, TimeUnit.SECONDS);

			if (!released) {
				for (ReaderRunner runner : readerRunners) {
					if (!runner.isStopped()) {
						LOG.info("Can not stop reader for split {}, it is stuck in method: \n {}.",
								runner.getSplit(), runner.getStackTrace());
					}
				}
			}
		} catch (Throwable e) {
			LOG.warn(e.toString());
		} finally {
			if (!released) {
				LOG.error("Shut down reader pool failed, exit process!");
				System.exit(1);
			}
			LOG.info("Stopped all split reader.");
		}
	}

	/**
	 * Run.
	 *
	 * @param ctx the ctx
	 * @throws Exception the exception
	 */
	protected void runImpl(SourceFunction.SourceContext<OUT> ctx) throws Exception {
		while (!stop && !readerRunners.isEmpty()) {
			Iterator<ReaderRunner<OUT, CURSOR>> it = readerRunners.iterator();
			boolean idle = true;
			// Gather record from all splits
			while (it.hasNext()) {
				ReaderRunner<OUT, CURSOR> readerRunner = it.next();
				// If SplitReader failed, nothing we can do but throw exception to upper layer
				if (readerRunner.isStopped() && readerRunner.getCause() != null) {
					LOG.error(String.format(
							"SplitReader for split[%d][%s] failed, cause: %s",
							readerRunner.getSplit().getSplitNumber(),
							readerRunner.getSplit().toString(),
							readerRunner.getCause()));
					throw new RuntimeException(readerRunner.getCause());
				}
				// Some SplitReader is exhausted, just remove it
				if (readerRunner.isExhausted()) {
					LOG.info(String.format(
							"SplitReader for split[%d][%s] finished",
							readerRunner.getSplit().getSplitNumber(),
							readerRunner.getSplit().toString()));
					exitedReadRunnerSplitCursor.put(readerRunner.getSplit(), readerRunner.getProgress());
					it.remove();
				} else {
					if (readerRunner.hasRecord()) {
						idle = false;
						inputCount++;
						if (tracingMetricEnabled && inputCount % sampleInterval == 0) {
							long now = System.nanoTime();
							processRecord(ctx, readerRunner);
							processLatency.update(System.nanoTime() - now);
						} else {
							processRecord(ctx, readerRunner);
						}
					}
				}
			}
			// If all pipes have no data, sleep for a while
			if (idle) {
				Thread.sleep(idleInterval);
			}
		}
		ctx.markAsTemporarilyIdle();
		LOG.info(String.format("This subTask [%d]/[%d] has finished, idle...", context.getIndexOfThisSubtask(),
							context.getNumberOfParallelSubtasks()));
		// Avoid the finish of this subtask causing the cp can not do normally
		while (!stop && !exitAfterReadFinished) {
			Thread.sleep(1000);
		}
	}

	private void processRecord(SourceFunction.SourceContext<OUT> ctx, ReaderRunner<OUT, CURSOR> readerRunner) {
		synchronized (ctx.getCheckpointLock()) {
			Tuple3<OUT, Long, Long> record = readerRunner.pollRecord();
			if (record != null) {
				ctx.collectWithTimestamp(record.f0, record.f1);
				tpsMetric.markEvent();

				if (record.f2 > 0) {
					partitionLatency.update(record.f2);
				}
			}
		}
	}

	public Progress<CURSOR> getProgress() throws IOException {
		Progress<CURSOR> progress = new Progress<>();
		for (ReaderRunner<OUT, CURSOR> readerRunner : readerRunners) {
			progress.addProgress(readerRunner.getSplit(), readerRunner.getProgress());
		}
		// record the progress of readOnly Shard, avoid read repetition data
		for (Map.Entry<InputSplit, CURSOR> entry:exitedReadRunnerSplitCursor.entrySet()){
			progress.addProgress(entry.getKey(), entry.getValue());
		}
		return progress;
	}

	/**
	 * Submit readers.
	 */
	protected void submitReaders() {
		for (ReaderRunner runner : readerRunners) {
			readerPool.submit(runner);
			LOG.info(String.format(
					"Start split reader for split[%d][%s]",
					runner.getSplit().getSplitNumber(),
					runner.getSplit().toString()));
		}
	}

	@Override
	public long getWatermark() {
		long watermark = Long.MAX_VALUE;
		Iterator<ReaderRunner<OUT, CURSOR>> iter = readerRunners.iterator();
		while (iter.hasNext()) {
			ReaderRunner<OUT, CURSOR> readerRunner = iter.next();
			watermark = watermark < readerRunner.getWatermark() ? watermark : readerRunner
					.getWatermark();
		}
		return watermark;
	}


	/**
	 * The type Reader runner.
	 *
	 * @param <OUT>    the type parameter
	 * @param <CURSOR> the type parameter
	 */
	protected static class ReaderRunner<OUT, CURSOR extends Serializable>
			implements Runnable, WatermarkProvider, Comparable<ReaderRunner<OUT, CURSOR>> {
		private final RuntimeContext runtimeContext;
		private final RecordReader<OUT, CURSOR> recordReader;
		private BlockingDeque<Tuple5<OUT, Long, Long, CURSOR, Long>> splitPipe;
		private InputSplit split;
		private volatile long watermark = Long.MIN_VALUE;
		private volatile CURSOR progress = null;
		private volatile boolean stop = false;
		private volatile boolean stopped = false;
		private volatile boolean finished = false;
		private volatile Throwable cause;
		private volatile Thread currentThread;
		private final int splitPipeLen;
		private final long idleInterval;
		private final boolean isTracingMetricEnabled;
		private final int tracingInterval;
		private long outputCount = 0;
		private long traceStart;

		/**
		 * Instantiates a new Reader runner.
		 *
		 * @param recordReader the record reader
		 * @param split        the split
		 */
		public ReaderRunner(
				RecordReader<OUT, CURSOR> recordReader,
				InputSplit split,
				CURSOR cursor,
				RuntimeContext context,
				int splitPipeLen,
				long idleInterval,
				boolean isTracingMetricEnabled,
				int tracingInterval) {
			this.recordReader = recordReader;
			this.split = split;
			this.progress = cursor;
			this.runtimeContext = context;
			this.splitPipeLen = splitPipeLen;
			this.idleInterval = idleInterval;
			this.isTracingMetricEnabled = isTracingMetricEnabled;
			this.tracingInterval = tracingInterval;
			splitPipe = new LinkedBlockingDeque<>(splitPipeLen);
		}

		/**
		 * Gets split.
		 *
		 * @return the split
		 */
		public InputSplit getSplit() {
			return split;
		}

		@Override
		public void run() {
			try {
				recordReader.open(split, runtimeContext);
				if (progress != null) {
					LOG.info("init progress not null, seek {}", progress);
					recordReader.seek(progress);
				}
				currentThread = Thread.currentThread();
				while (!stop && !finished) {
					if (!isTracingMetricEnabled) {
						getNextMessage();
					} else {
						traceStart = System.nanoTime();
						if (outputCount % tracingInterval == 0) {
							getNextMessageWithTracing();
						} else {
							getNextMessage();
						}
						outputCount++;
					}
				}
			} catch (InterruptedException e) {
				// If InterruptedException is not caused by canceling, mark this reader as failed
				if (!stop) {
					cause = e;
				}
				LOG.info("Split reader " + split + " is interrupted.", e);
			} catch (Throwable e) {
				cause = e;
				LOG.error("Split reader " + split + " is failed cause: ", e);
			} finally {
				try {
					if (recordReader != null) {
						LOG.info("Closing split {} reader runner.", split);
						recordReader.close();
					}
				} catch (Throwable e) {
					LOG.error("Exception caught in closing record reader", e);
					if (cause == null) {
						cause = e;
					}
				}
				stopped = true;
				LOG.info("Split reader {} thread will exit.", split);
			}
		}

		private void getNextMessageWithTracing() throws IOException, InterruptedException {
			while (!stop && !finished) {
				finished = !recordReader.next();
				if (!finished) {
					if (!recordReader.isHeartBeat()) {
						put(recordReader.getMessage(), System.nanoTime() - traceStart);
						break;
					} else {
						updateWatermarkAndProgress(
								recordReader.getWatermark(),
								recordReader.getProgress());
					}
				} else {
					// If no data continuous, sleep for a while
					LOG.info("Finishing Split {}.", split);
				}
			}
		}

		private void getNextMessage() throws IOException, InterruptedException {
			while (!stop && !finished) {
				finished = !recordReader.next();
				if (!finished) {
					if (!recordReader.isHeartBeat()) {
						put(recordReader.getMessage(), 0);
						break;
					} else {
						updateWatermarkAndProgress(
								recordReader.getWatermark(),
								recordReader.getProgress());
					}
				} else {
					// If no data continuous, sleep for a while
					LOG.info("Finishing Split {}.", split);
				}
			}
		}

		protected void put(OUT record, long latencyInNanos) throws IOException, InterruptedException {
			Tuple5<OUT, Long, Long, CURSOR, Long> tuple = new Tuple5<>(
					record,
					recordReader.getWatermark(),
					recordReader.getWatermark(),
					recordReader.getProgress(),
					latencyInNanos);
			while (!stop) {
				if (splitPipe.offer(tuple, idleInterval, TimeUnit.MILLISECONDS)) {
					break;
				}
			}
		}

		/**
		 * Stop.
		 */
		public void stop() {
			stop = true;
			if (recordReader instanceof Interruptible) {
				((Interruptible) recordReader).interrupt();
			}
		}

		/**
		 * Is stopped boolean.
		 *
		 * @return the boolean
		 */
		public boolean isStopped() {
			return stopped;
		}

		public boolean isExhausted() {
			return stop || (finished && splitPipe.isEmpty());
		}

		/**
		 * Gets cause.
		 *
		 * @return the cause
		 */
		public Throwable getCause() {
			return cause;
		}

		public synchronized void updateWatermarkAndProgress(long watermark, CURSOR progress) {
			Tuple5<OUT, Long, Long, CURSOR, Long> current = splitPipe.peekLast();
			if (current != null) {
				current.f2 = watermark;
				current.f3 = progress;
			} else {
				this.watermark = watermark;
				this.progress = progress;
			}
		}

		public boolean hasRecord() {
			return !splitPipe.isEmpty();
		}

		public synchronized Tuple3<OUT, Long, Long> pollRecord() {
			Tuple5<OUT, Long, Long, CURSOR, Long> current = splitPipe.poll();
			if (current == null) {
				return null;
			}
			this.watermark = current.f2;
			this.progress = current.f3;
			return new Tuple3<>(current.f0, current.f1, current.f4);
		}

		public synchronized CURSOR getProgress() throws IOException {
			return progress;
		}

		@Override
		public synchronized long getWatermark() {
			return this.watermark;
		}

		@Override
		public int compareTo(ReaderRunner<OUT, CURSOR> o) {
			return Long.compare(this.getWatermark(), o.getWatermark());
		}

		public String getStackTrace() {
			if (currentThread == null || !currentThread.isAlive()) {
				return "No stack trace, maybe thread already exited.";
			}
			StringBuilder sb = new StringBuilder();
			StackTraceElement[] stack = currentThread.getStackTrace();
			for (StackTraceElement e : stack) {
				sb.append(e).append('\n');
			}
			return sb.toString();
		}

		/**
		 * Get recordReader's last successful message's timestamp. If no successful message, return Long.MIN_VALUE.
		 *
		 * @return the timestamp
		 */
		public long getDelay(){
			return recordReader.getDelay();
		}

		public long getFetchedDelay(){
			return recordReader.getFetchedDelay();
		}

	}

	/**
	 * WatermarkEmitter.
	 * @param <OUT>
	 */
	protected static class WatermarkEmitter<OUT> implements Runnable {
		private volatile boolean stopped = false;
		private ParallelReader provider;
		private SourceFunction.SourceContext<OUT> ctx;
		private long watermarkInterval;

		public WatermarkEmitter(
				ParallelReader provider,
				long watermarkInterval,
				SourceFunction.SourceContext<OUT> ctx) {
			this.provider = provider;
			this.ctx = ctx;
			this.watermarkInterval = watermarkInterval;
		}

		@Override
		public void run() {
			long lastWatermark = 0;
			while (!stopped) {
				Watermark nextWatermark = new Watermark(provider.getWatermark());
				if (lastWatermark != nextWatermark.getTimestamp()) {
					lastWatermark = nextWatermark.getTimestamp();
					ctx.emitWatermark(nextWatermark);
				}
				try {
					Thread.sleep(this.watermarkInterval);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

		public void stop() {
			this.stopped = true;
		}
	}

	/**
	 * progress.
	 * @param <CURSOR>
	 */
	public static class Progress<CURSOR extends Serializable> implements Serializable {
		private Map<InputSplit, CURSOR> splitProgress = new HashMap<>();

		public void addProgress(InputSplit split, CURSOR cursor) {
			if (cursor != null) {
				splitProgress.put(split, cursor);
			}
		}

		public Map<InputSplit, CURSOR> getProgress() {
			return splitProgress;
		}
	}

	/**
	 * DelayGauge calculate delay of each source partition, which will be reported to master and
	 * displayed on WebUI and other metric system.
	 */
	public enum DelayKind{
		DELAY,
		FETCHED_DELAY,
		NO_DATA_DELAY
	}

	/**
	 * DelayGauge to collect delay info.
	 */
	public class DelayGauge implements Gauge<Long> {
		private final ConcurrentHashMap<Integer, Long> delayStats = new ConcurrentHashMap<>();
		private BlockingQueue<ReaderRunner<OUT, CURSOR>> readerRunners;
		private DelayKind delayKind;

		public DelayGauge(BlockingQueue<ReaderRunner<OUT, CURSOR>> readerRunners, DelayKind delayKind) {
			this.readerRunners = readerRunners;
			this.delayKind = delayKind;
		}

		@Override
		public Long getValue() {
			if (null != readerRunners) {
				delayStats.clear();
				for (ReaderRunner runner : readerRunners) {
					switch (delayKind) {
						case DELAY:
							if (runner.getDelay() > 0) {
								delayStats.put(runner.getSplit().getSplitNumber(),  System.currentTimeMillis() - runner
										.getDelay());
							}
							break;
						case FETCHED_DELAY:
							if (runner.getFetchedDelay() > 0) {
								delayStats.put(runner.getSplit().getSplitNumber(), runner.getFetchedDelay());
							}
							break;
						case NO_DATA_DELAY:
							if (runner.getDelay() > 0 && runner.getFetchedDelay() > 0) {
								long delay = System.currentTimeMillis() - runner.getDelay();
								long noDataDelay = delay - runner.getFetchedDelay();
								// add 10s to avoid report no data reported mistakenly
								if (noDataDelay > 10000) {
									delayStats.put(runner.getSplit().getSplitNumber(), noDataDelay);
								} else {
									delayStats.put(runner.getSplit().getSplitNumber(), 0L);
								}
							}
							break;
					}
				}
			}
			while (true) {
				try {
					long ret = 0;
					for (Map.Entry<Integer, Long> delayState : delayStats.entrySet()) {
						if (ret < delayState.getValue()) {
							ret = delayState.getValue();
						}
					}
					return ret;
					// Concurrent access onto the "delayStats" map could cause
					// ConcurrentModificationExceptions. To avoid unnecessary blocking
					// of the reportLatency() method, we retry this operation until
					// it succeeds.
				} catch (ConcurrentModificationException ignore) {
					LOG.debug("Unable to report delay statistics", ignore);
				}
			}

		}
	}

}
