package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class FlatMapperMTWrapper extends RichFlatMapFunction <Row, Row> {

	private static final long serialVersionUID = -1568535043779354034L;

	private static final Logger LOG = LoggerFactory.getLogger(FlatMapperMTWrapper.class);

	private static final int DEFAULT_BUFFER_SIZE = 32;

	private final int numThreads;
	private final int bufferSize;
	private final SupplierWithException <FlatMapper> supplier;

	private transient List <BlockingQueue <Row>> inputQueues;
	private transient List <BlockingQueue <Row>> outputQueues;
	private transient ExecutorService executorService;

	private transient long inputBlockId = 0L;

	private transient Collector <Row> collector;
	private transient AtomicReference <Throwable> threadException;

	private transient List <Row> buffer;

	public FlatMapperMTWrapper(int numThreads, SupplierWithException <FlatMapper> supplier) {
		this(numThreads, DEFAULT_BUFFER_SIZE, supplier);
	}

	public FlatMapperMTWrapper(
		int numThreads, int bufferSize,
		SupplierWithException <FlatMapper> supplier) {

		this.numThreads = numThreads;
		this.bufferSize = bufferSize;
		this.supplier = supplier;
	}

	public interface NeedContext {
		void setContext(RuntimeContext runtimeContext);
	}

	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		threadException = new AtomicReference <>();

		inputQueues = new ArrayList <>(numThreads);
		outputQueues = new ArrayList <>(numThreads);

		buffer = new ArrayList <>(bufferSize);

		for (int i = 0; i < numThreads; i++) {
			inputQueues.add(new LinkedBlockingQueue <>(bufferSize));
			outputQueues.add(new LinkedBlockingQueue <>(bufferSize));
		}

		executorService = new ThreadPoolExecutor(numThreads, numThreads,
			0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue <>(numThreads),
			new BasicThreadFactory
				.Builder()
				.namingPattern("mapper-mt-wrapper-%d")
				.daemon(true)
				.build(),
			new ThreadPoolExecutor.AbortPolicy()
		);

		for (int i = 0; i < numThreads; i++) {
			final int tid = i;
			final FlatMapper flatMapper = this.supplier.create();

			if (flatMapper instanceof NeedContext) {
				((NeedContext) flatMapper).setContext(getRuntimeContext());
			}

			CompletableFuture
				.supplyAsync(() -> {

					BlockingQueue <Row> inputQueue = inputQueues.get(tid);
					BlockingQueue <Row> outputQueue = outputQueues.get(tid);

					List <Row> inputs = new ArrayList <>(bufferSize);
					boolean exitFlag = false;

					while (!exitFlag) {
						inputs.clear();
						inputQueue.drainTo(inputs);
						for (Row input : inputs) {
							try {
								if (input.getArity() == 0) {
									exitFlag = true;
									outputQueue.put(new Row(0));
								} else {
									flatMapper.flatMap(input, new Collector <Row>() {
										@Override
										public void collect(Row record) {
											try {
												outputQueue.put(record);
											} catch (InterruptedException e) {
												throw new RuntimeException(e);
											}
										}

										@Override
										public void close() {
											// pass
										}
									});
								}
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						}
					}

					return null;
				}, executorService)
				.exceptionally(throwable -> {
					threadException.compareAndSet(null, throwable);

					return throwable;
				});
		}
	}

	@Override
	public void flatMap(Row value, Collector <Row> collector) throws Exception {
		this.collector = collector;

		boolean success;
		int targetTid = (int) (inputBlockId % numThreads);
		do {
			collect(outputQueues.get(targetTid), collector);

			if (!threadException.compareAndSet(null, null)) {
				throw new RuntimeException(threadException.get());
			}

			success = inputQueues.get(targetTid).offer(Row.copy(value));
			if (!success) {
				targetTid = (targetTid + 1) % numThreads;
			} else {
				inputBlockId++;
			}

			if (!threadException.compareAndSet(null, null)) {
				throw new RuntimeException(threadException.get());
			}

		} while (!success);
	}

	@Override
	public void close() throws Exception {
		super.close();

		try {

			if (!threadException.compareAndSet(null, null)) {
				throw new RuntimeException(threadException.get());
			}

			boolean[] inputEofStatus = new boolean[numThreads];
			boolean[] outputEofStatus = new boolean[numThreads];
			Arrays.fill(inputEofStatus, false);
			Arrays.fill(outputEofStatus, false);

			int outputEofCnt = 0;

			while (threadException.compareAndSet(null, null) && outputEofCnt < numThreads) {
				for (int i = 0; i < numThreads; i++) {
					if (outputEofStatus[i]) {
						continue;
					}

					if (collect(outputQueues.get(i), this.collector)) {
						outputEofCnt++;
						outputEofStatus[i] = true;
					}

					if (!threadException.compareAndSet(null, null)) {
						break;
					}

					if (!inputEofStatus[i]) {
						inputEofStatus[i] = inputQueues.get(i).offer(new Row(0));
					}
				}
			}

			if (!threadException.compareAndSet(null, null)) {
				throw new RuntimeException(threadException.get());
			}

		} finally {
			if (executorService != null) {
				ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executorService);
			}
		}
	}

	private boolean collect(BlockingQueue <Row> queue, Collector <Row> collector) {

		boolean eof = false;

		buffer.clear();

		long cnt = queue.drainTo(buffer, bufferSize);

		for (int i = 0; i < cnt; ++i) {
			Row row = buffer.get(i);
			if (row.getArity() > 0) {
				collector.collect(row);
			} else {
				eof = true;
			}
		}

		return eof;
	}

	public interface SupplierWithException<T> extends Serializable {

		/**
		 * Gets a result.
		 *
		 * @return a result
		 */
		T create() throws Exception;
	}
}
