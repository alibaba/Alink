package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class MapperMTWrapper extends RichFlatMapFunction <Row, Row> {

	private static final long serialVersionUID = -1568535043779354034L;

	public interface SupplierWithException<T> {

		/**
		 * Gets a result.
		 *
		 * @return a result
		 */
		T create() throws Exception;
	}

	public interface FunctionWithException<T, R> {

		/**
		 * Applies this function to the given argument.
		 *
		 * @param t the function argument
		 * @return the function result
		 */
		R apply(T t) throws Exception;
	}

	private static final Logger LOG = LoggerFactory.getLogger(MapperMTWrapper.class);
	private static final int QUEUE_CAPACITY = 32;

	private final int numThreads;
	private final SupplierWithException <FunctionWithException <Row, Row>> supplier;

	private transient List <BlockingQueue <Row>> inputQueues;
	private transient List <BlockingQueue <Row>> outputQueues;
	private transient ExecutorService executorService;
	private transient long numInputRecords = 0L;
	private transient long numOutputRecords = 0L;
	private transient Collector <Row> collector;
	private transient AtomicReference<Throwable> threadException;

	public MapperMTWrapper(int numThreads, SupplierWithException <FunctionWithException <Row, Row>> supplier) {
		this.numThreads = numThreads;
		this.supplier = supplier;
	}

	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		threadException = new AtomicReference <>();

		inputQueues = new ArrayList <>(numThreads);
		outputQueues = new ArrayList <>(numThreads);
		for (int i = 0; i < numThreads; i++) {
			inputQueues.add(new ArrayBlockingQueue <>(QUEUE_CAPACITY));
			outputQueues.add(new ArrayBlockingQueue <>(QUEUE_CAPACITY));
		}

		ThreadFactory namedThreadFactory = new BasicThreadFactory
			.Builder()
			.namingPattern("model-mapper-%d")
			.daemon(true)
			.build();

		executorService = new ThreadPoolExecutor(numThreads, numThreads,
			0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue <>(numThreads),
			namedThreadFactory,
			new ThreadPoolExecutor.AbortPolicy()
		);

		numInputRecords = 0L;
		numOutputRecords = 0L;

		for (int i = 0; i < numThreads; i++) {
			final int tid = i;
			final FunctionWithException <Row, Row> function = this.supplier.create();

			CompletableFuture
				.supplyAsync(() -> {
					BlockingQueue <Row> inputQueue = inputQueues.get(tid);
					BlockingQueue <Row> outputQueue = outputQueues.get(tid);
					boolean exitFlag = false;
					while (true) {
						if (exitFlag) {
							break;
						}
						List <Row> inputs = new ArrayList <>();
						inputQueue.drainTo(inputs);
						for (Row input : inputs) {
							try {
								if (input.getArity() == 0) {
									exitFlag = true;
									outputQueue.put(new Row(0));
								} else {
									Row output = function.apply(input);
									outputQueue.put(output); // blocking put
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

		// put the read & write in a loop to avoid dead lock between write queue and read queue.
		boolean writeSuccess;
		int targetTid = (int) (numInputRecords % numThreads);
		do {
			numOutputRecords += drainRead(outputQueues.get(targetTid), false, null, collector);
			writeSuccess = inputQueues.get(targetTid).offer(Row.copy(value)); // non-blocking write
			if (!writeSuccess) {
				if (!threadException.compareAndSet(null, null)) {
					throw new RuntimeException(threadException.get());
				}

				targetTid = (targetTid + 1) % numThreads;
				Thread.yield();
			} else {
				numInputRecords++;
			}
		} while (!writeSuccess);
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (int i = 0; i < numThreads; i++) {
			//put the read & write in a loop to avoid dead lock between write queue and read queue.
			boolean writeSuccess = false;
			do {
				numOutputRecords += drainRead(outputQueues.get(i), false, null, collector);
				// put an empty row to signal the end of input queue
				writeSuccess = inputQueues.get(i).offer(new Row(0)); // non-blocking write
				if (!writeSuccess) {
					Thread.yield();
				}
			} while (!writeSuccess);
		}
		for (int i = 0; i < numThreads; i++) {
			numOutputRecords += drainRead(outputQueues.get(i), true, threadException, this.collector);
		}

		if (executorService != null) {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executorService);
		}

		if (!threadException.compareAndSet(null, null)) {
			throw new RuntimeException(threadException.get());
		}
	}

	/**
	 * Drain read output queue. An empty row indicates end of queue.
	 *
	 * @param readUntilEOF If true, read til the end of the queue; otherwise read as many as possible.
	 * @param collector    collector of row.
	 */
	private long drainRead(BlockingQueue <Row> queue, boolean readUntilEOF, AtomicReference<Throwable> threadException, Collector <Row> collector)
		throws Exception {
		long numRead = 0L;
		if (readUntilEOF) {
			boolean eof = false;
			do {
				Row result = queue.poll(); // non-blocking read
				if (result == null) {
					if (!threadException.compareAndSet(null, null)) {
						return numRead;
					}
					Thread.yield();
				} else {
					if (result.getArity() > 0) {
						collector.collect(result);
						numRead++;
					} else { // an empty row indicates end of queue
						eof = true;
					}
				}
			} while (!eof);
		} else {
			boolean readSuccess;
			do {
				Row result = queue.poll(); // non-blocking read
				readSuccess = result != null;
				if (readSuccess) {
					collector.collect(result);
					numRead++;
				}
			} while (readSuccess);
		}
		return numRead;
	}
}
