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

package com.alibaba.flink.ml.tensorflow2.util;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.rpc.NodeClient;
import com.alibaba.flink.ml.cluster.rpc.RpcCode;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.proto.ContextResponse;
import com.google.common.base.Preconditions;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.ml.tensorflow2.util.TFConstants.INPUT_TF_EXAMPLE_CONFIG;
import static com.alibaba.flink.ml.tensorflow2.util.TFConstants.OUTPUT_TF_EXAMPLE_CONFIG;

/**
 * start java process for tensorflow inference.
 */
public class JavaInferenceRunner implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(JavaInferenceRunner.class);
	private static final Configuration HADOOP_CONF = new Configuration();

	private volatile boolean inputFinished = false;

	private final int batchSize;
	private final BlockingQueue<Row> batchCache;
	private final NodeClient nodeClient;
	private final MLContext mlContext;
	private final JavaInference javaInference;
	private transient DataExchange<Row, Row> dataExchange;

	/**
	 * @param tfIP IP of the TF node
	 * @param tfPort Port of the TF node
	 * @param inRowTypePath Path to the serialized input RowType
	 * @param outRowTypePath Path to the serialized output RowType
	 */
	JavaInferenceRunner(String tfIP, int tfPort, String inRowTypePath, String outRowTypePath) throws Exception {
		nodeClient = new NodeClient(tfIP, tfPort);
		ContextResponse response = nodeClient.getMLContext();
		Preconditions.checkState(response.getCode() == RpcCode.OK.ordinal(), "Failed to get TFContext");
		mlContext = MLContext.fromPB(response.getContext());
		RowTypeInfo inRowType = readRowType(new Path(inRowTypePath));
		RowTypeInfo outRowTpe = readRowType(new Path(outRowTypePath));
		javaInference = new JavaInference(mlContext.getProperties(), inRowType.getFieldNames(),
				outRowTpe.getFieldNames());
		batchSize = Integer.valueOf(mlContext.getProperties().getOrDefault(TFConstants.TF_INFERENCE_BATCH_SIZE, "1"));
		LOG.info("{} java inference with batch size {}", mlContext.getIdentity(), batchSize);
		batchCache = new ArrayBlockingQueue<>(batchSize);
		// reverse coding config
		String input = mlContext.getProperties().get(INPUT_TF_EXAMPLE_CONFIG);
		String output = mlContext.getProperties().get(OUTPUT_TF_EXAMPLE_CONFIG);
		mlContext.getProperties().put(INPUT_TF_EXAMPLE_CONFIG, output);
		mlContext.getProperties().put(OUTPUT_TF_EXAMPLE_CONFIG, input);
		dataExchange = new DataExchange<>(mlContext);
	}

	/**
	 * start read input date and write output data thread.
	 * @throws Exception
	 */
	private void run() throws Exception {
//		FutureTask<Void> inputConsumer = new FutureTask<>(new InputRowConsumer(), null);
//		Thread thread = new Thread(inputConsumer);
//		thread.setName(mlContext.getIdentity() + "-" + InputRowConsumer.class.getSimpleName());
//		thread.setDaemon(true);
//		thread.start();
//
//		FutureTask<Void> outputProducer = new FutureTask<>(new OutputRowProducer(), null);
//		thread = new Thread(outputProducer);
//		thread.setName(mlContext.getIdentity() + "-" + OutputRowProducer.class.getSimpleName());
//		thread.setDaemon(true);
//		thread.start();
//
//		try {
//			inputConsumer.get();
//			outputProducer.get();
//		} catch (InterruptedException e) {
//			LOG.info("{} interrupted", JavaInferenceRunner.class.getSimpleName());
//			inputConsumer.cancel(true);
//			outputProducer.cancel(true);
//		}

		final ExecutorService inputExecutor = Executors.newFixedThreadPool(1, r -> {
			Thread runnerThread = new Thread(r);
			runnerThread.setDaemon(true);
			runnerThread.setName(mlContext.getIdentity() + "-" + InputRowConsumer.class.getSimpleName());
			return runnerThread;
		});

		final ExecutorService outputExecutor = Executors.newFixedThreadPool(1, r -> {
			Thread runnerThread = new Thread(r);
			runnerThread.setDaemon(true);
			runnerThread.setName(mlContext.getIdentity() + "-" + OutputRowProducer.class.getSimpleName());
			return runnerThread;
		});
		Future inputConsumer = inputExecutor.submit(new InputRowConsumer());
		Future outputConsumer = outputExecutor.submit(new OutputRowProducer());
		try {
			inputConsumer.get();
			outputConsumer.get();
		}catch (InterruptedException | ExecutionException e){
			inputConsumer.cancel(true);
			outputConsumer.cancel(true);
		}finally {
			inputExecutor.shutdownNow();
			if(!inputExecutor.isTerminated()){
				inputExecutor.awaitTermination(1, TimeUnit.SECONDS);
			}
			outputExecutor.shutdownNow();
			if(!outputExecutor.isTerminated()){
				outputExecutor.awaitTermination(1, TimeUnit.SECONDS);
			}
		}


	}

	private RowTypeInfo readRowType(Path path) throws IOException, ClassNotFoundException {
		FileSystem fs = path.getFileSystem(HADOOP_CONF);
		try (ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(path))) {
			return (RowTypeInfo) objectInputStream.readObject();
		}
	}

	@Override
	public void close() throws IOException {
		if (nodeClient != null) {
			nodeClient.close();
		}
		if (mlContext != null) {
			mlContext.close();
		}
		if (javaInference != null) {
			javaInference.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Preconditions.checkArgument(args.length == 3, "Takes three arguments, got " + Arrays.toString(args));
		String[] ipPort = args[0].split(":");
		Preconditions.checkArgument(ipPort.length == 2,
				String.format("Invalid tf node address %s, please specify in form <IP>:<PORT>", args[0]));
		JavaInferenceRunner runner = new JavaInferenceRunner(ipPort[0], Integer.valueOf(ipPort[1]), args[1], args[2]);
		runner.run();
		runner.close();
	}

	private class InputRowConsumer implements Runnable {

		private long read = 0;

		@Override
		public void run() {
			try {
				while (true) {
					Row row = dataExchange.read(true);
					if (row == null) {
						LOG.info("{} Input rows depleted", mlContext.getIdentity());
						break;
					}
					read++;
					if (read % 1000 == 0) {
						LOG.info("{} Read {} rows from flink", mlContext.getIdentity(), read);
					}
					batchCache.put(row);
				}
				LOG.info("{} Read totally {} rows from flink", mlContext.getIdentity(), read);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				LOG.info("{} interrupted", Thread.currentThread().getName());
			} finally {
				inputFinished = true;
			}
		}
	}

	private class OutputRowProducer implements Runnable {

		private static final long INTERVAL = 1000;

		private final List<Object[]> batch = new ArrayList<>(batchSize);

		private long written = 0;

		@Override
		public void run() {
			try {
				while (!inputFinished) {
					Row inputRow = batchCache.poll(INTERVAL, TimeUnit.MILLISECONDS);
					if (inputRow != null) {
						batch.add(rowToObjects(inputRow));
						while (batch.size() >= batchSize) {
							outputRows();
						}
					}
				}
				LOG.info("{} Flush remaining {} records", mlContext.getIdentity(), batchCache.size() + batch.size());
				while (!batchCache.isEmpty()) {
					batch.add(rowToObjects(batchCache.remove()));
				}
				while (!batch.isEmpty()) {
					outputRows();
				}
				LOG.info("{} Written totally {} rows to flink", mlContext.getIdentity(), written);
			} catch (InterruptedException e) {
				LOG.info("{} interrupted", Thread.currentThread().getName());
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				LOG.info("Closing output queue");
				mlContext.getOutputQueue().markFinished();
			}
		}

		private Object[] rowToObjects(Row row) {
			Object[] res = new Object[row.getArity()];
			for (int i = 0; i < res.length; i++) {
				res[i] = row.getField(i);
			}
			return res;
		}

		private void outputRows() throws IOException, InterruptedException {
			Row[] rows = javaInference.generateRowsOneBatch(batch, batchSize);
			for (Row row : rows) {
				batch.remove(0);
				while (!dataExchange.write(row)) {
					Thread.sleep(INTERVAL);
				}
				written++;
				if (written % 1000 == 0) {
					LOG.info("{} Written {} rows to flink", mlContext.getIdentity(), written);
				}
			}
		}
	}
}
