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

package com.alibaba.flink.ml.tensorflow2.ops;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.tensorflow2.util.JavaInferenceUtil;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import com.google.common.base.Preconditions;
import io.grpc.Server;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * on tensorflow inference mode, input data come from flink up stream and execute tensorflow inference
 * through java process. java process entry class is JavaInferenceRunner.
 */
public class TFJavaInferenceFlatMap extends RichFlatMapFunction<Row, Row>
		implements ResultTypeQueryable<Row>, ListCheckpointed<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(TFJavaInferenceFlatMap.class);

	private final BaseRole role;
	private final MLConfig mlConfig;
	private final RowTypeInfo inTypeInfo;
	private final RowTypeInfo outTypeInfo;
	private transient DataExchange<Row, Row> dataExchange;
	// hold rows that have been fed to this function but haven't been processed by the inference process
	private transient Deque<Row> rowCache;
	private transient Collector<Row> collector;
	private transient Server server;
	private transient MLContext mlContext;
	private transient FutureTask<Void> processFuture;

	public TFJavaInferenceFlatMap(BaseRole role, MLConfig mlConfig, TypeInformation inTypeInfo,
			TypeInformation outTypeInfo) {
		this.role = role;
		this.mlConfig = mlConfig;
		// currently we only support RowTypeInfo
		this.inTypeInfo = (RowTypeInfo) inTypeInfo;
		this.outTypeInfo = (RowTypeInfo) outTypeInfo;
	}

	/**
	 * start tensorflow java inference process.
	 * @param parameters
	 * @throws Exception
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		mlContext = new MLContext(ExecutionMode.INFERENCE, mlConfig, role.toString(),
				getRuntimeContext().getIndexOfThisSubtask(),
				mlConfig.getEnvPath(), Collections.emptyMap());
		server = JavaInferenceUtil.startTFContextService(mlContext);
		dataExchange = new DataExchange<>(mlContext);
		final Process process = JavaInferenceUtil.launchInferenceProcess(mlContext, inTypeInfo, outTypeInfo);
		processFuture = JavaInferenceUtil.startInferenceProcessWatcher(process, mlContext);
		if (rowCache != null) {
			LOG.info("{} replaying {} rows", mlContext.getIdentity(), rowCache.size());
			for (Row row : new ArrayList<>(rowCache)) {
				writeToJavaInference(row);
			}
		} else {
			rowCache = new ArrayDeque<>();
		}
	}

	@Override
	public void close() throws Exception {
		try {
			if (mlContext != null && mlContext.getOutputQueue() != null) {
				mlContext.getOutputQueue().markFinished();
			}
			if (processFuture != null) {
				while (!processFuture.isDone()) {
					drainRead(false);
				}
				processFuture.get();
				drainRead(true);
			}
			if (rowCache != null) {
				Preconditions.checkState(rowCache.isEmpty(),
						String.format("Still got %d unprocessed rows", rowCache.size()));
			}
		} finally {
			if (processFuture != null) {
				processFuture.cancel(true);
			}
			if (mlContext != null) {
				mlContext.close();
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}

	/**
	 * process inference data, send flink data to tensorflow java inference process,and collect inference result data.
	 * @param row need to inference data.
	 * @param collector
	 * @throws Exception
	 */
	@Override
	public void flatMap(Row row, Collector<Row> collector) throws Exception {
		Preconditions.checkArgument(row.getArity() == inTypeInfo.getArity(), "Input fields length mismatch");
		Preconditions.checkState(!processFuture.isDone(), "Java inference process already finished");
		this.collector = collector;
		// make sure we attempted to read
		drainRead(false);
		writeToJavaInference(row);
		rowCache.add(row);
		if (rowCache.size() / 1000 != (rowCache.size() - 1) / 1000) {
			LOG.info("{} Caching {} rows", mlContext.getIdentity(), rowCache.size());
		}
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return outTypeInfo;
	}

	@Override
	public List<Row> snapshotState(long l, long l1) throws Exception {
		return rowCache == null ? Collections.emptyList() : new ArrayList<>(rowCache);
	}

	@Override
	public void restoreState(List<Row> list) throws Exception {
		LOG.info("Restoring from state with {} cached records", list.size());
		// this is called before open()
		if (rowCache == null) {
			rowCache = new ArrayDeque<>();
		}
		rowCache.addAll(list);
	}

	private void drainRead(boolean readUntilEOF) throws IOException {
		Row row = dataExchange.read(readUntilEOF);
		while (row != null) {
			collector.collect(row);
			rowCache.remove();
			row = dataExchange.read(readUntilEOF);
		}
	}

	private void writeToJavaInference(Row row) throws IOException, ExecutionException, InterruptedException {
		while (!dataExchange.write(row)) {
			Preconditions.checkState(!processFuture.isDone(), "Java inference process already finished");
			try {
				processFuture.get(1000, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				// ignored
			}
			drainRead(false);
		}
	}
}
