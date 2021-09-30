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

package com.alibaba.flink.ml.tensorflow2.ops.table;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.tensorflow2.util.JavaInferenceUtil;
import com.alibaba.flink.ml.operator.util.TypeUtil;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import com.alibaba.flink.ml.util.MLConstants;
import com.google.common.base.Preconditions;
import io.grpc.Server;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This table function is used for inference with Tensorflow Java APIs.
 */
public class TFTableInferenceJavaFunction extends TableFunction<Row> {

	private static Logger LOG = LoggerFactory.getLogger(TFTableInferenceJavaFunction.class);

	private final BaseRole role;
	private final MLConfig mlConfig;
	private final RowTypeInfo inRowType;
	private final RowTypeInfo outRowType;
	private transient DataExchange<Row, Row> dataExchange;
	private transient Server server;
	private transient MLContext mlContext;
	private transient FutureTask<Void> processFuture;
	private transient long numWritten = 0;
	private transient long numRead = 0;

	public TFTableInferenceJavaFunction(BaseRole role, MLConfig mlConfig, TableSchema inSchema, TableSchema outSchema) {
		this.role = role;
		this.mlConfig = mlConfig;
		inRowType = TypeUtil.schemaToRowTypeInfo(inSchema);
		outRowType = TypeUtil.schemaToRowTypeInfo(outSchema);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		mlContext = new MLContext(ExecutionMode.INFERENCE, mlConfig, role.toString(), -1,
				mlConfig.getEnvPath(), Collections.emptyMap());
		server = JavaInferenceUtil.startTFContextService(mlContext);
		dataExchange = new DataExchange<>(mlContext);

		final Process process = JavaInferenceUtil.launchInferenceProcess(mlContext, inRowType, outRowType);
		processFuture = JavaInferenceUtil.startInferenceProcessWatcher(process, mlContext);
	}

	public void eval(Object... strs) {
		Preconditions.checkArgument(strs.length == inRowType.getArity(), "Input fields length mismatch");
		Preconditions.checkState(!processFuture.isDone(), "Java inference process already finished");
		Row row = new Row(inRowType.getArity());
		for (int i = 0; i < inRowType.getArity(); i++) {
			row.setField(i, strs[i]);
		}
		try {
			// make sure we attempted to read
			drainRead(false);
			while (!dataExchange.write(row)) {
				Preconditions.checkState(!processFuture.isDone(), "Java inference process already finished");
				try {
					processFuture.get(1000, TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					// ignored
				}
				drainRead(false);
			}
			numWritten++;
		} catch (InterruptedIOException e) {
			LOG.info("{} interrupted reading from inference process");
		} catch (IOException e) {
			throw new RuntimeException("Error interacting with Java inference process", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("Java inference process failed", e);
		} catch (InterruptedException e) {
			LOG.info("{} interrupted evaluating rows", mlContext.getIdentity());
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
			Preconditions.checkState(numWritten == numRead, String.format(
					"Wrote %d records to inference process but read %d from it", numWritten, numRead));
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


	@Override
	public String toString() {
		return mlConfig.getProperties().getOrDefault(MLConstants.FLINK_VERTEX_NAME, role.name());
	}

	private void drainRead(boolean readUntilEOF) throws IOException {
		Row row = dataExchange.read(readUntilEOF);
		while (row != null) {
			collect(row);
			numRead++;
			row = dataExchange.read(readUntilEOF);
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return outRowType;
	}
}
