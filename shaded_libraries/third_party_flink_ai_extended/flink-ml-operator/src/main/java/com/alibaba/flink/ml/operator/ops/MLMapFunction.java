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

package com.alibaba.flink.ml.operator.ops;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.operator.util.PythonFileUtil;
import com.alibaba.flink.ml.cluster.role.BaseRole;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * machine learning node has input and output,MLMapFunction is a util function to help create MLFlatMapOp class object.
 * @param <IN> machine learning node input class.
 * @param <OUT> machine learning node output class.
 */
public class MLMapFunction<IN, OUT> implements Closeable, Serializable {
	private BaseRole role;
	private MLConfig config;
	private TypeInformation<IN> inTI;
	private TypeInformation<OUT> outTI;
	private MLContext mlContext;
	private FutureTask<Void> serverFuture;
	private ExecutionMode mode;
	private transient DataExchange<IN, OUT> dataExchange;
	private volatile Collector<OUT> collector = null;

	private static final Logger LOG = LoggerFactory.getLogger(MLMapFunction.class);

	public MLMapFunction(ExecutionMode mode, BaseRole role, MLConfig config, TypeInformation<IN> inTI,
			TypeInformation<OUT> outTI) {
		this.mode = mode;
		this.role = role;
		this.config = config;
		this.outTI = outTI;
		this.inTI = inTI;
	}

	/**
	 * create machine learning node and data exchange object.
	 * @param runtimeContext flink operator RuntimeContext.
	 * @throws Exception
	 */
	public void open(RuntimeContext runtimeContext) throws Exception {
		ResourcesUtils.parseGpuInfo(runtimeContext, config);
		mlContext = new MLContext(mode, config, role.name(), runtimeContext.getIndexOfThisSubtask(),
				config.getEnvPath(), null);
		PythonFileUtil.preparePythonFilesForExec(runtimeContext, mlContext);

		dataExchange = new DataExchange<>(mlContext);

		try {
			serverFuture = new FutureTask<>(new NodeServer(mlContext, role.name()), null);
			Thread t = new Thread(serverFuture);
			t.setDaemon(true);
			t.setName("NodeServer_" + mlContext.getIdentity());
			t.start();
		} catch (Exception e) {
			LOG.error("Fail to start node service.", e);
			throw new IOException(e.getMessage());
		}
		System.out.println("start:" + mlContext.getRoleName() + " index:" + mlContext.getIndex());
	}

	/**
	 * stop machine learning node and resource.
	 */
	@Override
	public void close() {
		if (mlContext != null && mlContext.getOutputQueue() != null) {
			mlContext.getOutputQueue().markFinished();
		}

		// wait for tf thread finish
		try {
			if (serverFuture != null && !serverFuture.isCancelled()) {
				serverFuture.get();
			}
			//as in batch mode, we can't user timer to drain queue, so drain it here
			drainRead(collector, true);
		} catch (InterruptedException e) {
			LOG.error("Interrupted waiting for server join {}.", e.getMessage());
			serverFuture.cancel(true);
		} catch (ExecutionException e) {
			LOG.error(mlContext.getIdentity() + " node server failed");
			throw new RuntimeException(e);
		} finally {
			serverFuture = null;

			LOG.info("Records output: " + dataExchange.getReadRecords());

			if (mlContext != null) {
				try {
					mlContext.close();
				} catch (IOException e) {
					LOG.error("Fail to close mlContext.", e);
				}
				mlContext = null;
			}
		}
	}

	/**
	 * process input data and collect results.
	 * @param value input object.
	 * @param out output result.
	 * @throws Exception
	 */
	void flatMap(IN value, Collector<OUT> out) throws Exception {
		collector = out;

		//put the read & write in a loop to avoid dead lock between write queue and read queue.
		boolean writeSuccess = false;
		do {
			drainRead(out, false);

			writeSuccess = dataExchange.write(value);
			if (!writeSuccess) {
				Thread.yield();
			}
		} while (!writeSuccess);
	}

	public TypeInformation<OUT> getProducedType() {
		return outTI;
	}

	private void drainRead(Collector<OUT> out, boolean readUntilEOF) {
		while (true) {
			try {
				Object r = dataExchange.read(readUntilEOF);
				if (r != null) {
					out.collect((OUT) r);
				} else {
					break;
				}
			} catch (InterruptedIOException iioe) {
				LOG.info("{} Reading from is interrupted, canceling the server", mlContext.getIdentity());
				serverFuture.cancel(true);
			} catch (IOException e) {
				LOG.error("Fail to read data from python.", e);
			}
		}
	}
}
