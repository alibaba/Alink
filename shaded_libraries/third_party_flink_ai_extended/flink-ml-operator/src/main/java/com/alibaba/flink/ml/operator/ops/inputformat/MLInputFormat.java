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

package com.alibaba.flink.ml.operator.ops.inputformat;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.rpc.AppMasterServer;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.operator.hook.FlinkOpHookManager;
import com.alibaba.flink.ml.operator.ops.ResourcesUtils;
import com.alibaba.flink.ml.operator.util.ColumnInfos;
import com.alibaba.flink.ml.operator.util.PythonFileUtil;
import com.alibaba.flink.ml.cluster.role.AMRole;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * machine learning cluster corresponds to input format.
 * NodeSource class created with InputFormatSourceFunction class and MLInputFormat.
 */
public class MLInputFormat<OUT> extends RichInputFormat<OUT, MLInputSplit> {
	private static Logger LOG = LoggerFactory.getLogger(MLInputFormat.class);

	private MLConfig mlConfig;
	private ExecutionMode mode;
	private BaseRole role;
	private TypeInformation<OUT> outTI;
	private transient FutureTask<Void> serverFuture;
	private transient MLContext mlContext;
	private final AtomicBoolean isClose = new AtomicBoolean(false);
	private transient FlinkOpHookManager hookManager;
	private transient DataExchange<OUT, OUT> dataExchange;

	public MLInputFormat(ExecutionMode mode, BaseRole role, MLConfig config, TypeInformation<OUT> outTI) {
		this.mode = mode;
		this.role = role;
		this.mlConfig = config;
		this.outTI = outTI;
	}

	@Override
	public void configure(Configuration configuration) {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
		return null;
	}

	/**
	 * create flink input split,corresponds to machine learning cluster role.
	 * @param minNumSplits
	 * @return machine learning cluster flink input split.
	 */
	@Override
	public MLInputSplit[] createInputSplits(int minNumSplits) {
		minNumSplits = mlConfig.getRoleParallelismMap().getOrDefault(role.name(), 1);
		MLInputSplit[] inputSplit = new MLInputSplit[minNumSplits];
		for (int i = 0; i < minNumSplits; i++) {
			inputSplit[i] = new MLInputSplit(minNumSplits, i);
		}
		return inputSplit;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(MLInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
//		boolean[] assigned = new boolean[inputSplits.length];
//		return new InputSplitAssigner() {
//			@Override
//			public InputSplit getNextInputSplit(String host, int taskId) {
//				for (int i = 0; i < assigned.length; i++) {
//					if (!assigned[i]){
//						assigned[i] = true;
//						return inputSplits[i];
//					}
//				}
//				return null;
//			}
////			@Override
////			public void returnInputSplit(List<InputSplit> list, int i) {
////
////			}
//		};
	}

	/**
	 * create machine learning cluster application master or node.
	 * @param split
	 * @throws IOException
	 */
	@Override
	public void open(MLInputSplit split) throws IOException {
		ResourcesUtils.parseGpuInfo(getRuntimeContext(), mlConfig);
		mlContext = new MLContext(mode, mlConfig, role.name(), split.getSplitNumber(),
				mlConfig.getEnvPath(), ColumnInfos.dummy().getNameToTypeMap());

		if (role.getClass().equals(AMRole.class)) {
			serverFuture = new FutureTask<>(new AppMasterServer(mlContext), null);
		} else {
			PythonFileUtil.preparePythonFilesForExec(getRuntimeContext(), mlContext);
			serverFuture = new FutureTask<>(new NodeServer(mlContext, role.name()), null);
		}
		try {
			Thread t = new Thread(serverFuture);
			t.setDaemon(true);
			t.setName("NodeServer_" + mlContext.getIdentity());
			t.start();
		} catch (Exception e) {
			LOG.error("Fail to start node service.", e);
			throw new IOException(e.getMessage());
		}
		LOG.info("start: {}", mlContext.getIdentity());
		// we have no data to write
		mlContext.getOutputQueue().markFinished();
		// exec hook open func
		try {
			List<String> hookList = mlContext.getHookClassNames();
			hookManager = new FlinkOpHookManager(hookList);
			hookManager.open();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		this.dataExchange = new DataExchange<>(mlContext);
	}


	@Override
	public boolean reachedEnd() throws IOException {
		return role.getClass().equals(AMRole.class) || serverFuture.isDone() || dataExchange.getRecordReader()
				.isReachEOF();
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		return dataExchange.read(true);
	}

	@Override
	public void close() throws IOException {
		synchronized (isClose) {
			if (!isClose.get()) {
				try {
					if (serverFuture != null && !serverFuture.isCancelled()) {
						serverFuture.get();
					}
				} catch (ExecutionException e) {
					LOG.error(mlContext.getIdentity() + " node server failed {}", e.getMessage());
					throw new IOException(e);
				} catch (InterruptedException e) {
					LOG.warn("{} interrupted during waiting server join {}.", mlContext.getIdentity(), e.getMessage());
					serverFuture.cancel(true);
				} finally {
					serverFuture = null;
					if (mlContext != null) {
						mlContext.close();
						mlContext = null;
					}
				}
				isClose.set(true);
			}
		}
		if (null != hookManager) {
			try {
				hookManager.close();
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
	}
}
