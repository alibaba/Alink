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

package com.alibaba.alink.common.io.catalog.datahub.common.sink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by sleepy on 15/12/14.
 * Use OutputFormat in DataStream
 */
public class OutputFormatSinkFunction<RECORD> extends RichSinkFunction<RECORD>
	implements CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(OutputFormatSinkFunction.class);
	private static final long RETRY_INTERVAL = 100;
	private OutputFormat<RECORD> outputFormat;
	private long retryTimeout = 30 * 60 * 1000; // half an hour

	public OutputFormatSinkFunction(OutputFormat<RECORD> outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public void open(Configuration config) throws IOException {
		if (RichOutputFormat.class.isAssignableFrom(outputFormat.getClass())) {
			((RichOutputFormat) outputFormat).setRuntimeContext(getRuntimeContext());
		}
		outputFormat.configure(config);
		outputFormat.open(
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks());
		if (outputFormat instanceof HasRetryTimeout) {
			retryTimeout = ((HasRetryTimeout) outputFormat).getRetryTimeout();
		}
		LOG.info(
			"Initialized OutputFormatSinkFunction of {}/{} task.",
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks());
	}

	@Override
	public void close() throws IOException {
		LOG.info("Closing OutputFormatSinkFunction.");
		outputFormat.close();
	}

	@Override
	public void invoke(RECORD record) throws Exception {
		outputFormat.writeRecord(record);
	}

	public OutputFormat<RECORD> getOutputFormat() {
		return outputFormat;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" + outputFormat.toString();
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (outputFormat instanceof Syncable) {
			long startSyncing = System.currentTimeMillis();
			// Retry until successful
			while (true) {
				try {
					((Syncable) outputFormat).sync();
					break;
				} catch (IOException e) {
					LOG.error("Sync output format failed", e);
					try {
						Thread.sleep(RETRY_INTERVAL);
					} catch (InterruptedException e1) {
						//						throw new RuntimeException(e1);
					}
				}

				long retryingTimeCost = System.currentTimeMillis() - startSyncing;
				if (retryingTimeCost > retryTimeout) {
					throw new IOException(String.format("Retry time exceed timeout Error: %s, %s", retryingTimeCost, retryTimeout));
				}
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}
}
