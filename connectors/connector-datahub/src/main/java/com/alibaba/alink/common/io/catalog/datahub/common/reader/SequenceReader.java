/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.reader;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.alibaba.alink.common.io.catalog.datahub.common.MetricUtils;
import com.alibaba.alink.common.io.catalog.datahub.common.source.AbstractParallelSourceBase;

import java.io.IOException;

/**
 * SequenceReader is used by {@link AbstractParallelSourceBase} which requests input split dynamically after finish current
 * split.
 * @param <T> Output type of the {@link AbstractParallelSourceBase}
 */
public class SequenceReader<T> {

	private InputSplitProvider inputSplitProvider;
	private AbstractParallelSourceBase <T, ?> sourceFunction;
	private Configuration config;
	private volatile boolean isStop = false;
	private Counter outputCounter;
	private Meter tpsMetric;

	public SequenceReader(AbstractParallelSourceBase<T, ?> source, InputSplitProvider provider, Configuration config) {
		this.sourceFunction = source;
		this.inputSplitProvider = provider;
		this.config = config;
		RuntimeContext context = source.getRuntimeContext();
		outputCounter = context.getMetricGroup().counter(MetricUtils.METRICS_TPS + "_counter", new SimpleCounter());
		tpsMetric = context.getMetricGroup().meter(MetricUtils.METRICS_TPS, new MeterView(outputCounter, 60));
	}

	public void run(SourceFunction.SourceContext<T> ctx) throws InputSplitProviderException, IOException, InterruptedException {

		InputSplit inputSplit;
		inputSplit = inputSplitProvider.getNextInputSplit(sourceFunction.getRuntimeContext().getUserCodeClassLoader());
		while (!isStop && inputSplit != null) {
			RecordReader<T, ?> recordReader = sourceFunction.createReader(config);
			try {
				recordReader.open(inputSplit, sourceFunction.getRuntimeContext());
				while (!isStop && recordReader.next()) {
					if (recordReader.isHeartBeat()) {
						continue;
					}
					synchronized (ctx.getCheckpointLock()) {
						tpsMetric.markEvent();
						ctx.collect(recordReader.getMessage());
					}
				}
			} finally {
				recordReader.close();
			}
			inputSplit = inputSplitProvider.getNextInputSplit(sourceFunction.getRuntimeContext().getUserCodeClassLoader());
		}
	}

	/**
	 * Stop externally.
	 */
	public void stop() {
		isStop = true;
	}
}
