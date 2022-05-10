/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.alibaba.alink.common.io.catalog.datahub.common.source;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.catalog.datahub.common.reader.ParallelReader;
import com.alibaba.alink.common.io.catalog.datahub.common.reader.RecordReader;
import com.alibaba.alink.common.io.catalog.datahub.common.reader.SequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * AbstractParallelSourceBase.
 * @param <T>
 * @param <CURSOR>
 */
public abstract class AbstractParallelSourceBase<T, CURSOR extends Serializable>
		extends InputFormatSourceFunction<T>
		implements ResultTypeQueryable<T> {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractParallelSourceBase.class);
	private static final long serialVersionUID = -7848357196819780804L;

	protected transient ParallelReader <T, CURSOR> parallelReader;
	protected transient List<Tuple2<InputSplit, CURSOR>> initialProgress;

	protected transient SequenceReader <T> sequenceReader;

	protected boolean recoryFromState = false;

	protected boolean enableWatermarkEmitter = true;

	public void disableWatermarkEmitter() {
		this.enableWatermarkEmitter = false;
	}

	protected boolean disableParallelRead = false;

	protected boolean initInputSplitInMaster = false;

	protected boolean tracingMetricEnabled = true;

	protected int tracingSampleInterval = 1;

	protected volatile boolean exitAfterReadFinished = false;

	public void enableExitAfterReadFinished() {
		this.exitAfterReadFinished = true;
	}

	public AbstractParallelSourceBase() {
		super(null, null);
	}

	public abstract RecordReader <T, CURSOR> createReader(Configuration config) throws IOException;

	public abstract InputSplit[] createInputSplitsForCurrentSubTask(int numberOfParallelSubTasks, int indexOfThisSubTask)
			throws IOException;

	/**
	 * interface for user init code.
	 *
	 * @param config Configuration
	 */
	public void initOperator(Configuration config) throws IOException {
	}

	/**
	 * Get the partition name list of the source.
	 * @return Paritition list
	 * @throws Exception Get partition list failed
	 */
	public abstract List<String> getPartitionList() throws Exception;

	public boolean isRecoryFromState() {
		return recoryFromState;
	}

	@Override
	public void open(Configuration config) throws IOException {
		this.initOperator(config);

		StreamingRuntimeContext context = (StreamingRuntimeContext) this.getRuntimeContext();
		if (null != context && null != context.getExecutionConfig()
			&& null != context.getExecutionConfig().getGlobalJobParameters()) {
			Map<String, String> globalParametersMap = context.getExecutionConfig().getGlobalJobParameters().toMap();
			if (null != globalParametersMap && globalParametersMap.size() != 0) {
				for (String s : globalParametersMap.keySet()) {
					config.setString(s, globalParametersMap.get(s));
				}
			}
		}

		if (disableParallelRead) {
			createSequenceReader(config);
		} else {
			createParallelReader(config);
		}

		LOG.info("Init source succ.");
	}

	private void createSequenceReader(Configuration config) {
		Preconditions.checkArgument(initialProgress == null, "sequence read mode could not support checkpoint");
		StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
		InputSplitProvider provider = runtimeContext.getInputSplitProvider();
		this.sequenceReader = new SequenceReader<>(this, provider, config);
	}

	protected void createParallelReader(Configuration config) throws IOException {
		if (initialProgress == null) {
			createInitialProgress();
		}

		long watermarkInterval = 0;
		if (enableWatermarkEmitter) {
			// only enable watermark emitter, then get the real watermark interval
			// the watermark interval is a toggle of watermark emitter
			watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
		}

		this.parallelReader = new ParallelReader<>(getRuntimeContext(), config, watermarkInterval, tracingMetricEnabled, tracingSampleInterval);
		parallelReader.setExitAfterReadFinished(exitAfterReadFinished);
		for (Tuple2<InputSplit, CURSOR> entry : initialProgress) {
			LOG.info("entry of initialProgress:{}", entry);
			RecordReader<T, CURSOR> reader = createReader(config);
			parallelReader.addRecordReader(reader, entry.f0, entry.f1);
			LOG.info("Reader {} seeking to {}", entry.f0, String.valueOf(entry.f1));
		}

		getRuntimeContext().getMetricGroup().counter("partition").inc(initialProgress.size());
	}

	protected void createInitialProgress() throws IOException {
		initialProgress = new LinkedList<>();
		if (initInputSplitInMaster) {
			StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
			InputSplitProvider provider = runtimeContext.getInputSplitProvider();
			try {
				InputSplit inputSplit = provider.getNextInputSplit(runtimeContext.getUserCodeClassLoader());
				while (inputSplit != null) {
					initialProgress.add(Tuple2.<InputSplit, CURSOR>of(inputSplit, null));
					inputSplit = provider.getNextInputSplit(runtimeContext.getUserCodeClassLoader());
				}
			} catch (InputSplitProviderException e) {
				throw new IOException("Get inputsplit from JM error.", e);
			}
		} else {
			StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
			InputSplit[] inputSplits = createInputSplitsForCurrentSubTask(
					runtimeContext.getNumberOfParallelSubtasks(), runtimeContext.getIndexOfThisSubtask());
			for (InputSplit inputSplit: inputSplits) {
				initialProgress.add(Tuple2.<InputSplit, CURSOR>of(inputSplit, null));
			}
		}
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		if (disableParallelRead) {
			sequenceReader.run(ctx);
		} else {
			parallelReader.run(ctx);
		}
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void cancel() {
		if (parallelReader != null) {
			parallelReader.stop();
		}

		if (sequenceReader != null) {
			sequenceReader.stop();
		}

	}

	@Override
	public TypeInformation<T> getProducedType() {
		ParameterizedType p = (ParameterizedType) this.getClass().getGenericSuperclass();
		return (TypeInformation<T>) TypeExtractor.createTypeInfo(p.getActualTypeArguments()[0]);
	}

	public List<Tuple2<InputSplit, CURSOR>> getInitialProgress() {
		return initialProgress;
	}

	@Override
	public InputFormat<T, InputSplit> getFormat() {
		return new ParallelSourceInputFormatWrapper<>(this);
	}

	public void disableParallelRead() {
		this.disableParallelRead = true;
	}

	public void setInitInputSplitInMaster(boolean enabled) {
		initInputSplitInMaster = enabled;
	}

	public boolean getInitInputSplitInMaster() {
		return initInputSplitInMaster;
	}

	public void enableParallelRead() {
		this.disableParallelRead = false;
	}

	public boolean isParallelReadDisabled() {
		return this.disableParallelRead;
	}

	public void enableTracingMetrics(int tracingSampleInterval) {
		this.tracingMetricEnabled = true;
		this.tracingSampleInterval = tracingSampleInterval;
	}

	public void disableTracingMetrics() {
		this.tracingMetricEnabled = false;
	}

	/**
	 * ParallelSourceInputFormatWrapper.
	 * @param <T>
	 */
	protected static class ParallelSourceInputFormatWrapper<T> implements InputFormat<T, InputSplit> {

		protected AbstractParallelSourceBase source;
		protected transient Configuration config;
		protected transient int[] taskInputSplitStartIndex;
		protected transient int[] taskInputSplitSize;

		public ParallelSourceInputFormatWrapper(AbstractParallelSourceBase<T, ?> source) {
			this.source = source;
		}

		@Override
		public void configure(Configuration configuration) {
			this.config = configuration;
		}

		@Override
		public InputSplit[] createInputSplits(int taskNum) throws IOException {
			if (!source.initInputSplitInMaster) {
				return new InputSplit[0];
			}
			taskInputSplitSize = new int[taskNum];
			taskInputSplitStartIndex = new int[taskNum];
			ArrayList<InputSplit> allSplits = new ArrayList<>();
			source.initOperator(config);
			for (int i = 0; i < taskNum; i++) {
				InputSplit[] inputSplits = source.createInputSplitsForCurrentSubTask(taskNum, i);
				taskInputSplitStartIndex[i] = allSplits.size();
				taskInputSplitSize[i] = inputSplits.length;
				for (InputSplit inputSplit : inputSplits) {
					allSplits.add(inputSplit);
				}
			}
			source.close();
			return allSplits.toArray(new InputSplit[allSplits.size()]);
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
			if (!source.initInputSplitInMaster || source.disableParallelRead) {
				return new DefaultInputSplitAssigner(inputSplits);
			} else {
				return new PreAssignedInputSplitAssigner(inputSplits, taskInputSplitSize, taskInputSplitStartIndex);
			}
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public boolean reachedEnd() throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public T nextRecord(T t) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public void close() throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public void open(InputSplit inputSplit) throws IOException {
			throw new NotImplementedException();
		}

	}

	/**
	 * Publicly visible for testing.
	 */
	@VisibleForTesting
	public static class PreAssignedInputSplitAssigner implements InputSplitAssigner {
		private final Map<Integer, Integer> assignedSplits;
		private final Map<Integer, Deque<InputSplit>> unassignedSplitsByTask;

		public PreAssignedInputSplitAssigner(InputSplit[] inputSplits, int[] taskInputSplitSize, int[] taskInputSplitStartIndex) {
			this.assignedSplits = new HashMap<>(inputSplits.length);
			this.unassignedSplitsByTask = new HashMap<>();
			List<InputSplit> splitList = Arrays.asList(inputSplits);
			for (int i = 0; i < taskInputSplitStartIndex.length; i++) {
				int startingIndexForTask = taskInputSplitStartIndex[i];
				Deque<InputSplit> splitsForTask =
						new LinkedList<>(splitList.subList(startingIndexForTask, taskInputSplitSize[i] + startingIndexForTask));
				unassignedSplitsByTask.put(i, splitsForTask);
			}
		}

		@Override
		public InputSplit getNextInputSplit(String location, int taskIndex) {
			checkTaskIndex(taskIndex);
			Deque<InputSplit> unassignedSplitsForTask = unassignedSplitsByTask.get(taskIndex);
			InputSplit split = unassignedSplitsForTask.pollFirst();
			if (split != null) {
				assignedSplits.put(split.getSplitNumber(), taskIndex);
			}
			return split;
		}

		@Override
		public void returnInputSplit(List<InputSplit> splits, int taskIndex) {
			checkTaskIndex(taskIndex);
			Deque<InputSplit> splitsForTask = unassignedSplitsByTask.get(taskIndex);
			for (InputSplit split : splits) {
				Integer assignedTask = assignedSplits.get(split.getSplitNumber());
				Preconditions.checkState(assignedTask != null && assignedTask == taskIndex,
					"Split " + split.getSplitNumber() + " was not assigned to task " + taskIndex);
				splitsForTask.addFirst(split);
			}
		}

		private void checkTaskIndex(int taskIndex) {
			Preconditions.checkArgument(taskIndex >= 0 && taskIndex < unassignedSplitsByTask.size(), "Fail to create");
		}
	}
}
