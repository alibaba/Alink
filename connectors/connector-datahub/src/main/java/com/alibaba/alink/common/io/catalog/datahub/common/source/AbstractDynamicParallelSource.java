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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AbstractDynamicParallelSource.
 * @param <T>
 * @param <CURSOR>
 */
public abstract class AbstractDynamicParallelSource<T, CURSOR extends Serializable>
		extends AbstractParallelSourceBase<T, CURSOR>
		implements CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractDynamicParallelSource.class);
	private static final long serialVersionUID = -7848357196819780804L;
	private static final String SOURCE_STATE_NAME = "source_offsets_state_name";
	private transient ListState<InnerProgress<CURSOR>> unionInitialProgress;
	private transient List<InnerProgress<CURSOR>> allSplitsInCP;

	protected transient List<Tuple2<InputSplit, CURSOR>> reservedProgress;

	public AbstractDynamicParallelSource() {
		super();
	}

	public abstract List<Tuple2<InputSplit, CURSOR>> reAssignInputSplitsForCurrentSubTask(
			int numberOfParallelSubTasks,
			int indexOfThisSubTask,
			List<InnerProgress<CURSOR>> allSplitsInState) throws IOException;

	/**
	 * Used to deal with situation where some state needed to reserve.
	 * @param numberOfParallelSubTasks
	 * @param indexOfThisSubTask
	 * @param allSplitsInState
	 * @return the split list
	 * @throws IOException
	 */
	public List<Tuple2<InputSplit, CURSOR>> reserveInputSplitsForCurrentSubTask(
			int numberOfParallelSubTasks,
			int indexOfThisSubTask,
			List<InnerProgress<CURSOR>> allSplitsInState) throws IOException{
		List<Tuple2<InputSplit, CURSOR>> result = new ArrayList<>();
		return result;
	}

	protected void createParallelReader(Configuration config) throws IOException {
		if (isRecoryFromState()) {
			LOG.info("Reocory State!");
			initialProgress = reAssignInputSplitsForCurrentSubTask(getRuntimeContext().getNumberOfParallelSubtasks(),
																getRuntimeContext().getIndexOfThisSubtask(),
																allSplitsInCP);
			reservedProgress = reserveInputSplitsForCurrentSubTask(getRuntimeContext().getNumberOfParallelSubtasks(),
																getRuntimeContext().getIndexOfThisSubtask(),
																allSplitsInCP);
		}
		super.createParallelReader(config);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		LOG.info("initializeState");
		ParameterizedType p = (ParameterizedType) this.getClass().getGenericSuperclass();
		TypeInformation type0 = TypeExtractor.createTypeInfo(InputSplit.class);
		TypeInformation type1 = TypeExtractor.createTypeInfo(p.getActualTypeArguments()[1]);
//		TypeInformation<Tuple2<InputSplit, CURSOR>> stateTypeInfo = new TupleTypeInfo<>(type0, type1);
		List<PojoField> pojoFields = new ArrayList<>();
		pojoFields.add(new PojoField(InnerProgress.class.getField("inputSplit"), type0));
		pojoFields.add(new PojoField(InnerProgress.class.getField("cursor"), type1));
		TypeInformation<InnerProgress> stateTypeInfo = new PojoTypeInfo<>(InnerProgress.class, pojoFields);

//		ListStateDescriptor<Tuple2<InputSplit, CURSOR>> descriptor = new ListStateDescriptor<>(SOURCE_STATE_NAME, stateTypeInfo);
		ListStateDescriptor<InnerProgress<CURSOR>> descriptor = new ListStateDescriptor(SOURCE_STATE_NAME, stateTypeInfo);
		unionInitialProgress = context.getOperatorStateStore().getUnionListState(descriptor);
		LOG.info("Restoring state: {}", unionInitialProgress);
		allSplitsInCP = new ArrayList<>();
		if (context.isRestored()) {
			recoryFromState = true;
			for (InnerProgress progress: unionInitialProgress.get()){
				allSplitsInCP.add(new InnerProgress(progress.inputSplit, progress.cursor));
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (disableParallelRead) {
			return;
		}
		unionInitialProgress.clear();

		// partition with progress
		Set<InputSplit> partitionWithState = new HashSet<>();
		for (Map.Entry<InputSplit, CURSOR> entry : parallelReader.getProgress().getProgress().entrySet()) {
			unionInitialProgress.add(new InnerProgress(entry.getKey(), entry.getValue()));
			partitionWithState.add(entry.getKey());
		}

		// partition without progress
		for (Tuple2<InputSplit, CURSOR> entry : initialProgress) {
			if (!partitionWithState.contains(entry.f0)) {
				unionInitialProgress.add(new InnerProgress(entry.f0, entry.f1));
			}
		}

		if (null != reservedProgress) {
			// reserved partition progress
			for (Tuple2<InputSplit, CURSOR> entry : reservedProgress) {
				if (!partitionWithState.contains(entry.f0)) {
					unionInitialProgress.add(new InnerProgress(entry.f0, entry.f1));
				}
			}
		}
	}

	/**
	 * InnerProgress.
	 * @param <CURSOR>
	 */
	public static class InnerProgress<CURSOR extends Serializable> implements Serializable {

		private static final long serialVersionUID = -7756210303146639268L;
		public InputSplit inputSplit;
		public CURSOR cursor;

		public InnerProgress() {
		}

		public InnerProgress(InputSplit inputSplit, CURSOR cursor) {
			this.inputSplit = inputSplit;
			this.cursor = cursor;
		}

		public InputSplit getInputSplit() {
			return inputSplit;
		}

		public InnerProgress<CURSOR> setInputSplit(InputSplit inputSplit) {
			this.inputSplit = inputSplit;
			return this;
		}

		public CURSOR getCursor() {
			return cursor;
		}

		public InnerProgress<CURSOR> setCursor(CURSOR cursor) {
			this.cursor = cursor;
			return this;
		}

		@Override
		public String toString() {
			return "InnerProgress{" + "inputSplit=" + inputSplit + ", cursor=" + cursor + '}';
		}
	}
}
