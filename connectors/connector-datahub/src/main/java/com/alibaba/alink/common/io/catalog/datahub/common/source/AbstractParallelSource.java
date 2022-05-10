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

package com.alibaba.alink.common.io.catalog.datahub.common.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ParallelSource.
 * @param <T>
 * @param <CURSOR>
 */
public abstract class AbstractParallelSource<T, CURSOR extends Serializable>
	extends AbstractParallelSourceBase<T, CURSOR>
		implements ListCheckpointed<Tuple2<InputSplit, CURSOR>> {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractParallelSource.class);
	private static final long serialVersionUID = -7848357196819780804L;

	/**
	 * Gets the current state of the function of operator. The state must reflect the result of all
	 * prior invocations to this function.
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @param timestamp Timestamp of the checkpoint.
	 * @return The operator state in a list of redistributable, atomic sub-states.
	 *         Should not return null, but empty list instead.
	 * @throws Exception Thrown if the creation of the state object failed. This causes the
	 *                   checkpoint to fail. The system may decide to fail the operation (and trigger
	 *                   recovery), or to discard this checkpoint attempt and to continue running
	 *                   and to try again with the next checkpoint attempt.
	 */
	public List<Tuple2<InputSplit, CURSOR>> snapshotState(long checkpointId, long timestamp) throws Exception {

		List<Tuple2<InputSplit, CURSOR>> state = new LinkedList<>();
		if (disableParallelRead) {
			return state;
		}

		// partition with progress
		Set<InputSplit> partitionWithState = new HashSet<>();
		for (Map.Entry<InputSplit, CURSOR> entry : parallelReader.getProgress().getProgress().entrySet()) {
			state.add(Tuple2.of(entry.getKey(), entry.getValue()));
			partitionWithState.add(entry.getKey());
		}

		// partition without progress
		for (Tuple2<InputSplit, CURSOR> entry: initialProgress) {
			if (!partitionWithState.contains(entry.f0)) {
				state.add(entry);
			}
		}

		return state;
	}

	/**
	 * Restores the state of the function or operator to that of a previous checkpoint.
	 * This method is invoked when a function is executed as part of a recovery run.
	 * <p>Note that restoreState() is called before open().</p>
	 *
	 * @param state The state to be restored as a list of atomic sub-states.
	 */
	public void restoreState(List<Tuple2<InputSplit, CURSOR>> state) throws Exception {
		LOG.info("Restoring state: {}", state);
		// mark restore state
		recoryFromState = true;
		if (state != null && !state.isEmpty()) {
			this.initialProgress = state;
		}
	}
}
