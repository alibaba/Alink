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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.IOException;
import java.io.Serializable;

/**
 * Base class for locatable source, which will assign input split according to location of input splits.
 * @param <T>
 * @param <CURSOR>
 */
public abstract class AbstractLocatableParallelSource<T, CURSOR extends Serializable> extends AbstractParallelSource<T, CURSOR> {
	@Override
	public abstract LocatableInputSplit[] createInputSplitsForCurrentSubTask(int numberOfParallelSubTasks, int indexOfThisSubTask)
			throws IOException;

	@Override
	public InputFormat<T, InputSplit> getFormat() {
		return new LocatableParallelSourceInputFormatWrapper<>(this);
	}

	/**
	 * LocatableParallelSourceInputFormatWrapper.
	 * @param <T>
	 */
	protected static class LocatableParallelSourceInputFormatWrapper<T> extends ParallelSourceInputFormatWrapper<T> {

		public LocatableParallelSourceInputFormatWrapper(AbstractParallelSource<T, ?> source) {
			super(source);
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
			LocatableInputSplit[] locatableInputSplits = new LocatableInputSplit[inputSplits.length];
			for (int i = 0; i < inputSplits.length; i++) {
				locatableInputSplits[i] = (LocatableInputSplit) inputSplits[i];
			}
			if (source.isParallelReadDisabled()) {
				return new LocatableInputSplitAssigner(locatableInputSplits);
			}
			return super.getInputSplitAssigner(inputSplits);
		}
	}
}
