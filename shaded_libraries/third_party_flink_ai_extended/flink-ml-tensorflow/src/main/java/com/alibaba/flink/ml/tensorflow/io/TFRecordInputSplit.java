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

package com.alibaba.flink.ml.tensorflow.io;

import org.apache.flink.core.io.InputSplit;
import org.apache.hadoop.fs.Path;

/**
 * tensorflow TFRecord format file InputSplit.
 */
public class TFRecordInputSplit implements InputSplit {
	private final int index;
	private int epochs = 0;
	private final String file;

	public TFRecordInputSplit(int num, String file) {
		this.index = num;
		this.file = file;
	}


	@Override
	public int getSplitNumber() {
		return index;
	}

	public Path getPath() {
		return new Path(file);
	}

	public int getEpochs() {
		return epochs;
	}

	public void setEpochs(int epochs) {
		this.epochs = epochs;
	}

	public int getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return "TFRecordInputSplit{" +
				"index=" + index +
				", epochs=" + epochs +
				'}';
	}

	@Override
	public boolean equals(Object obj) {
		if (getClass().getCanonicalName().equals(obj.getClass().getCanonicalName())) {
			TFRecordInputSplit other = (TFRecordInputSplit) obj;
			return index == other.index && file.equals(other.file);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return file.hashCode() * 31 + index;
	}
}
