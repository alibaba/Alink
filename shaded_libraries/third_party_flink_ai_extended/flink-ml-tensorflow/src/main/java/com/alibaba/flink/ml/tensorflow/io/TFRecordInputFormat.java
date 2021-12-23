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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.hadoop.util.TFRecordReader;

import java.io.IOException;
import java.util.List;

/**
 * flink read tensorflow TFRecord file input format.
 * output TFRecord record byte array.
 */
public class TFRecordInputFormat extends RichInputFormat<byte[], TFRecordInputSplit> {
	private int epochs = 1;
	private String[] paths;
	private transient TFRecordReader tfRecordReader;
	private transient FSDataInputStream fsdis;
	private boolean end = false;
	private static Logger LOG = LoggerFactory.getLogger(TFRecordInputFormat.class);

	public TFRecordInputFormat(String[] paths, int epochs) {
		this.paths = paths;
		this.epochs = epochs;
		if (epochs <= 0) {
			this.epochs = Integer.MAX_VALUE;
		}
		LOG.info("input epochs:" + this.epochs);
	}

	@Override
	public void configure(Configuration configuration) {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
		return null;
	}

	@Override
	public TFRecordInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		TFRecordInputSplit[] inputSplit = new TFRecordInputSplit[paths.length];
		int i = 0;
		for (String path : paths) {
			inputSplit[i] = new TFRecordInputSplit(i, path);
			i++;
		}
		return inputSplit;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(TFRecordInputSplit[] inputSplits) {
		int[] assigned = new int[inputSplits.length];
		return new InputSplitAssigner() {
			@Override
			public InputSplit getNextInputSplit(String host, int taskId) {
				synchronized (inputSplits){
					for (int i = 0; i < inputSplits.length; i++) {
						if (assigned[inputSplits[i].getSplitNumber()] < epochs){
							assigned[inputSplits[i].getSplitNumber()] ++;
							inputSplits[i].setEpochs(assigned[inputSplits[i].getSplitNumber()]);
							return inputSplits[i];
						}
					}
				}
				return null;
			}

			@Override
			public void returnInputSplit(List<InputSplit> splits, int taskId) {
				synchronized (inputSplits){
					for (InputSplit inputSplit:splits){
						assigned[inputSplit.getSplitNumber()] --;
					}
				}
			}
		};
	}

	@Override
	public void open(TFRecordInputSplit split) throws IOException {
		final Path file = split.getPath();
		LOG.info("open split path: " + file.toString());
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		FileSystem fs = file.getFileSystem(configuration);
		fsdis = fs.open(file, 4 * 1024 * 1024);
		tfRecordReader = new TFRecordReader(fsdis, true);
	}


	@Override
	public boolean reachedEnd() throws IOException {
		return end;
	}

	@Override
	public byte[] nextRecord(byte[] reuse) throws IOException {
		byte[] record = tfRecordReader.read();
		if (null == record) {
			end = true;
		}
		return record;
	}


	@Override
	public void close() throws IOException {
		if (fsdis != null) {
			fsdis.close();
		}
	}
}
