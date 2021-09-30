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

package com.alibaba.alink.common.dl.data;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.data.RecordReader;
import com.alibaba.flink.ml.util.SpscOffHeapQueue;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * TFRecord format implementation of RecordReader.
 *
 * This file is an copy of {@link com.alibaba.flink.ml.tensorflow.data.TFRecordReaderImpl} except depending on
 * the copy of {@link TFRecordReader}.
 */
public class TFRecordReaderImpl implements RecordReader {
	private TFRecordReader recordReader;
	private final DataInputStream input;
	private boolean eof = false;

	public TFRecordReaderImpl(MLContext mlContext) {
		SpscOffHeapQueue.QueueInputStream in = mlContext.getInReader();
		input = new DataInputStream(in);
		recordReader = new TFRecordReader(input, true);
	}

	@Override
	public byte[] tryRead() throws IOException {
		if (input.available() > 0) {
			return read();
		}
		return null;
	}

	@Override
	public boolean isReachEOF() {
		return eof;
	}

	@Override
	public byte[] read() throws IOException {
		byte[] bytes = recordReader.read();
		if(null == bytes){
			eof = true;
		}
		return bytes;
	}

	@Override
	public void close() throws IOException {
	}
}
