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

package com.alibaba.flink.ml.tensorflow2.data;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.data.RecordWriter;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * TFRecord format implementation of RecordWriter.
 */
public class TFRecordWriterImpl implements RecordWriter {
	private TFRecordWriter recordWriter;
	private MLContext mlContext;
	private final DataOutputStream outputStream;

	public TFRecordWriterImpl(MLContext mlContext) {
		this.mlContext = mlContext;
		this.outputStream = new DataOutputStream(mlContext.getOutWriter());
		recordWriter = new TFRecordWriter(outputStream);
	}

	@Override
	public boolean write(byte[] record, int offset, int length) throws IOException {
		recordWriter.write(record, offset, length);
		return true;
	}

	@Override
	public boolean write(byte[] record) throws IOException {
		recordWriter.write(record);
		return true;
	}

	@Override
	public void close() throws IOException {
		//mlContext.getOutWriter().close();
	}
}
