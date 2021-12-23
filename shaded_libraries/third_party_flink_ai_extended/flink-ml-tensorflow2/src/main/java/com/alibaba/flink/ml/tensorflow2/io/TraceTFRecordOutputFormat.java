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

package com.alibaba.flink.ml.tensorflow2.io;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import com.alibaba.flink.ml.util.ProtoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.proto.example.Example;

import java.io.IOException;

public class TraceTFRecordOutputFormat extends RichOutputFormat<byte[]> {
	public static Logger LOG = LoggerFactory.getLogger(TraceTFRecordOutputFormat.class);

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

	}

	@Override
	public void writeRecord(byte[] record) throws IOException {
		Example example = Example.parseFrom(record);
        LOG.info(ProtoUtil.protoToJson(example).substring(0, 30));
	}

	@Override
	public void close() throws IOException {

	}
}
