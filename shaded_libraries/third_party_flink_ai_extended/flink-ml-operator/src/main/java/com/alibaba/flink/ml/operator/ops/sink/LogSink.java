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

package com.alibaba.flink.ml.operator.ops.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * print up stream object.
 * @param <IN> up stream object class.
 */
public class LogSink<IN> extends RichSinkFunction<IN> {
	private static Logger LOG = LoggerFactory.getLogger(LogSink.class);

	private int count = 0;

	@Override
	public void invoke(IN value, Context context) {
		count++;
		if (0 == count % 10) {
			LOG.info("LogSink:" + count + " value:" + value.toString());
		}
	}

	@Override
	public void close() {
		LOG.info("LogSink process count:" + count);
	}
}
