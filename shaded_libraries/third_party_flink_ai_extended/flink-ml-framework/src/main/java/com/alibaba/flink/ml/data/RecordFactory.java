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

package com.alibaba.flink.ml.data;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.data.impl.RecordReaderImpl;
import com.alibaba.flink.ml.data.impl.RecordWriterImpl;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.ReflectUtil;

/**
 * a factory for RecordWriter and RecordReader.
 */
public class RecordFactory {
	public static RecordWriter getRecordWriter(MLContext mlContext) throws Exception {
		String className = mlContext.getProperties().getOrDefault(MLConstants.RECORD_WRITER_CLASS,
				RecordWriterImpl.class.getCanonicalName());
		Class[] classes = new Class[1];
		classes[0] = MLContext.class;
		Object[] objects = new Object[1];
		objects[0] = mlContext;
		return ReflectUtil.createInstance(className, classes, objects);
	}

	public static RecordReader getRecordRecord(MLContext mlContext) throws Exception {
		String className = mlContext.getProperties().getOrDefault(MLConstants.RECORD_READER_CLASS,
				RecordReaderImpl.class.getCanonicalName());
		Class[] classes = new Class[1];
		classes[0] = MLContext.class;
		Object[] objects = new Object[1];
		objects[0] = mlContext;
		return ReflectUtil.createInstance(className, classes, objects);
	}
}
