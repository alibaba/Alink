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

package com.alibaba.flink.ml.coding;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.impl.JsonCodingImpl;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.ReflectUtil;

/**
 * a factory to decoding and encoding
 */
public class CodingFactory {


	public static <T> Decoding<T> getDecoding(MLContext mlContext) throws Exception {
		String className = mlContext.getProperties().getOrDefault(MLConstants.DECODING_CLASS,
				JsonCodingImpl.class.getCanonicalName());
		Class[] classes = new Class[1];
		classes[0] = MLContext.class;
		Object[] objects = new Object[1];
		objects[0] = mlContext;
		return ReflectUtil.createInstance(className, classes, objects);
	}

	public static <T> Encoding<T> getEncoding(MLContext mlContext) throws Exception {
		String className = mlContext.getProperties().getOrDefault(MLConstants.ENCODING_CLASS,
				JsonCodingImpl.class.getCanonicalName());
		Class[] classes = new Class[1];
		classes[0] = MLContext.class;
		Object[] objects = new Object[1];
		objects[0] = mlContext;
		return ReflectUtil.createInstance(className, classes, objects);
	}
}
