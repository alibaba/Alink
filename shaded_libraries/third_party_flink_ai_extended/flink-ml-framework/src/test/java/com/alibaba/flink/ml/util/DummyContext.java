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

package com.alibaba.flink.ml.util;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.role.WorkerRole;

import java.util.HashMap;
import java.util.Map;

public class DummyContext {
	public static MLContext createDummyMLContext() {
		return createDummyMLContext(createDummyMLConfig());
	}

	public static MLConfig createDummyMLConfig() {
		Map<String, Integer> jobNumberMap = new HashMap<>();
		jobNumberMap.put(new WorkerRole().name(), 1);
		return new MLConfig(jobNumberMap, null, (String) null, "", "");
	}

	public static MLContext createDummyMLContext(MLConfig mlConfig) {
		try {
			Map<String, Integer> jobNumberMap = new HashMap<>();
			jobNumberMap.put(new WorkerRole().name(), 1);
			return new MLContext(ExecutionMode.TRAIN, mlConfig, new WorkerRole().name(), 0,
					null, null);
		} catch (MLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
