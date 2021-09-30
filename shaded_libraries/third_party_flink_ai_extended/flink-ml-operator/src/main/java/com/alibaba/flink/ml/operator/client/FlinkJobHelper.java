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

package com.alibaba.flink.ml.operator.client;

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.HashMap;
import java.util.Map;


/**
 * flink job helper, this util function help user change flink operator parallelism.
 */
public class FlinkJobHelper {
	private Map<String, Integer> parallelismMap = new HashMap<>();
	private int defaultParallelism = -1;

	/**
	 * @param streamGraph flink job StreamGraph.
	 * @return flink stream graph json string.
	 */
	public static String streamPlan(StreamGraph streamGraph) {
		return streamGraph.getStreamingPlanAsJSON();
	}

	/**
	 * set flink operator parallelism by name.
	 * @param name if flink operator name contains name param, then match the pattern.
	 * @param parallelism set flink operator parallelism.
	 */
	public void like(String name, int parallelism) {
		parallelismMap.put(name, parallelism);
	}

	/**
	 * set flink operator default parallelism.
	 * @param parallelism flink operator default parallelism.
	 */
	public void setDefaultParallelism(int parallelism) {
		defaultParallelism = parallelism;
	}

	/**
	 * @param streamGraph flink job StreamGraph.
	 * @return after user set flink job parallelism, the flink job StreamGraph.
	 */
	public StreamGraph matchStreamGraph(StreamGraph streamGraph) {

		for (StreamNode node : streamGraph.getStreamNodes()) {
			System.out.println(node.getOperatorName());
			boolean flag = true;
			for (String key : parallelismMap.keySet()) {
				if (node.getOperatorName().contains(key)) {
					node.setParallelism(parallelismMap.get(key));
					flag = false;
				}
			}
			if (defaultParallelism > 0 && flag) {
				node.setParallelism(defaultParallelism);
			}
		}
		return streamGraph;
	}


}
