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

package com.alibaba.flink.ml.cluster;

import java.util.Map;

/**
 * user define reporter to notify events.
 */
public class BaseEventReporter {
	/**
	 *
	 * @param jobId
	 * @param properties
	 */
	public void configure(String jobId, Map<String, String> properties) {
	}

	/**
	 * report job finish event.
	 */
	public void jobFinish() {
	}

	/**
	 * report job kill event.
	 */
	public void jobKill() {
	}

	/**
	 * report job failover event.
	 */
	public void jobFailover() {
	}

	/**
	 * report job failed event.
	 */
	public void jobFail(String msg) {
	}

	/**
	 * report node failed event.
	 */
	public void nodeFail(String... failedNodes) {
	}

	/**
	 * report node finish event.
	 */
	public void nodeFinish(String... node) {
	}

	/**
	 * report node register event.
	 */
	public void nodeRegister(String... node) {
	}
}
