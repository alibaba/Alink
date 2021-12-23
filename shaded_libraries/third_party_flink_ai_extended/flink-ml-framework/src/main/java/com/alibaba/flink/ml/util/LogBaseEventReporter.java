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

import com.alibaba.flink.ml.cluster.BaseEventReporter;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * a BaseEventReporter implementation with log.
 */
public class LogBaseEventReporter extends BaseEventReporter {
	private static Logger LOG = LoggerFactory.getLogger(LogBaseEventReporter.class);

	private String jobId;
	private Map<String, String> properties;

	@Override
	public void configure(String jobId, Map<String, String> properties) {
		this.jobId = jobId;
		this.properties = properties;
	}

	@Override
	public void jobFinish() {
		LOG.info("### Job " + jobId + " has finished");
	}

	@Override
	public void jobKill() {
		LOG.info("### Job " + jobId + " has been killed.");
	}

	@Override
	public void jobFailover() {
		LOG.info("### Job " + jobId + " has failed over.");
	}

	@Override
	public void jobFail(String msg) {
		LOG.info("### Job " + jobId + " has failed over.");
	}

	@Override
	public void nodeFail(String... failedNodes) {
		LOG.info("### Job " + jobId + " has failed nodes: " + Joiner.on(", ").join(failedNodes));
	}

	@Override
	public void nodeFinish(String... nodes) {
		LOG.info("### Job " + jobId + " has finished nodes: " + Joiner.on(", ").join(nodes));
	}

	@Override
	public void nodeRegister(String... nodes) {
		LOG.info("### Job " + jobId + " has following nodes be registered: " + Joiner.on(", ").join(nodes));
	}
}
