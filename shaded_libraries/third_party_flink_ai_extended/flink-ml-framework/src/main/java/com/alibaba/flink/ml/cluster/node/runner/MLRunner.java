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

package com.alibaba.flink.ml.cluster.node.runner;

/**
 * abstract ml scriptRunner.
 * ml scriptRunner created by cluster node, define run machine learning task process.
 */
public interface MLRunner extends Runnable {

	/**
	 * get am client.
	 * @throws Exception
	 */
	void initAMClient() throws Exception;

	/**
	 * get current cluster run job version.
	 * @throws Exception
	 */
	void getCurrentJobVersion() throws Exception;

	/**
	 * register node information to am server.
	 * @throws Exception
	 */
	void registerNode() throws Exception;

	/**
	 * start heartbeat thread with application master.
	 * @throws Exception
	 */
	void startHeartBeat() throws Exception;

	/**
	 * wait am until running status.
	 * @throws Exception
	 */
	void waitClusterRunning() throws Exception;

	/**
	 * get machine learning cluster information.
	 * @throws Exception
	 */
	void getClusterInfo() throws Exception;

	/**
	 * change or add some information to node runtime context.
	 * @throws Exception
	 */
	void resetMLContext() throws Exception;

	/**
	 * start machine learning script process.
	 * @throws Exception
	 */
	void runScript() throws Exception;

	/**
 	 * stop scriptRunner thread.
	 * @throws Exception
	 */
	void notifyStop() throws Exception;

	/**
	 * @return ml scriptRunner execute result.
	 */
	ExecutionStatus getResultStatus();
}
