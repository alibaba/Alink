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

package com.alibaba.flink.ml.cluster.master;

import com.alibaba.flink.ml.cluster.rpc.NodeClient;
import com.alibaba.flink.ml.proto.NodeSpec;
import com.google.protobuf.MessageOrBuilder;

/**
 * machine learning application master service interface.
 * AM state machine and transition can call am service methods then trigger state machine status change and transition.
 */
public interface AMService {
	/**
	 * @return current machine learning cluster version.
	 */
	long version();

	/**
	 * set current machine learning cluster version.
	 * @param version
	 */
	void setVersion(long version);

	/**
	 * handle transition exception interface.
	 * @param request am event request.
	 * @param t transition throw exception.
	 */
	void handleStateTransitionError(MessageOrBuilder request, Throwable t);

	/**
	 * update am server node client.
	 * @param key machine learning node identity.
	 * @param client node client.
	 */
	void updateNodeClient(String key, NodeClient client);

	/**
	 * send stop request to node server.
	 * @param nodeSpec machine learning node information.
	 * @throws Exception
	 */
	void stopNode(NodeSpec nodeSpec) throws Exception;

	/**
	 * send stop node request to all machine learning cluster nodes.
	 */
	void stopAllNodes();

	/**
	 * send restart request to machine learning node.
	 * @param nodeSpec machine learning node information.
	 * @throws Exception
	 */
	void restartNode(NodeSpec nodeSpec) throws Exception;

	/**
	 * send restart request to all machine learning cluster nodes.
	 * @throws Exception
	 */
	void restartAllNodes() throws Exception;

	/**
	 * start a heartbeat monitor to machine learning cluster node.
	 * @param nodeSpec node information.
	 * @param version current machine learning cluster version.
	 */
	void startHeartBeatMonitor(NodeSpec nodeSpec, long version);

	/**
	 * stop heart beat monitor of a machine learning node.
	 * @param clientKey machine learning node identity.
	 */
	void stopHeartBeatMonitorNode(String clientKey);

	/**
	 * stop heart beat monitor of all machine learning cluster nodes.
	 */
	void stopHeartBeatMonitorAllNode();

	/**
	 * stop application master service.
	 */
	void stopService();
}
