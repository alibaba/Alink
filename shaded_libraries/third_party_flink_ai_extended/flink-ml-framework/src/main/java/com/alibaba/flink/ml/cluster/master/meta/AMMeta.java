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

package com.alibaba.flink.ml.cluster.master.meta;

import com.alibaba.flink.ml.proto.AMStatus;
import com.alibaba.flink.ml.proto.MLClusterDef;
import com.alibaba.flink.ml.proto.NodeSpec;
import com.alibaba.flink.ml.proto.NodeSpecList;
import com.google.common.net.HostAndPort;

import java.io.IOException;

/**
 * This class is responsible for saving the status and meta information of restored AM.
 */
public interface AMMeta {

	String AM_ADDRESS = "am_address";
	String CLUSTER_INFO = "cluster_info";
	String FINISH_CLUSTER_INFO = "finish_cluster_info";
	String AM_STATUS = "am_status";
	String FAILED_NODES = "failed_nodes";
	String VERSION_NODE = "cluster_version";


	/**
	 * persistence current cluster version.
 	 */
	void saveClusterVersion(long version) throws IOException;

	/**
	 * get current cluster version.
	 * @return current cluster version
	 * @throws IOException
	 */
	long restoreClusterVersion() throws IOException;

	/**
	 * persistence application master address.
	 * @param ip am ip address.
	 * @param port am server port.
	 * @throws IOException
	 */
	void saveAMIpPort(String ip, int port) throws IOException;

	/**
	 * clear application master address.
	 * @throws IOException
	 */
	void removeAMIpPort() throws IOException;


	/**
	 * get application master address.
	 * @return am ip and port.
	 * @throws IOException
	 */
	HostAndPort restoreAMIpPort() throws IOException;

	/**
	 * get node information.
	 * @param roleName cluster role name.
	 * @param taskIndex node index in a cluster role.
	 * @return node information.
	 * @throws IOException
	 */
	NodeSpec restoreNodeSpec(String roleName, int taskIndex) throws IOException;

	/**
	 * persistence node information.
	 * @param nodeSpec node information for saving.
	 * @return cluster information.
	 * @throws IOException
	 */
	MLClusterDef saveNodeSpec(NodeSpec nodeSpec) throws IOException;

	/**
	 * clean cluster information.
	 * @throws IOException
	 */
	void cleanCluster() throws IOException;

	/**
	 * @return all node information.
	 * @throws IOException
	 */
	MLClusterDef restoreClusterDef() throws IOException;

	/**
	 * @return finished nodes information.
	 * @throws IOException
	 */
	MLClusterDef restoreFinishClusterDef() throws IOException;

	/**
	 * persistence finished node information.
	 * @param nodeSpec node information.
	 * @return finished node cluster.
	 * @throws IOException
	 */
	MLClusterDef saveFinishNodeSpec(NodeSpec nodeSpec) throws IOException;

	/**
	 * persistence am status.
	 * @param amStatus current am state.
	 * @param preAmStatus old am state
	 * @return current am state.
	 * @throws IOException
	 */
	AMStatus saveAMStatus(AMStatus amStatus, AMStatus preAmStatus) throws IOException;

	/**
	 * @return current am state.
	 * @throws IOException
	 */
	AMStatus restoreAMStatus() throws IOException;

	/**
	 * @return failed node list.
	 * @throws IOException
	 */
	NodeSpecList restoreFailedNodes() throws IOException;

	/**
	 * persistence failed node information.
	 * @param nodeSpec node information.
	 * @throws IOException
	 */
	void saveFailedNode(NodeSpec nodeSpec) throws IOException;

	/**
	 * close am meta handle.
	 */
	void close();

	/**
	 * clear all AM state.
	 */
	void clear();

}
