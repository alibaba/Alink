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

import com.alibaba.flink.ml.proto.MLClusterDef;
import com.alibaba.flink.ml.proto.MLJobDef;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.storage.StorageFactory;
import com.alibaba.flink.ml.cluster.storage.Storage;
import com.alibaba.flink.ml.proto.AMStatus;
import com.alibaba.flink.ml.proto.AMStatusMessage;
import com.alibaba.flink.ml.proto.NodeSpec;
import com.alibaba.flink.ml.proto.NodeSpecList;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a common implementation of AMMeta interface.
 */
public class AMMetaImpl implements AMMeta {
	private static final Logger LOG = LoggerFactory.getLogger(AMMetaImpl.class);

	Storage storage;

	public AMMetaImpl(MLContext MLContext) {
		storage = StorageFactory.getStorageInstance(MLContext.getProperties());
	}

	@Override
	public void saveClusterVersion(long version) throws IOException {
		storage.setValue(VERSION_NODE, String.valueOf(version).getBytes());
	}

	@Override
	public long restoreClusterVersion() throws IOException {
		byte[] bytes = storage.getValue(VERSION_NODE);
		if (null == bytes) {
			return 0;
		} else {
			String version = new String(bytes);
			return Long.valueOf(version);
		}
	}

	@Override
	public void saveAMIpPort(String ip, int port) throws IOException {
		String ipPort = String.format("%s:%d", ip, port);
		LOG.info("Saving {} as {}", AM_ADDRESS, ipPort);
		storage.setValue(AM_ADDRESS, ipPort.getBytes());
	}

	@Override
	public void removeAMIpPort() throws IOException {
		storage.removeValue(AM_ADDRESS);
	}

	@Override
	public HostAndPort restoreAMIpPort() throws IOException {
		byte[] bytes = storage.getValue(AM_ADDRESS);
		if (null == bytes) {
			return null;
		} else {
			String ipPort = new String(bytes);
			return HostAndPort.fromString(ipPort);
		}
	}

	@Override
	public NodeSpec restoreNodeSpec(String roleName, int taskIndex) throws IOException {
		byte[] bytes = storage.getValue(CLUSTER_INFO);
		if (null == bytes) {
			return null;
		} else {
			MLClusterDef clusterDef = MLClusterDef.parseFrom(bytes);
			for (MLJobDef jobDef : clusterDef.getJobList()) {
				if (jobDef.getName().equals(roleName)) {
					if (jobDef.containsTasks(taskIndex)) {
						return jobDef.getTasksOrDefault(taskIndex, null);
					} else {
						return null;
					}
				}
			}
			return null;
		}
	}

	@Override
	public synchronized MLClusterDef saveNodeSpec(NodeSpec nodeSpec) throws IOException {
		byte[] clusterBytes = this.storage.getValue(CLUSTER_INFO);
		MLClusterDef.Builder builder = MLClusterDef.newBuilder();
		if (null == clusterBytes) {
			// pass
		} else {
			MLClusterDef clusterDef = MLClusterDef.parseFrom(clusterBytes);
			builder.mergeFrom(clusterDef);
		}
		boolean flag = false;
		for (MLJobDef.Builder builder1 : builder.getJobBuilderList()) {
			if (builder1.getName().equals(nodeSpec.getRoleName())) {
				builder1.putTasks(nodeSpec.getIndex(), nodeSpec);
				flag = true;
			}
		}
		if (!flag) {
			builder.addJob(
					MLJobDef.newBuilder()
							.setName(nodeSpec.getRoleName())
							.putTasks(nodeSpec.getIndex(), nodeSpec));

		}
		this.storage.setValue(CLUSTER_INFO, builder.build().toByteArray());
		return builder.build();
	}


	@Override
	public void cleanCluster() throws IOException {
		this.storage.removeValue(CLUSTER_INFO);
	}

	@Override
	public MLClusterDef restoreClusterDef() throws IOException {
		byte[] bytes = storage.getValue(CLUSTER_INFO);
		if (null == bytes) {
			return null;
		} else {
			return MLClusterDef.parseFrom(bytes);
		}
	}

	@Override
	public MLClusterDef restoreFinishClusterDef() throws IOException {
		byte[] bytes = storage.getValue(FINISH_CLUSTER_INFO);
		if (null == bytes) {
			return null;
		} else {
			return MLClusterDef.parseFrom(bytes);
		}
	}

	@Override
	public synchronized MLClusterDef saveFinishNodeSpec(NodeSpec nodeSpec) throws IOException {
		byte[] finishBytes = this.storage.getValue(FINISH_CLUSTER_INFO);
		if (null == finishBytes) {
			MLClusterDef clusterDef = MLClusterDef.newBuilder()
					.addJob(MLJobDef.newBuilder()
							.setName(nodeSpec.getRoleName())
							.putTasks(nodeSpec.getIndex(), nodeSpec)
					).build();
			this.storage.setValue(FINISH_CLUSTER_INFO, clusterDef.toByteArray());
			return clusterDef;
		} else {
			MLClusterDef clusterDef = MLClusterDef.parseFrom(finishBytes);
			MLClusterDef.Builder builder = MLClusterDef.newBuilder();
			builder.mergeFrom(clusterDef);
			boolean flag = false;
			for (MLJobDef.Builder builder1 : builder.getJobBuilderList()) {
				if (builder1.getName().equals(nodeSpec.getRoleName())) {
					builder1
							.putTasks(nodeSpec.getIndex(), nodeSpec);
					flag = true;
				}
			}
			if (!flag) {
				builder.addJob(
						MLJobDef.newBuilder()
								.setName(nodeSpec.getRoleName())
								.putTasks(nodeSpec.getIndex(), nodeSpec)

				);
			}
			this.storage.setValue(FINISH_CLUSTER_INFO, builder.build().toByteArray());
			return builder.build();
		}
	}

	@Override
	public synchronized AMStatus saveAMStatus(AMStatus amStatus, AMStatus preAmStatus) throws IOException {
		if (preAmStatus != AMStatus.AM_UNKNOW) {
			AMStatus status = restoreAMStatus();
			LOG.debug("master status is " + status.toString());
			if (AMStatus.AM_UNKNOW == status) {
				storage.setValue(AM_STATUS, AMStatusMessage.newBuilder()
						.setStatus(amStatus).build().toByteArray());
				return amStatus;
			} else if (status != preAmStatus) {
				return status;
			}
		}
		storage.setValue(AM_STATUS, AMStatusMessage.newBuilder()
				.setStatus(amStatus).build().toByteArray());
		return amStatus;
	}

	@Override
	public AMStatus restoreAMStatus() throws IOException {
		byte[] bytes = storage.getValue(AM_STATUS);
		if (null == bytes) {
			return AMStatus.AM_UNKNOW;
		} else {
			return AMStatusMessage.parseFrom(bytes).getStatus();
		}
	}

	@Override
	public NodeSpecList restoreFailedNodes() throws IOException {
		byte[] bytes = storage.getValue(FAILED_NODES);
		if (null == bytes) {
			return null;
		} else {
			return NodeSpecList.parseFrom(bytes);
		}
	}

	@Override
	public synchronized void saveFailedNode(NodeSpec nodeSpec) throws IOException {
		byte[] bytes = storage.getValue(FAILED_NODES);
		if (null == bytes) {
			NodeSpecList nodeSpecList = NodeSpecList.newBuilder()
					.addNodes(nodeSpec).build();
			storage.setValue(FAILED_NODES, nodeSpecList.toByteArray());
		} else {
			NodeSpecList nodeSpecList = NodeSpecList.parseFrom(bytes);
			NodeSpecList.Builder builder = NodeSpecList.newBuilder().mergeFrom(nodeSpecList);
			builder.addNodes(nodeSpec);
			storage.setValue(FAILED_NODES, builder.build().toByteArray());
		}
	}

	@Override
	public void close() {
		storage.close();
	}

	@Override
	public void clear() {
		storage.clear();
	}
}
