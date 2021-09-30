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

package com.alibaba.flink.ml.cluster.rpc;

import com.alibaba.flink.ml.proto.*;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * client to communicate with application master.
 */
public class AMClient implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(AMClient.class);

	private final ManagedChannel channel;
	private AppMasterServiceGrpc.AppMasterServiceBlockingStub blockingStub;
	private final String host;
	private final int port;

	/**
	 *
	 * @param host
	 *      application master ip address.
	 * @param port
	 *      application master port.
	 */
	public AMClient(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port)
				.enableRetry()
				.maxRetryAttempts(3)
				// Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
				// needing certificates.
				.usePlaintext()
				.build(), host, port);
	}

	/**
	 * close am channel.
	 */
	@Override
	public void close() {
		LOG.info("close am client");
		if (channel != null) {
			channel.shutdownNow();
			try {
				boolean res = channel.awaitTermination(2, TimeUnit.MINUTES);
				LOG.info("am client channel termination:" + res);
			} catch (InterruptedException e) {
				LOG.info("am client channel termination interrupted.");
				//e.printStackTrace();
			}
//			if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
//				channel.shutdown();
//			} else {
//				channel.shutdown();
//				try {
//					channel.awaitTermination(2, TimeUnit.MINUTES);
//				} catch (InterruptedException e) {
//				}
//			}
		}
	}

	/**
	 * @return application master ip.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return application master server port.
	 */
	public int getPort() {
		return port;
	}

	/** Construct client for accessing AM server using the existing channel. */
	AMClient(ManagedChannel channel, String host, int port) {
		this.channel = channel;
		blockingStub = AppMasterServiceGrpc.newBlockingStub(channel);
		this.host = host;
		this.port = port;
	}

	/**
	 * register machine learning cluster node
	 * @param version
	 *      machine learning job instance id
	 * @param nodeSpec
	 *      machine learning cluster node description
	 * @return SimpleResponse
	 */
	public SimpleResponse registerNode(long version, NodeSpec nodeSpec) {
		RegisterNodeRequest request = RegisterNodeRequest.newBuilder()
				.setVersion(version).setNodeSpec(nodeSpec).build();
		return blockingStub.registerNode(request);
	}

	/**
	 * cluster node report heartbeat to application master
	 * @param version
	 *      machine learning job instance id
	 * @param nodeSpec
	 *      machine learning cluster node description
	 * @return SimpleResponse
	 */
	public SimpleResponse heartbeat(long version, NodeSpec nodeSpec) {
		HeartBeatRequest request = HeartBeatRequest.newBuilder().setVersion(version).setNodeSpec(nodeSpec).build();
		return blockingStub.heartBeatNode(request);
	}

	/**
	 * cluster node report finished
	 * @param version
	 *      machine learning job instance id
	 * @param nodeSpec
	 *      machine learning cluster node description
	 * @return SimpleResponse
	 */
	public SimpleResponse nodeFinish(long version, NodeSpec nodeSpec) {
		FinishNodeRequest request = FinishNodeRequest.newBuilder().setVersion(version).setNodeSpec(nodeSpec).build();
		return blockingStub.nodeFinish(request);
	}

	/**
	 * get cluster information
	 * @param version
	 *      machine learning job instance id
	 * @return cluster information
	 */
	public GetClusterInfoResponse getClusterInfo(long version) {
		GetClusterInfoRequest request = GetClusterInfoRequest.newBuilder().setVersion(version).setMessage("").build();
		return blockingStub.getClusterInfo(request);
	}

	/**
	 * get job version
	 * @return current job version
	 */
	public GetVersionResponse getVersion() {
		GetVersionRequest request = GetVersionRequest.newBuilder().setMessage("").build();
		return blockingStub.getVersion(request);
	}

	/**
	 * get application master status
	 * @return application master status
	 */
	public AMStatus getAMStatus() {
		GetAMStatusRequest request = GetAMStatusRequest.newBuilder().setMessage("").build();
		return blockingStub.getAMStatus(request).getStatus();
	}

	/**
	 * node report failed to application master
	 * @param version
	 *      machine learning job instance id
	 * @param nodeSpec
	 *      machine learning cluster node description
	 * @return SimpleResponse
	 */
	public SimpleResponse reportFailedNode(long version, NodeSpec nodeSpec) {
		return blockingStub.registerFailNode(
				RegisterFailedNodeRequest.newBuilder().setVersion(version).setNodeSpec(nodeSpec).build());
	}

	/***
	 *
	 * @param version
	 *      machine learning job instance id
	 * @param roleName
	 *      machine learning cluster role name
	 * @param index
	 *      cluster a role index
	 * @return SimpleResponse
	 */
	public SimpleResponse stopJob(long version, String roleName, int index) {
		StopAllWorkerRequest request = StopAllWorkerRequest.newBuilder()
				.setVersion(version)
				.setJobName(roleName)
				.setIndex(index)
				.build();
		return blockingStub.stopAllWorker(request);
	}

	/**
	 * get finished workers
	 * @param version
	 *      machine learning job instance id
	 * @return finished workers information
	 */
	public GetFinishNodeResponse getFinishedWorker(long version) {
		GetFinishedNodeRequest request = GetFinishedNodeRequest.newBuilder()
				.setVersion(version)
				.build();
		return blockingStub.getFinishedNode(request);
	}

	/**
	 * create node information.
	 * @param roleName cluster role name.
	 * @param ip node ip address.
	 * @param index node index.
	 * @param clientPort node server port.
	 * @return node information.
	 */
	public static NodeSpec newNodeSpec(String roleName, String ip, int index, int clientPort) {
		NodeSpec node = NodeSpec.newBuilder()
				.setRoleName(roleName)
				.setClientPort(clientPort)
				.setIndex(index)
				.setIp(ip)
				.build();
		return node;
	}

	public int getTaskIndex(long version, String scope, String key) {
		GetTaskIndexResponse response = blockingStub.getTaskIndex(GetTaskIndexRequest.newBuilder()
				.setScope(scope).setKey(key).setVersion(version).build());
		if (response.getCode() == RpcCode.OK.ordinal()) {
			return response.getIndex();
		} else {
			throw new RuntimeException(response.getMessage());
		}
	}

	/**
	 * wait machine learning cluster ready or timeout
	 * @param duration
	 *      wait time out
	 * @return cluster ready or not
	 * @throws InterruptedException
	 */
	public boolean waitForReady(Duration duration) throws InterruptedException {
		final long deadline = System.currentTimeMillis() + duration.toMillis();
		while (channel.getState(true) != ConnectivityState.READY) {
			if (System.currentTimeMillis() > deadline) {
				return false;
			}
			Thread.sleep(1000);
		}
		return true;
	}
}
