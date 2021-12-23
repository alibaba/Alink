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
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * node client to communicate with node service.
 */
public class NodeClient implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(NodeClient.class.getName());

	private final ManagedChannel channel;
	private NodeServiceGrpc.NodeServiceBlockingStub blockingStub;
	private NodeServiceGrpc.NodeServiceFutureStub futureStub;


	public NodeClient(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port)
				// Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
				// needing certificates.
				.usePlaintext()
				.build());
	}

	@Override
	public void close() {
		if (channel != null) {
			channel.shutdown();
			try {
				channel.awaitTermination(2, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
			}
		}
	}

	/** Construct client for accessing AM server using the existing channel. */
	private NodeClient(ManagedChannel channel) {
		this.channel = channel;
		blockingStub = NodeServiceGrpc.newBlockingStub(channel);
		futureStub = NodeServiceGrpc.newFutureStub(channel);
	}

	/**
	 * method to get node runtime context
	 * @return response to machine learning context
	 */
	public ContextResponse getMLContext() {
		ContextRequest request = ContextRequest.newBuilder().setMessage("").build();
		return blockingStub.getContext(request);
	}

	/**
	 * send stop node request, no blocking interface
	 * @return stop node response
	 */
	public ListenableFuture<NodeStopResponse> stopNode() {
		NodeStopRequest request = NodeStopRequest.newBuilder().setMessage("").build();
		return futureStub.nodeStop(request);
	}

	/**
	 * send stop node request, blocking interface
	 * @return stop node response
	 */
	public NodeStopResponse stopNodeBlocking() {
		NodeStopRequest request = NodeStopRequest.newBuilder().setMessage("").build();
		return blockingStub.nodeStop(request);
	}

	/**
	 * send restart request to node
	 * @return restart response
	 */
	public ListenableFuture<NodeRestartResponse> restartNode() {
		NodeRestartRequest request = NodeRestartRequest.newBuilder().setMessage("").build();
		return futureStub.nodeRestart(request);
	}

	/**
	 * get finished workers information
	 * @return finished workers
	 */
	public List<Integer> getFinishWorker() {
		NodeSimpleRequest request = NodeSimpleRequest.newBuilder()
				.setCode(0)
				.build();
		FinishWorkerResponse response = blockingStub.getFinishWorker(request);
		int code = response.getCode();
		if (0 == code) {
			return response.getWorkersList();
		} else {
			return new ArrayList<>();
		}
	}

	/**
	 * sent stop job to node
	 */
	public void stopJob() {
		NodeSimpleRequest request = NodeSimpleRequest.newBuilder()
				.setCode(0)
				.build();
		blockingStub.finishJob(request);
	}


}
