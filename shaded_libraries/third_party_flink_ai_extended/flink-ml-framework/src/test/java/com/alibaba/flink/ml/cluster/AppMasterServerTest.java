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

import com.alibaba.flink.ml.cluster.rpc.AMClient;
import com.alibaba.flink.ml.cluster.rpc.AMRegistry;
import com.alibaba.flink.ml.cluster.rpc.AppMasterServer;
import com.alibaba.flink.ml.cluster.rpc.RpcCode;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.proto.*;
import com.alibaba.flink.ml.cluster.role.AMRole;
import com.alibaba.flink.ml.cluster.role.PsRole;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.util.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.curator.test.TestingServer;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class AppMasterServerTest {

	private static TestingServer testingServer;
	private static String IP;
	private static final Logger LOG = LoggerFactory.getLogger(AppMasterServerTest.class);

	@BeforeClass
	public static void init() throws Exception {
		IP = IpHostUtil.getIpAddress();
	}

	@Before
	public void setUp() throws Exception {
		testingServer = new TestingServer(2181, true);
	}

	@After
	public void tearDown() throws Exception {
		testingServer.stop();
	}

	private static class DummyNodeServer {

		private static final long timeout = Duration.ofSeconds(30).toMillis();

		private final MLContext mlContext;
		private final Server server;
		private AMClient amClient;
		private long version;

		DummyNodeServer(MLContext mlContext, NodeServiceGrpc.NodeServiceImplBase service) throws IOException {
			this.mlContext = mlContext;
			server = ServerBuilder.forPort(0).addService(service).build();
			server.start();
			amClient = AMRegistry.getAMClient(mlContext);
		}

		SimpleResponse registerNode() {
			waitForAMStatus(AMStatus.AM_INIT);
			version = amClient.getVersion().getVersion();
			NodeSpec nodeSpec = AMClient
					.newNodeSpec(mlContext.getRoleName(), IP, mlContext.getIndex(), server.getPort());
			return amClient.registerNode(version, nodeSpec);
		}

		void ensureRegisterSucceed() {
			Assert.assertEquals(mlContext.getIdentity() + " register node failed",
					RpcCode.OK.ordinal(), registerNode().getCode());
		}

		SimpleResponse twiceRegisterNode() {
			waitForAMStatus(AMStatus.AM_RUNNING);
			version = amClient.getVersion().getVersion();
			NodeSpec nodeSpec = AMClient
					.newNodeSpec(mlContext.getRoleName(), IP, mlContext.getIndex(), server.getPort());
			return amClient.registerNode(version, nodeSpec);
		}

		AMStatus getAmStatus() {
			while (true) {
				try {
					return amClient.getAMStatus();
				} catch (Exception e) {
//                    e.printStackTrace();
					try {
						amClient = AMRegistry.getAMClient(mlContext);
						Thread.sleep(3000);
					} catch (Exception e1) {
//                        e1.printStackTrace();
						LOG.warn("{} failed update AM address", mlContext.getIdentity(), e);
					}
				}
			}
		}

		SimpleResponse finishNode() {
			waitForAMStatus(AMStatus.AM_RUNNING);
			NodeSpec nodeSpec = AMClient
					.newNodeSpec(mlContext.getRoleName(), IP, mlContext.getIndex(), server.getPort());
			return amClient.nodeFinish(version, nodeSpec);
		}

		void updateAmAddress() {
			try {
				amClient = AMRegistry.getAMClient(mlContext);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		void ensureFinishSucceed() {
			Assert.assertEquals(mlContext.getIdentity() + " finish node failed",
					RpcCode.OK.ordinal(), finishNode().getCode());
		}

		SimpleResponse failNode() {
			waitForAMStatus(AMStatus.AM_RUNNING);
			return amClient.reportFailedNode(version, getNodeSpec());
		}

		NodeSpec getNodeSpec() {
			return NodeSpec.newBuilder().setRoleName(mlContext.getRoleName()).setIndex(mlContext.getIndex())
					.setIp(IP).setClientPort(server.getPort()).build();
		}

		MLClusterDef getCluster() {
			return amClient.getClusterInfo(version).getClusterDef();
		}

		public void waitForAMStatus(AMStatus target) {
			long deadline = System.currentTimeMillis() + timeout;
			while (amClient.getAMStatus() != target) {
				if (System.currentTimeMillis() > deadline) {
					throw new RuntimeException("Timed out waiting for status:" + target);
				}
				Thread.yield();
			}
		}

		void close() {
			amClient.close();
		}
	}


	@Test
	public void testAMFailOver() throws Exception {
		System.out.println(SysUtil._FUNC_());
		final int numWorker = 3;
		final Duration timeout = Duration.ofSeconds(100);
		ExecutorService executor = new ThreadPoolExecutor(numWorker, numWorker, 60, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(numWorker));
		MLConfig mlConfig = DummyContext.createDummyMLConfig();
		mlConfig.setRoleNum(new WorkerRole().name(), numWorker);
		mlConfig.getProperties().put(MLConstants.HEARTBEAT_TIMEOUT, String.valueOf(timeout.toMillis()));
		mlConfig.getProperties().put(MLConstants.FAILOVER_STRATEGY, MLConstants.FAILOVER_RESTART_INDIVIDUAL_STRATEGY);
		FutureTask<Void> amFuture = startAMServer(mlConfig);
		AtomicBoolean failoverFlag = new AtomicBoolean(false);

		List<FutureTask<Void>> serverFutures = new ArrayList<>(numWorker);
		for (int i = 0; i < numWorker; i++) {
			MLContext mlContext = new MLContext(ExecutionMode.TRAIN, mlConfig, new WorkerRole().name(),
					i, null, null);
			NodeServiceGrpc.NodeServiceImplBase service = mockNodeService();
			DummyNodeServer server = new DummyNodeServer(mlContext, service);
			FutureTask<Void> serverFuture = new FutureTask<>(() -> {
				Assert.assertEquals(server.mlContext.getIdentity() + " register node failed",
						RpcCode.OK.ordinal(), server.registerNode().getCode());
				while (!failoverFlag.get()) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				server.updateAmAddress();
				Assert.assertEquals("am not running", AMStatus.AM_RUNNING, server.getAmStatus());
				SimpleResponse response = server.finishNode();
				Assert.assertEquals(server.mlContext.getIdentity() + " finish node failed "
								+ response.getMessage(),
						RpcCode.OK.ordinal(), response.getCode());

				server.close();
			}, null);
			executor.submit(serverFuture);
			serverFutures.add(serverFuture);
		}
		MLContext amContext = new MLContext(ExecutionMode.TRAIN, mlConfig, new AMRole().name(), 0,
				null, null);
		AMClient amClient = AMRegistry.getAMClient(amContext);
		while (true) {
			if (amClient.getAMStatus().equals(AMStatus.AM_RUNNING)) {
				break;
			} else {
				Thread.sleep(1000);
			}
		}
		amFuture.cancel(true);
		amFuture = startAMServer(mlConfig);
		failoverFlag.set(true);

		for (FutureTask<Void> serverFuture : serverFutures) {
			serverFuture.get();
		}
		executor.shutdown();
		amFuture.get();
	}

	@Test
	public void testHeartbeatTimeout() throws Exception {
		System.out.println(SysUtil._FUNC_());
		final int numWorker = 2;
		final Duration timeout = Duration.ofSeconds(3);
		ExecutorService executor = new ThreadPoolExecutor(numWorker, numWorker, 60, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(numWorker));
		MLConfig mlConfig = DummyContext.createDummyMLConfig();
		mlConfig.setRoleNum(new WorkerRole().name(), numWorker);
		mlConfig.getProperties().put(MLConstants.HEARTBEAT_TIMEOUT, String.valueOf(timeout.toMillis()));
		FutureTask<Void> amFuture = startAMServer(mlConfig);

		List<FutureTask<Void>> serverFutures = new ArrayList<>(numWorker);
		for (int i = 0; i < numWorker; i++) {
			MLContext tfContext = new MLContext(ExecutionMode.TRAIN, mlConfig, new WorkerRole().name(), i,
					null, null);
			NodeServiceGrpc.NodeServiceImplBase service = mockNodeService();
			DummyNodeServer server = new DummyNodeServer(tfContext, service);
			FutureTask<Void> serverFuture = new FutureTask<>(() -> {
				server.ensureRegisterSucceed();
				// let the heartbeat timeout
				try {
					Thread.sleep(timeout.toMillis());
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				// AM status should be set back to INIT after the timeout
				server.ensureRegisterSucceed();
				server.ensureFinishSucceed();
				server.close();
				// make sure the failover happened
				verify(service).nodeRestart(any(), any());
			}, null);
			executor.submit(serverFuture);
			serverFutures.add(serverFuture);
		}
		executor.shutdown();
		for (FutureTask<Void> serverFuture : serverFutures) {
			serverFuture.get();
		}

		amFuture.get();
	}

//    @Test
//    public void multiNodeFailRestartOnlyOnce() throws Exception {
//        System.out.println(SysUtil._FUNC_());
//        final int numWorker = 10;
//        ExecutorService executor = new ThreadPoolExecutor(numWorker, numWorker, 60, TimeUnit.SECONDS,
//                new LinkedBlockingQueue<>(numWorker));
//        TFConfig tfConfig = DummyContext.createDummyMLConfig();
//        tfConfig.setWorkerNum(numWorker);
//        tfConfig.setPsNum(0);
//        FutureTask<Void> amFuture = startAMServer(tfConfig);
//
//        List<FutureTask<Void>> serverFutures = new ArrayList<>(numWorker);
//        // start workers
//        for (int i = 0; i < numWorker; i++) {
//            TFContext mlContext = new TFContext(ExecutionMode.TRAIN, tfConfig, Role.WORKER.toString(), i, null, null);
//            NodeMessage tfNodeMessage = new NodeMessage();
//            TFNodeServiceImplBase service = mockNodeServiceSetFlag(tfNodeMessage);
//            DummyNodeServer server = new DummyNodeServer(mlContext, service);
//            FutureTask<Void> serverFuture = new FutureTask<>(() -> {
//                Assert.assertEquals(server.mlContext.getIdentity() + " register node failed",
//                        RpcCode.OK.ordinal(), server.registerNode().getCode());
//                Assert.assertEquals(server.mlContext.getIdentity() + " fail node failed",
//                        RpcCode.OK.ordinal(), server.failNode().getCode());
//                Assert.assertEquals(server.mlContext.getIdentity() + " re-register node failed",
//                        RpcCode.OK.ordinal(), server.registerNode().getCode());
//                if(0 == mlContext.getIndex()) {
//                    Assert.assertEquals(server.mlContext.getIdentity() + " finish node failed",
//                        RpcCode.OK.ordinal(), server.finishNode().getCode());
//                }else {
//                    tfNodeMessage.waitForStop(1);
//                    Assert.assertEquals(server.mlContext.getIdentity() + " finish node failed",
//                        RpcCode.OK.ordinal(), server.finishNode().getCode());
//                }
//                server.close();
//                // each worker should be restarted only once
//                verify(service).nodeRestart(any(), any());
//            }, null);
//            executor.submit(serverFuture);
//            serverFutures.add(serverFuture);
//        }
//
//        executor.shutdown();
//        for (FutureTask<Void> serverFuture : serverFutures) {
//            serverFuture.get();
//        }
//
//        amFuture.get();
//    }

	@Test
	public void multiRegisterNode() throws Exception {
		System.out.println(SysUtil._FUNC_());
		final int numWorker = 3;
		ExecutorService executor = new ThreadPoolExecutor(numWorker, numWorker, 60, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(numWorker));
		MLConfig tfConfig = DummyContext.createDummyMLConfig();
		tfConfig.setRoleNum(new WorkerRole().name(), numWorker);
		FutureTask<Void> amFuture = startAMServer(tfConfig);

		List<FutureTask<Void>> serverFutures = new ArrayList<>(numWorker);
		// start workers
		for (int i = 0; i < numWorker; i++) {
			MLContext tfContext = new MLContext(ExecutionMode.TRAIN, tfConfig, new WorkerRole().name(), i, null, null);
			NodeServiceGrpc.NodeServiceImplBase service = mockNodeService();
			DummyNodeServer server = new DummyNodeServer(tfContext, service);
			final int index = i;
			FutureTask<Void> serverFuture = new FutureTask<>(() -> {
				Assert.assertEquals(server.mlContext.getIdentity() + " register node failed",
						RpcCode.OK.ordinal(), server.registerNode().getCode());
				if(0 == index) {
					Assert.assertEquals(server.mlContext.getIdentity() + " re-register node failed",
							RpcCode.OK.ordinal(), server.twiceRegisterNode().getCode());
				}else {
					server.waitForAMStatus(AMStatus.AM_RUNNING);
				}
				Assert.assertEquals(server.mlContext.getIdentity() + " register node failed",
						RpcCode.OK.ordinal(), server.registerNode().getCode());
				Assert.assertEquals(server.mlContext.getIdentity() + " finish node failed",
						RpcCode.OK.ordinal(), server.finishNode().getCode());
				server.close();
				// each worker should be restarted only once
				verify(service).nodeRestart(any(), any());
			}, null);
			executor.submit(serverFuture);
			serverFutures.add(serverFuture);
		}

		executor.shutdown();
		for (FutureTask<Void> serverFuture : serverFutures) {
			serverFuture.get();
		}

		amFuture.get();
	}


	@Test
	public void testMergeFinishedNodes() throws Exception {
		System.out.println(SysUtil._FUNC_());
		MLConfig mlConfig = DummyContext.createDummyMLConfig();
		mlConfig.setRoleNum(new WorkerRole().name(), 3);
		mlConfig.setRoleNum(new PsRole().name(), 1);
		mlConfig.addProperty(MLConstants.CONFIG_JOB_HAS_INPUT, "true");
		FutureTask<Void> amFuture = startAMServer(mlConfig);

		NodeServiceGrpc.NodeServiceImplBase dummyService = mockNodeService();

		MLContext psContext = new MLContext(ExecutionMode.TRAIN, mlConfig, new PsRole().name(), 0, null, null);
		MLContext workerContext0 = new MLContext(ExecutionMode.TRAIN, mlConfig, new WorkerRole().name(), 0, null, null);
		MLContext workerContext1 = new MLContext(ExecutionMode.TRAIN, mlConfig, new WorkerRole().name(), 1, null, null);
		MLContext workerContext2 = new MLContext(ExecutionMode.TRAIN, mlConfig, new WorkerRole().name(), 2, null, null);
		DummyNodeServer psServer = new DummyNodeServer(psContext, dummyService);
		DummyNodeServer workerServer0 = new DummyNodeServer(workerContext0, dummyService);
		DummyNodeServer workerServer1 = new DummyNodeServer(workerContext1, dummyService);
		DummyNodeServer workerServer2 = new DummyNodeServer(workerContext2, dummyService);


		psServer.registerNode();
		workerServer0.registerNode();
		workerServer1.registerNode();
		workerServer2.registerNode();

		long firstVersion = psServer.version;

		// worker 0 finishes
		workerServer1.finishNode();
		workerServer1.close();
		// worker 1 triggers failover
		workerServer2.failNode();
		// re-register the nodes
		workerServer0.registerNode();
		psServer.registerNode();
		workerServer2.registerNode();
		verify(dummyService, times(3)).nodeRestart(any(), any());

		Assert.assertNotEquals("Version is not updated", firstVersion, workerServer2.version);

		MLClusterDef clusterDef = workerServer0.getCluster();
		Assert.assertEquals("There should be 2 jobs", 2, clusterDef.getJobList().size());
		MLJobDef workerDef = null;
		for (MLJobDef jobDef : clusterDef.getJobList()) {
			if (jobDef.getName().equals(new WorkerRole().name())) {
				workerDef = jobDef;
				break;
			}
		}
		Assert.assertNotNull("Worker job not found", workerDef);
		Assert.assertEquals("There should be 2 tasks in worker job", 3, workerDef.getTasksCount());


		workerServer0.finishNode();
		workerServer0.close();
		workerServer2.finishNode();
		workerServer2.close();
		psServer.finishNode();
		psServer.close();
		amFuture.get();
	}

	private FutureTask<Void> startAMServer(MLConfig mlConfig) throws MLException {
		MLContext amContext = new MLContext(ExecutionMode.TRAIN, mlConfig, new AMRole().name(), 0, null, null);
		AppMasterServer amServer = new AppMasterServer(amContext);
		FutureTask<Void> amFuture = new FutureTask<>(amServer, null);
		Thread thread = new Thread(amFuture);
		thread.setDaemon(true);
		thread.start();
		return amFuture;
	}

	private NodeServiceGrpc.NodeServiceImplBase mockNodeService() {
		NodeServiceGrpc.NodeServiceImplBase dummyService = mock(NodeServiceGrpc.NodeServiceImplBase.class);
		doAnswer(i -> {
			NodeStopResponse response = NodeStopResponse.newBuilder().setCode(RpcCode.OK.ordinal())
					.setMessage("").build();
			StreamObserver<NodeStopResponse> responseObserver = (StreamObserver<NodeStopResponse>) i.getArguments()[1];
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return null;
		}).when(dummyService).nodeStop(any(NodeStopRequest.class), any(StreamObserver.class));
		doAnswer(i -> {
			NodeRestartResponse restartResponse = NodeRestartResponse.newBuilder()
					.setCode(RpcCode.OK.ordinal()).setMessage("").build();
			StreamObserver<NodeRestartResponse> responseObserver = (StreamObserver<NodeRestartResponse>) i
					.getArguments()[1];
			responseObserver.onNext(restartResponse);
			responseObserver.onCompleted();
			return null;
		}).when(dummyService).nodeRestart(any(NodeRestartRequest.class), any(StreamObserver.class));
		return dummyService;
	}

	public static class NodeMessage {
		int nodeStopNum = 0;
		int nodeRestartNum = 0;


		public int getNodeStopNum() {
			return nodeStopNum;
		}

		public void setNodeStopNum(int nodeStopNum) {
			this.nodeStopNum = nodeStopNum;
		}

		public void addNodeStopNum() {
			this.nodeStopNum += 1;
		}

		public int getNodeRestartNum() {
			return nodeRestartNum;
		}

		public void setNodeRestartNum(int nodeRestartNum) {
			this.nodeRestartNum = nodeRestartNum;
		}

		public void addNodeRestartNum() {
			this.nodeRestartNum += 1;
		}

		public void waitForStop(int num) {
			while (true) {
				if (num == this.nodeStopNum) {
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private NodeServiceGrpc.NodeServiceImplBase mockNodeServiceSetFlag(NodeMessage message) {
		NodeServiceGrpc.NodeServiceImplBase dummyService = mock(NodeServiceGrpc.NodeServiceImplBase.class);
		doAnswer(i -> {
			NodeStopResponse response = NodeStopResponse.newBuilder().setCode(RpcCode.OK.ordinal())
					.setMessage("").build();
			StreamObserver<NodeStopResponse> responseObserver = (StreamObserver<NodeStopResponse>) i.getArguments()[1];
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			message.addNodeStopNum();
			return null;
		}).when(dummyService).nodeStop(any(NodeStopRequest.class), any(StreamObserver.class));
		doAnswer(i -> {
			NodeRestartResponse restartResponse = NodeRestartResponse.newBuilder()
					.setCode(RpcCode.OK.ordinal()).setMessage("").build();
			StreamObserver<NodeRestartResponse> responseObserver = (StreamObserver<NodeRestartResponse>) i
					.getArguments()[1];
			responseObserver.onNext(restartResponse);
			responseObserver.onCompleted();
			message.addNodeRestartNum();
			return null;
		}).when(dummyService).nodeRestart(any(NodeRestartRequest.class), any(StreamObserver.class));
		return dummyService;
	}
}
