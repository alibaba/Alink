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

import com.alibaba.flink.ml.cluster.BaseEventReporter;
import com.alibaba.flink.ml.cluster.master.AMEvent;
import com.alibaba.flink.ml.cluster.master.AMEventType;
import com.alibaba.flink.ml.cluster.master.AMService;
import com.alibaba.flink.ml.cluster.master.AMStateMachineFactory;
import com.alibaba.flink.ml.cluster.master.AbstractAMStateMachine;
import com.alibaba.flink.ml.cluster.master.HeartbeatListener;
import com.alibaba.flink.ml.cluster.master.HeartbeatMonitor;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.master.meta.AMMetaImpl;
import com.alibaba.flink.ml.cluster.master.meta.AMMeta;
import com.alibaba.flink.ml.proto.*;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.util.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * application master rpc server.
 *      management machine learning cluster life cycle.
 *      accept node register, finish, failed etc.
 */
public class AppMasterServer implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(AppMasterServer.class);
	private Server server;
	private MLContext mlContext;
	private AtomicBoolean end;
	private AMMeta amMeta;
	private AbstractAMStateMachine amStateMachine;
	private AMService appMasterService;
	private BaseEventReporter eventReporter;
	private volatile long rpcLastContact;
	private final long rpcContactTimeout;
	private final ScheduledExecutorService scheduledExecutor;
	private final Duration heartbeatTimeout;
	private volatile Throwable error = null;

	public AppMasterServer(MLContext mlContext) {
		this.mlContext = mlContext;
		rpcContactTimeout = Long.valueOf(mlContext.getProperties().getOrDefault(
				MLConstants.SERVER_RPC_CONTACT_TIMEOUT, MLConstants.SERVER_RPC_CONTACT_TIMEOUT_DEFAULT));
		heartbeatTimeout = Duration.ofMillis(Long.valueOf(mlContext.getProperties().getOrDefault(
				MLConstants.HEARTBEAT_TIMEOUT, MLConstants.HEARTBEAT_TIMEOUT_DEFAULT)));
		Map<String, String> properties = mlContext.getProperties();
		if (properties.containsKey(MLConstants.AM_STATE_CLASS)) {
			String className = properties.get(MLConstants.AM_STATE_CLASS);
			Class[] classes = new Class[1];
			classes[0] = MLContext.class;
			Object[] objects = new Object[1];
			objects[0] = this.mlContext;
			try {
				this.amMeta = ReflectUtil.createInstance(className, classes, objects);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		} else {
			this.amMeta = new AMMetaImpl(mlContext);
		}
		// If we use the restart-all strategy, we should clear the old states and start all over again
		// TODO: use ephemeral ZK node?
		if (MLConstants.FAILOVER_RESTART_ALL_STRATEGY.equalsIgnoreCase(
				mlContext.getProperties().getOrDefault(MLConstants.FAILOVER_STRATEGY,
						MLConstants.FAILOVER_STRATEGY_DEFAULT))) {
			amMeta.clear();
		}
		this.end = new AtomicBoolean(false);

		// eventReporter
		if (properties.containsKey(MLConstants.CONFIG_EVENT_REPORTER)) {
			String className = properties.get(MLConstants.CONFIG_EVENT_REPORTER);
			try {
				eventReporter = ReflectUtil.createInstance(className, new Class[0], new Object[0]);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			eventReporter = new LogBaseEventReporter();
		}

		if (eventReporter != null) {
			String jobName = properties.getOrDefault(MLConstants.CONFIG_JOB_NAME, "flink-ml");
			jobName += properties.get(MLConstants.JOB_VERSION);
			eventReporter.configure(jobName, properties);
		}

		this.appMasterService = new AppMasterServiceImpl();

		// am state machine
		try {
			amStateMachine = AMStateMachineFactory
					.getAMStateMachine(appMasterService, amMeta, mlContext, eventReporter);
		} catch (MLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		int nodeNumSum = 0;
		for (Integer i : mlContext.getRoleParallelismMap().values()) {
			nodeNumSum += i;
		}
		scheduledExecutor = Executors.newScheduledThreadPool(nodeNumSum);
	}

	@Override
	public void run() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			// Use stderr here since the logger may has been reset by its JVM shutdown hook.
			if (AppMasterServer.this.server != null) {
				LOG.error("*** shutting down gRPC server since JVM is shutting down");
				AppMasterServer.this.cleanup();
				LOG.error("*** AM server shut down");
			}
		}));
		try {
			updateRpcLastContact();
			this.server = ServerBuilder.forPort(0)
					.addService((BindableService)appMasterService).build();
			this.server.start();
			this.end.set(false);
			LOG.info("App Master Server started, listening on " + server.getPort());
			amMeta.saveAMIpPort(IpHostUtil.getIpAddress(), server.getPort());
			amStateMachine.handle(new AMEvent(AMEventType.INTI_AM_STATE, null, 0));

			while (!getEnd()) {
				long duration = System.currentTimeMillis() - rpcLastContact;
				if (duration > rpcContactTimeout) {
					throw new MLException(String.format("%d seconds elapsed since last grpc contact",
							duration / 1000));
				}
				Thread.sleep(1000);
			}
			if (error != null) {
				throw new MLException("Error encountered in AM", error);
			}
		} catch (InterruptedException e) {
			LOG.warn("AM server interrupted");
		} catch (Exception e) {
			LOG.error("Fail to execute AM.");
			throw new RuntimeException(e);
		} finally {
			cleanup();
		}
	}

	private void updateRpcLastContact() {
		rpcLastContact = System.currentTimeMillis();
	}

	/** Stop serving requests and shutdown resources. */
	private void cleanup() {
		LOG.info("Clean up AM node.");
		try {
			LOG.info("before clean up am node, sleep 10 seconds to wait for node complete stop.");
			Thread.sleep(10*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (server != null) {
			server.shutdownNow();
			try {
				server.awaitTermination(2, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOG.error("Interrupted shutting down GRPC server.");
			}
			server = null;
		}
		LOG.info("stop am service!");
		if (amMeta != null) {
			amMeta.close();
		}
		LOG.info("stop am meta!");
		if (null != amStateMachine) {
			amStateMachine.close();
		}
		LOG.info("stop am state machine!");
		this.end.set(true);
		appMasterService.stopHeartBeatMonitorAllNode();
		LOG.info("stop heartbeat all nodes!");
		scheduledExecutor.shutdownNow();
		LOG.info("stop heartbeat thread pool!");
//		try {
//			scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//			LOG.warn("heart beat thread pool stop was interrupted:" + e.getMessage());
//		}
		LOG.info("app master server stopped.");
	}

	public int getPort() {
		if (null == server) {
			return -1;
		} else {
			return server.getPort();
		}
	}

	private boolean getEnd() {
		return end.get();
	}

	private void setEnd(boolean end) {
		this.end.set(end);
	}

	private synchronized void onError(Throwable t) {
		if (error != null) {
			error = t;
		}
		setEnd(true);
	}

	/**
	 * @param nodeSpec node information.
	 * @return node identity.
	 */
	public static String getNodeClientKey(NodeSpec nodeSpec) {
		return nodeSpec.getRoleName() + "_" + nodeSpec.getIndex();
	}

	/**
	 * application master service.
	 *      interactive with application client.
	 */
	public class AppMasterServiceImpl extends AppMasterServiceGrpc.AppMasterServiceImplBase implements AMService {
		private volatile long version = 0;
		private volatile Map<String, NodeClient> nodeClientCache = new ConcurrentHashMap<>();
		private volatile Map<String, Map<String, Integer>> nodeIndexMap = new ConcurrentHashMap<>();
		private final Map<String, HeartbeatMonitor> heartbeatMonitors = new ConcurrentHashMap<>();
		private volatile boolean isRestart = false;

		@Override
		public long version() {
			return version;
		}

		@Override
		public void setVersion(long version) {
			this.version = version;
		}


		public AppMasterServiceImpl() {
		}


		/**
		 * handle node register request,collect node description to create cluster
		 * @param request node register request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void registerNode(RegisterNodeRequest request, StreamObserver<SimpleResponse> responseObserver) {
			SimpleResponse.Builder response = SimpleResponse.newBuilder();

			if (checkVersionError(request.getVersion(), responseObserver)) {
				return;
			}
			updateRpcLastContact();
			try {
				String clientKey = getNodeClientKey(request.getNodeSpec());
				NodeClient client = nodeClientCache.getOrDefault(clientKey, null);
				if (null != client) {
					client.close();
				}
				client = new NodeClient(request.getNodeSpec().getIp(),
						request.getNodeSpec().getClientPort());
				nodeClientCache.put(clientKey, client);
				LOG.info("register node:" + clientKey);
				startHeartBeatMonitor(request.getNodeSpec(), request.getVersion());
				amStateMachine.handle(new AMEvent(AMEventType.REGISTER_NODE, request, request.getVersion()));
				response.setCode(RpcCode.OK.ordinal());
				response.setMessage("");
			} catch (Exception e) {
				response.setCode(RpcCode.ERROR.ordinal());
				response.setMessage(e.getMessage());
				handleStateTransitionError(request, e);
			}
			responseObserver.onNext(response.build());
			responseObserver.onCompleted();

		}

		@Override
		public void startHeartBeatMonitor(NodeSpec nodeSpec, long version) {
			String clientKey = getNodeClientKey(nodeSpec);

			HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(
					new HeartbeatListenerImpl(nodeSpec, version));
			heartbeatMonitor.updateTimeout(heartbeatTimeout, scheduledExecutor);
			heartbeatMonitors.put(clientKey, heartbeatMonitor);
			LOG.info("Started monitoring heartbeat for {}", clientKey);
		}

		@Override
		public void stopService() {
			setEnd(true);
		}


		/**
		 * handle node heartbeat request
		 * @param request node heartbeat request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void heartBeatNode(HeartBeatRequest request, StreamObserver<SimpleResponse> responseObserver) {
			if (!isRestart) {
				if (checkVersionError(request.getVersion(), responseObserver)) {
					return;
				}
			}
			updateRpcLastContact();
			HeartbeatMonitor monitor = heartbeatMonitors.get(getNodeClientKey(request.getNodeSpec()));
			if (monitor != null) {
				monitor.updateTimeout(heartbeatTimeout, scheduledExecutor);
			}
			SimpleResponse.Builder builder = SimpleResponse.newBuilder();
			builder.setCode(RpcCode.OK.ordinal()).setMessage("");
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		/**
		 * handle node finish request
		 * @param request node finish request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void nodeFinish(FinishNodeRequest request, StreamObserver<SimpleResponse> responseObserver) {
			if (checkVersionError(request.getVersion(), responseObserver)) {
				return;
			}
			updateRpcLastContact();

			SimpleResponse.Builder response = SimpleResponse.newBuilder();
			try {
				NodeClient client = nodeClientCache.remove(getNodeClientKey(request.getNodeSpec()));
				if (client != null) {
					client.close();
				}
				stopHeartBeatMonitorNode(getNodeClientKey(request.getNodeSpec()));
				amStateMachine.handle(new AMEvent(AMEventType.FINISH_NODE, request, request.getVersion()));
				response.setCode(RpcCode.OK.ordinal());
				response.setMessage("");
			} catch (Exception e) {
				response.setCode(RpcCode.ERROR.ordinal());
				response.setMessage(e.getMessage());
				handleStateTransitionError(request, e);
			}
			responseObserver.onNext(response.build());
			responseObserver.onCompleted();
		}

		private boolean checkVersionError(long version, StreamObserver<SimpleResponse> responseObserver) {
			if (this.version != version) {
				String message = String.format("version change current:%d request:%d", this.version, version);
				SimpleResponse.Builder response = SimpleResponse.newBuilder();
				response.setCode(RpcCode.VERSION_ERROR.ordinal()).setMessage(message);
				responseObserver.onNext(response.build());
				responseObserver.onCompleted();
				return true;
			}
			return false;
		}

		/**
		 * restart cluster node.
		 * @param nodeSpec cluster node information.
		 * @throws Exception
		 */
		@Override
		public void restartNode(NodeSpec nodeSpec) throws Exception {
			String clientKey = getNodeClientKey(nodeSpec);
			NodeClient client = nodeClientCache.getOrDefault(clientKey, null);
			if (null == client) {
				client = new NodeClient(nodeSpec.getIp(), nodeSpec.getClientPort());
				nodeClientCache.put(clientKey, client);
			}
			ListenableFuture<NodeRestartResponse> future = client.restartNode();
			try {
				NodeRestartResponse response1 = future.get();
				if (response1.getCode() == RpcCode.OK.ordinal()) {
					LOG.info("restart response:" + response1.getMessage());
				} else {
					LOG.info(response1.getMessage());
					throw new Exception(response1.getMessage());
				}
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw e;
			}
			stopHeartBeatMonitorNode(clientKey);
		}

		/**
		 * restart all cluster node.
		 * @throws Exception
		 */
		@Override
		public void restartAllNodes() throws Exception {
			isRestart = true;
			version = System.currentTimeMillis();
			LOG.info("current version:" + this.version);
			List<ListenableFuture<NodeRestartResponse>> listenableFutures = new ArrayList<>();
			Set<String> toRemove = new HashSet<>();
			for (Map.Entry<String, NodeClient> client : nodeClientCache.entrySet()) {
				ListenableFuture<NodeRestartResponse> future = client.getValue().restartNode();
				listenableFutures.add(future);
				LOG.info("send restart to node:" + client.getKey());
				toRemove.add(client.getKey());
			}
			for (ListenableFuture<NodeRestartResponse> future : listenableFutures) {
				try {
					NodeRestartResponse response1 = future.get();
					if (response1.getCode() == RpcCode.OK.ordinal()) {
						LOG.info("restart response:" + response1.getMessage());
					} else {
						LOG.info(response1.getMessage());
						//throw new Exception(response1.getMessage());
					}
				} catch (ExecutionException e) {
					e.printStackTrace();
					LOG.info("restart err:" + e.getMessage());
					//throw new Exception(e);
				}
			}

			for (NodeClient client : nodeClientCache.values()) {
				client.close();
			}
			for (String key : toRemove) {
				nodeClientCache.remove(key);
				stopHeartBeatMonitorNode(key);
			}
			isRestart = false;
		}

		@Override
		public void stopHeartBeatMonitorNode(String clientKey) {
			HeartbeatMonitor monitor = heartbeatMonitors.remove(clientKey);
			if (monitor != null) {
				monitor.cancel();
				LOG.info("Stopped monitoring heartbeat for {}", clientKey);
			}
		}

		@Override
		public void stopHeartBeatMonitorAllNode() {
			for (String clientKey : heartbeatMonitors.keySet()) {
				stopHeartBeatMonitorNode(clientKey);
			}
		}


		/**
		 * stop a cluster node.
		 * @param nodeSpec node information.
		 * @throws Exception
		 */
		@Override
		public void stopNode(NodeSpec nodeSpec) throws Exception {
			String clientKey = getNodeClientKey(nodeSpec);
			NodeClient client = nodeClientCache.getOrDefault(clientKey, null);
			if (null == client) {
				client = new NodeClient(nodeSpec.getIp(), nodeSpec.getClientPort());
				nodeClientCache.put(clientKey, client);
			}
			ListenableFuture<NodeStopResponse> future = client.stopNode();
			try {
				NodeStopResponse response1 = future.get();
				if (response1.getCode() == RpcCode.OK.ordinal()) {
					LOG.info("stop response:" + response1.getMessage());
				} else {
					LOG.info(response1.getMessage());
					throw new Exception(response1.getMessage());
				}
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw e;
			}
		}

		public void stopAllNodes() {
			if (nodeClientCache.isEmpty()) {
				return;
			}
			List<ListenableFuture<NodeStopResponse>> listenableFutures = new ArrayList<>();
			LOG.info("client size:" + nodeClientCache.size());
			for (Map.Entry<String, NodeClient> client : nodeClientCache.entrySet()) {
				ListenableFuture<NodeStopResponse> future = client.getValue().stopNode();
				listenableFutures.add(future);
				LOG.info("send stop to node:" + client.getKey());
			}
			for (ListenableFuture<NodeStopResponse> future : listenableFutures) {
				try {
					NodeStopResponse response1 = future.get();
					if (response1.getCode() == RpcCode.OK.ordinal()) {
						// do nothing
					} else {
						LOG.info(response1.getMessage());
					}
				} catch (InterruptedException | ExecutionException e) {
					//this error is expected as we are asking the TFNodeServers to stop their service.
					LOG.debug("Stop node server.", e);
				}
			}

			for (NodeClient client : nodeClientCache.values()) {
				client.close();
			}
			nodeClientCache.clear();
			stopHeartBeatMonitorAllNode();
//            version = System.currentTimeMillis();
//            LOG.info("current version:" + this.version);
		}

		private boolean checkVersionError(long version, StreamObserver responseObserver,
				GetClusterInfoResponse.Builder builder) {
			if (this.version != version) {
				String message = String.format("version change current:%d request:%d", this.version, version);
				builder.setCode(RpcCode.VERSION_ERROR.ordinal()).setMessage(message)
						.setClusterDef(MLClusterDef.newBuilder());
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				return true;
			}
			return false;
		}

		/**
		 * handle node get cluster information request
		 * @param request get cluster information request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void getClusterInfo(GetClusterInfoRequest request,
				StreamObserver<GetClusterInfoResponse> responseObserver) {
			GetClusterInfoResponse.Builder responseBuilder = GetClusterInfoResponse.newBuilder();
			if (checkVersionError(request.getVersion(), responseObserver, responseBuilder)) {
				return;
			}
			updateRpcLastContact();
			try {
				MLClusterDef clusterDef = amMeta.restoreClusterDef();
				if (null != clusterDef) {
					MLClusterDef merged = mergeFinishedClusterDef(clusterDef, amMeta.restoreFinishClusterDef());
					responseBuilder.setCode(RpcCode.OK.ordinal()).setClusterDef(merged);
				} else {
					responseBuilder.setCode(RpcCode.NOT_READY.ordinal())
							.setMessage("cluster is null!");
				}
			} catch (IOException e) {
				e.printStackTrace();
				responseBuilder.setCode(RpcCode.ERROR.ordinal());
			}
			responseObserver.onNext(responseBuilder.build());
			responseObserver.onCompleted();
		}

		/**
		 * handle node get current job version request
		 * @param request get current job version request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void getVersion(GetVersionRequest request, StreamObserver<GetVersionResponse> responseObserver) {
			updateRpcLastContact();
			GetVersionResponse.Builder builder = GetVersionResponse.newBuilder()
					.setVersion(this.version);
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		/**
		 * handle stop cluster request
		 * @param request stop cluster request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void stopAllWorker(StopAllWorkerRequest request, StreamObserver<SimpleResponse> responseObserver) {
//            if (checkVersionError(request.getVersion(), responseObserver)) {
//                return;
//            }
			updateRpcLastContact();
			SimpleResponse response = SimpleResponse.newBuilder().setMessage("").setCode(RpcCode.OK.ordinal()).build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			try {
				amStateMachine.handle(new AMEvent(AMEventType.STOP_JOB, request, request.getVersion()));
			} catch (Exception e) {
				handleStateTransitionError(request, e);
			}
		}

		/**
		 * handle node get application master status
		 * @param request get application master status request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void getAMStatus(GetAMStatusRequest request, StreamObserver<AMStatusMessage> responseObserver) {
			updateRpcLastContact();
			AMStatus status = amStateMachine.getInternalState();
			AMStatusMessage message = AMStatusMessage.newBuilder()
					.setStatus(status).build();
			responseObserver.onNext(message);
			responseObserver.onCompleted();
		}

		/**
		 * handle node report failed request
		 * @param request node report failed request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void registerFailNode(RegisterFailedNodeRequest request,
				StreamObserver<SimpleResponse> responseObserver) {
			if (checkVersionError(request.getVersion(), responseObserver)) {
				return;
			}
			updateRpcLastContact();
			SimpleResponse.Builder builder = SimpleResponse.newBuilder();
			try {
				amStateMachine.handle(new AMEvent(AMEventType.FAIL_NODE, request, request.getVersion()));
				builder.setCode(RpcCode.OK.ordinal()).setMessage("");
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
			} catch (Exception e) {
				builder.setCode(RpcCode.ERROR.ordinal()).setMessage(e.getMessage());
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				handleStateTransitionError(request, e);
			}

		}

		private boolean checkVersionError(long version, StreamObserver responseObserver,
				GetTaskIndexResponse.Builder builder) {
			if (this.version != version) {
				String message = String.format("version change current:%d request:%d", this.version, version);
				builder.setCode(RpcCode.VERSION_ERROR.ordinal()).setMessage(message).setIndex(0);
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
				return true;
			}
			return false;
		}

		/**
		 * handle node generate task index request
		 * @param request node get task index request
		 * @param responseObserver grpc response object
		 */
		@Override
		public synchronized void getTaskIndex(GetTaskIndexRequest request,
				StreamObserver<GetTaskIndexResponse> responseObserver) {
			GetTaskIndexResponse.Builder builder = GetTaskIndexResponse.newBuilder();
			if (checkVersionError(request.getVersion(), responseObserver, builder)) {
				return;
			}
			updateRpcLastContact();
			Map<String, Integer> map = nodeIndexMap
					.computeIfAbsent(request.getScope(), k -> new ConcurrentHashMap<>());
			int index = map.computeIfAbsent(request.getKey(), k -> map.size());
			builder.setIndex(index);
			builder.setCode(RpcCode.OK.ordinal());
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		/**
		 * handle node get finished nodes request
		 * @param request get finished nodes request
		 * @param responseObserver grpc response object
		 */
		@Override
		public void getFinishedNode(GetFinishedNodeRequest request,
				StreamObserver<GetFinishNodeResponse> responseObserver) {
			updateRpcLastContact();
			GetFinishNodeResponse.Builder builder = GetFinishNodeResponse.newBuilder();
			try {
				builder.setCode(0)
						.setMessage("");
				MLClusterDef clusterDef = amMeta.restoreFinishClusterDef();
				if (null != clusterDef) {
					for (MLJobDef jobDef : clusterDef.getJobList()) {
						if (jobDef.getName().equals(new WorkerRole().name())) {
							for (Integer index : jobDef.getTasksMap().keySet()) {
								builder.addWorkers(index);
							}
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				builder.setCode(1)
						.setMessage(e.getMessage());
			}
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		}

		private MLClusterDef mergeFinishedClusterDef(MLClusterDef clusterDef, MLClusterDef finishDef) {
			if (finishDef == null) {
				return clusterDef;
			}
			MLClusterDef.Builder clusterBuilder = MLClusterDef.newBuilder();
			Map<String, MLJobDef> nameToRunningDef = clusterDef.getJobList().stream().collect(
					Collectors.toMap(MLJobDef::getName, def -> def));
			for (MLJobDef finishedJob : finishDef.getJobList()) {
				if (!nameToRunningDef.containsKey(finishedJob.getName())) {
					clusterBuilder.addJob(finishedJob);
				} else {
					MLJobDef.Builder jobBuilder = MLJobDef.newBuilder();
					MLJobDef runningJobDef = nameToRunningDef.get(finishedJob.getName());
					jobBuilder.mergeFrom(runningJobDef);
					for (Integer index : finishedJob.getTasksMap().keySet()) {
						// a task may exist in both running and finished def due to race condition
						if (!runningJobDef.getTasksMap().containsKey(index)) {
							jobBuilder.putTasks(index, finishedJob.getTasksMap().get(index));
						}
					}
					clusterBuilder.addJob(jobBuilder.build());
					nameToRunningDef.remove(finishedJob.getName());
				}
			}
			clusterBuilder.addAllJob(nameToRunningDef.values());
			return clusterBuilder.build();
		}

		public void handleStateTransitionError(MessageOrBuilder request, Throwable t) {
			String msg = request != null ?
					String.format("Failed to handle request %s:\n%s",
							request.getClass().getName(), ProtoUtil.protoToJson(request)) :
					"State transition failed";
			LOG.error(msg, t);
			// currently we don't recover from state transition errors, so fail the job
			onError(t);
		}

		@Override
		public void updateNodeClient(String key, NodeClient client) {
			nodeClientCache.put(key, client);
		}

	}

	private class HeartbeatListenerImpl implements HeartbeatListener {

		private final NodeSpec nodeSpec;
		private final long version;

		HeartbeatListenerImpl(NodeSpec nodeSpec, long version) {
			this.nodeSpec = nodeSpec;
			this.version = version;
		}

		@Override
		public void notifyHeartbeatTimeout() {
			LOG.info("Lost heartbeat of {}, marking it as failed", getNodeClientKey(nodeSpec));
			RegisterFailedNodeRequest.Builder builder = RegisterFailedNodeRequest.newBuilder();
			builder.setVersion(version).setNodeSpec(nodeSpec)
					.setMessage("heartbeat timeout");
			RegisterFailedNodeRequest request = builder.build();
			try {
				// We assume the heartbeat timeout is caused by issues in BaseMLRunner, so we can go through the FAIL_NODE
				// process (i.e. ask the node to restart)
				// We rely on Flink to handle cases like network partition, where the node can't be contacted
				amStateMachine.handle(new AMEvent(AMEventType.FAIL_NODE, request, request.getVersion()));
			} catch (Exception e) {
				appMasterService.handleStateTransitionError(request, e);
			}
		}
	}
}
