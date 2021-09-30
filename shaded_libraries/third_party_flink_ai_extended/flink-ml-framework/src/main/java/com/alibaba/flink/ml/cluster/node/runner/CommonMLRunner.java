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

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.rpc.AMClient;
import com.alibaba.flink.ml.cluster.rpc.AMRegistry;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.cluster.rpc.RpcCode;
import com.alibaba.flink.ml.proto.AMStatus;
import com.alibaba.flink.ml.proto.GetClusterInfoResponse;
import com.alibaba.flink.ml.proto.MLClusterDef;
import com.alibaba.flink.ml.proto.NodeSpec;
import com.alibaba.flink.ml.proto.SimpleResponse;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import com.alibaba.flink.ml.util.ProtoUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * a common ml script runner implement MLRunner.
 */
public class CommonMLRunner implements MLRunner {
	private static Logger LOG = LoggerFactory.getLogger(CommonMLRunner.class);
	protected volatile AMClient amClient;
	protected NodeSpec nodeSpec;
	protected long version = 0;
	protected String localIp;
	protected NodeServer server;
	protected volatile MLContext mlContext;
	protected ScriptRunner scriptRunner;
	//the execution result of this thread
	protected ExecutionStatus resultStatus;
	protected ExecutionStatus currentResultStatus;
	protected ExecutorService heartbeatService;

	protected MLClusterDef mlClusterDef;


	public CommonMLRunner(MLContext mlContext, NodeServer server) {
		this.mlContext = mlContext;
		this.server = server;
	}

	protected boolean doRegisterAction() throws Exception {
		createNodeSpec(true);
		SimpleResponse response = amClient.registerNode(version, nodeSpec);
		if (RpcCode.OK.ordinal() == response.getCode()) {
			return true;
		}
		if (RpcCode.VERSION_ERROR.ordinal() == response.getCode()) {
			throw new MLException(mlContext.getIdentity() + " version mismatch with AM");
		}
		LOG.warn("register to master failed code :" + response.getCode()
				+ " message:" + response.getMessage());
		LOG.error("Fail to register node. This node is " + localIp + ":" + server.getPort()
				+ ", am server is " + amClient.getHost() + ":" + amClient.getPort());
		return false;
	}

	/**
	 * runner register node information to application master.
	 * @throws Exception
	 */
	@Override
	public void registerNode() throws Exception {
		long startTime = System.currentTimeMillis();
		// we can register while running if
		// the failover strategy only restart individual tasks or this is tensorboard
		final boolean registerWhileRunning = mlContext.getProperties().getOrDefault(
				MLConstants.FAILOVER_STRATEGY, MLConstants.FAILOVER_STRATEGY_DEFAULT)
				.equalsIgnoreCase(MLConstants.FAILOVER_RESTART_INDIVIDUAL_STRATEGY);
		doRegisterAction(startTime, registerWhileRunning);
		return;
	}

	protected void doRegisterAction(long startTime, boolean registerWhileRunning)
			throws Exception {
		while (true) {
			checkEnd();
			AMStatus amStatus = amClient.getAMStatus();
			LOG.info(mlContext.getIdentity() + " registerNode status:" + amStatus.toString());
			checkEnd();
			switch (amStatus) {
				case AM_INIT: {
					if (doRegisterAction()) {
						return;
					}
					break;
				}
				case AM_RUNNING:
					if (registerWhileRunning) {
						if (doRegisterAction()) {
							return;
						}
						break;
					}
				case AM_UNKNOW:
				case AM_FAILOVER: {
					LOG.warn("master status is {} wait for INIT!", amStatus.toString());
					break;
				}
				default: {
					throw new RuntimeException("AM status is " + amStatus.toString() + " can not register node!");
				}
			}
			Thread.sleep(3000);
			if (System.currentTimeMillis() - startTime > MLConstants.TIMEOUT) {
				throw new MLException(mlContext.getIdentity() + " timed out registering to AM");
			}
		}
	}

	protected NodeSpec createNodeSpec(boolean reset) throws Exception {
		if (reset || (null == nodeSpec)) {
			nodeSpec = NodeSpec.newBuilder()
					.setIp(localIp)
					.setIndex(mlContext.getIndex())
					.setClientPort(server.getPort())
					.setRoleName(mlContext.getRoleName())
					.build();
		}
		return nodeSpec;
	}

	/**
	 * get cluster information from application master.
	 * @throws MLException
	 * @throws InterruptedException
	 */
	@Override
	public void getClusterInfo() throws MLException, InterruptedException {
		long startTime = System.currentTimeMillis();
		while (true) {
			if (System.currentTimeMillis() - startTime > MLConstants.TIMEOUT) {
				break;
			}
			checkEnd();
			GetClusterInfoResponse response = amClient.getClusterInfo(version);
			if (RpcCode.OK.ordinal() == response.getCode()) {
				mlClusterDef = response.getClusterDef();
				return;
			} else {
				LOG.info("wait for cluster info:" + response.getCode() + " message:" + response.getMessage());
				Thread.sleep(3000);
			}
		}
		mlClusterDef = null;
	}

	protected void checkEnd() throws MLException {
		if (resultStatus == ExecutionStatus.KILLED_BY_FLINK) {
			throw new FlinkKillException("Exit per request.");
		}
	}

	/**
	 * machine learning runner step:
	 * 1. init application master client.
	 * 2. get current version from application master.
	 * 3. register node to application master.
	 * 4. start heart beat thread.
	 * 5. wait for cluster to running status.
	 * 6. get cluster information from application master.
	 * 7. set machine learning current context.
	 * 8. run machine learning process.
	 */
	@Override
	public void run() {
		resultStatus = ExecutionStatus.RUNNING;
		currentResultStatus = ExecutionStatus.RUNNING;
		try {
			//1. init amClient
			initAMClient();
			checkEnd();
			LOG.info("init amClient.");
			//2. get version from master
			getCurrentJobVersion();
			checkEnd();
			LOG.info("get current job version.");

			getTaskIndex();
			LOG.info("get task index.");

			//3. register to master
			registerNode();
			checkEnd();
			LOG.info("register node to application master.");
			//4. start heart beat
			startHeartBeat();
			LOG.info("start heart beat thread.");
			//5. wait for cluster running
			waitClusterRunning();
			LOG.info("wait for cluster to running status.");
			//6. get cluster
			getClusterInfo();
			Preconditions.checkNotNull(mlClusterDef, "Cannot get cluster def from AM");
			checkEnd();
			LOG.info("get cluster info.");
			//7. set  machine learning context
			resetMLContext();
			checkEnd();
			LOG.info("reset machine learning context.");
			//8. run python script
			runScript();
			checkEnd();
			LOG.info("run script.");
			currentResultStatus = ExecutionStatus.SUCCEED;
		} catch (Exception e) {
			if (e instanceof FlinkKillException || e instanceof InterruptedException) {
				LOG.info("{} killed by flink.", mlContext.getIdentity());
				currentResultStatus = ExecutionStatus.KILLED_BY_FLINK;
			} else {
				//no one ask for this thread to stop, thus there must be some error occurs
				LOG.error("Got exception during python running", e);
				mlContext.addFailNum();
				currentResultStatus = ExecutionStatus.FAILED;
			}
		} finally {
			stopExecution(currentResultStatus == ExecutionStatus.SUCCEED);
			// set resultStatus value after node notified to am.
			resultStatus = currentResultStatus;
		}
	}

	@Override
	public void runScript() throws Exception {
		scriptRunner = ScriptRunnerFactory.getScriptRunner(mlContext);
		scriptRunner.runScript();
	}

	@Override
	public void resetMLContext() {
		String clusterStr = ProtoUtil.protoToJson(mlClusterDef);
		LOG.info("java cluster:" + clusterStr);
		mlContext.getProperties().put(MLConstants.CONFIG_CLUSTER_PATH, clusterStr);
		mlContext.setNodeServerIP(localIp);
		mlContext.setNodeServerPort(server.getPort());
	}

	@Override
	public void startHeartBeat() throws Exception{
		heartbeatService = Executors.newFixedThreadPool(1, r -> {
			Thread heartbeat = new Thread(r);
			heartbeat.setName(mlContext.getIdentity() + "-HeartBeat");
			heartbeat.setDaemon(true);
			return heartbeat;
		});
		heartbeatService.submit(new NodeHeartBeatRunner(mlContext, server, nodeSpec, version));
	}

	protected void stopHeartBeat(){
		if(null != heartbeatService && (!heartbeatService.isShutdown())){
			heartbeatService.shutdownNow();
			try {
				heartbeatService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOG.warn("stop heart beat exception:" + e.getMessage());
			}
		}
	}

	@Override
	public void getCurrentJobVersion() {
		version = amClient.getVersion().getVersion();
	}

	@Override
	public void initAMClient() throws Exception{
		localIp = IpHostUtil.getIpAddress();
		checkEnd();
		amClient = AMRegistry.getAMClient(mlContext);
		LOG.info("{} at {}:{}, am server at {}:{}", mlContext.getIdentity(), localIp, server.getPort(),
				amClient.getHost(), amClient.getPort());
	}

	@Override
	public void waitClusterRunning() throws InterruptedException, MLException {
		long startTime = System.currentTimeMillis();
		while (true) {
			AMStatus status = amClient.getAMStatus();
			if (AMStatus.AM_RUNNING == status) {
				break;
			}
			Thread.sleep(5000);
			checkEnd();
			if (System.currentTimeMillis() - startTime > MLConstants.TIMEOUT) {
				throw new MLException("Timed out waiting for job to start running");
			}
		}
	}

	public void getTaskIndex() throws MLException, InterruptedException {
		//set task index if current one is invalid (in case when TFTableFunction is used
		// where FunctionContext missing the getIndexOfThisSubtask().
		if (mlContext.getIndex() < 0) {
			String key;
			try {
				key = IpHostUtil.getIpAddress() + ":" + server.getPort();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
			int taskIndex = amClient.getTaskIndex(version, mlContext.getRoleName(), key);
			mlContext.setIndex(taskIndex);
			checkEnd();
		}
	}

	protected void stopExecution(boolean success) {
		if (scriptRunner != null) {
			IOUtils.closeQuietly(scriptRunner);
			scriptRunner = null;
		}
		stopHeartBeat();
		notifyAmWorkerFinish(success);
		if (amClient != null) {
			LOG.info("{} closing AM connection", mlContext.getIdentity());
			amClient.close();
			amClient = null;
		}
		if (!success) {
			mlContext.reset();
		}
	}


	protected void notifyAmWorkerFinish(boolean success) {
		if (amClient == null || nodeSpec == null) {
			return;
		}
		SimpleResponse response;
		try {
			// for PS node, being killed can mean success
			if (success) {
				LOG.info("report node finish:" + mlContext.getIdentity());
				response = amClient.nodeFinish(version,
						nodeSpec);
				if (RpcCode.OK.ordinal() != response.getCode() && RpcCode.VERSION_ERROR.ordinal() != response
						.getCode()) {
					LOG.error("Fail to report node finish status to AM.");
				}
			} else {
				if (currentResultStatus == ExecutionStatus.FAILED) {
					response = amClient.reportFailedNode(version, nodeSpec);
					LOG.info("report failed node:" + mlContext.getIdentity());
					if (RpcCode.OK.ordinal() != response.getCode()) {
						LOG.error("Fail to report node failed status to AM.");
					}
				} else if (currentResultStatus == ExecutionStatus.KILLED_BY_FLINK) {
					// do nothing
				}
			}
		} catch (Exception e) {
			LOG.error(mlContext.getIdentity() + " failed to notify AM of finished node", e);
			throw new RuntimeException(e);
		}finally {
			amClient.close();
		}
	}

	@Override
	public ExecutionStatus getResultStatus() {
		return resultStatus;
	}

	//called by other thread
	@Override
	public void notifyStop(){
		stopHeartBeat();
		if (scriptRunner != null) {
			scriptRunner.notifyKillSignal();
		}
		resultStatus = ExecutionStatus.KILLED_BY_FLINK;
	}
}

