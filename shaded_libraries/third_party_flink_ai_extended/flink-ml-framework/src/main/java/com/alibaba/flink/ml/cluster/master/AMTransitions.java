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

import com.alibaba.flink.ml.cluster.rpc.AppMasterServer;
import com.alibaba.flink.ml.proto.*;
import com.alibaba.flink.ml.cluster.rpc.NodeClient;
import com.alibaba.flink.ml.cluster.statemachine.InvalidStateTransitionException;
import com.alibaba.flink.ml.cluster.statemachine.transition.MultipleArcTransition;
import com.alibaba.flink.ml.cluster.statemachine.transition.SingleArcTransition;
import com.alibaba.flink.ml.util.ProtoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is a default class implement some common transitions.
 */
public class AMTransitions {
	private static final Logger LOG = LoggerFactory.getLogger(AMTransitions.class);


	/**
	 * do nothing transition, only ignore am event.
	 */
	public static class IgnoreMessage extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent> {


		public IgnoreMessage(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent) {
			LOG.info("ignore event :" + amEvent.toString() + " current status:" + getInternalState().toString());
		}
	}

	/**
	 * receive COMPLETE_CLUSTER message and change am status from init to running.
	 */
	public static class CompleteCluster extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent>{

		public CompleteCluster(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine abstractAMStateMachine, AMEvent amEvent)
				throws InvalidStateTransitionException {
			try {
				amMeta.saveAMStatus(AMStatus.AM_RUNNING, AMStatus.AM_INIT);
			} catch (IOException e) {
				e.printStackTrace();
				throw new InvalidStateTransitionException(abstractAMStateMachine.getInternalState(), amEvent);
			}

		}
	}

	/**
	 * receive REGISTER_NODE and FAIL_NODE am event, change am state to AM_FAILOVER, then send restart cluster event to
	 * state machine.
	 */
	public static class FailNode extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent> {

		public FailNode(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent)
				throws InvalidStateTransitionException {
			NodeSpec nodeSpec;
			long version = 0;
			if (amEvent.getType().equals(AMEventType.FAIL_NODE)) {
				RegisterFailedNodeRequest request = (RegisterFailedNodeRequest) amEvent.getMessage();
				nodeSpec = request.getNodeSpec();
				version = request.getVersion();
			} else if (amEvent.getType().equals(AMEventType.REGISTER_NODE)) {
				RegisterNodeRequest request = (RegisterNodeRequest) amEvent.getMessage();
				nodeSpec = request.getNodeSpec();
				version = request.getVersion();
			} else {
				throw new InvalidStateTransitionException(amStateMachine.getInternalState(), amEvent);
			}
			LOG.info("Fail Node:" + ProtoUtil.protoToJson(nodeSpec));
			try {
				amMeta.saveFailedNode(nodeSpec);
				amMeta.saveAMStatus(AMStatus.AM_FAILOVER, AMStatus.AM_RUNNING);
				if (eventReporter != null) {
					eventReporter.nodeFail(nodeSpec2Str(nodeSpec));
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new InvalidStateTransitionException(amStateMachine.getInternalState(), amEvent);
			}
			AMEvent restartEvent = new AMEvent(AMEventType.RESTART_CLUSTER, null, version);
			LOG.info("put restart event to state machine:" + version);
			boolean res = stateMachine.sendEvent(restartEvent);
			if (!res) {
				throw new InvalidStateTransitionException(amStateMachine.getInternalState(), amEvent);
			}
		}
	}

	/**
	 * receive RESTART_CLUSTER event message,then notify all node restart and change am state from AM_FAILOVER to AM_INIT.
	 */
	public static class RestartCluster extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent> {


		public RestartCluster(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent) {
			try {
				amService.restartAllNodes();
				amMeta.cleanCluster();
				amMeta.saveAMStatus(AMStatus.AM_INIT, AMStatus.AM_FAILOVER);
				if (eventReporter != null) {
					eventReporter.jobFailover();
				}
			} catch (Exception e) {
				throw new RuntimeException("Restart cluster failed", e);
			}
		}
	}

	/**
	 * set am state machine state to AM_FINISH.
	 */
	public static class StopJob extends AMTransition implements SingleArcTransition<AbstractAMStateMachine, AMEvent> {

		public StopJob(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent)
				throws InvalidStateTransitionException {
			amService.stopAllNodes();
			try {
				amMeta.saveAMStatus(AMStatus.AM_FINISH, getInternalState());
			} catch (IOException e) {
				e.printStackTrace();
				throw new InvalidStateTransitionException(getInternalState(), amEvent);
			}
			amService.stopService();
			if (eventReporter != null) {
				eventReporter.jobKill();
			}
		}
	}

	/**
	 * handle FINISH_NODE event, if all node finish then send FINISH_CLUSTER event to am state machine.
	 */
	public static class FinishNode extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent> {

		public FinishNode(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent)
				throws InvalidStateTransitionException {
			FinishNodeRequest request = (FinishNodeRequest) amEvent.getMessage();
			LOG.info("Finish Node:" + ProtoUtil.protoToJson(request.getNodeSpec()));
			try {
				if (eventReporter != null) {
					eventReporter.nodeFinish(nodeSpec2Str(request.getNodeSpec()));
				}
				amMeta.saveFinishNodeSpec(request.getNodeSpec());
			} catch (IOException e) {
				e.printStackTrace();
				throw new InvalidStateTransitionException(getInternalState(), amEvent);
			}

			Map<String, Integer> remainJobNumMap = updateRemainJobNum(amEvent);
			boolean flag = true;
			for (Map.Entry<String, Integer> entry : remainJobNumMap.entrySet()) {
				if (0 == entry.getValue()) {
					//pass
				} else {
					flag = false;
					break;
				}
			}
			if (flag) {
				LOG.info("send finish cluster event!");
				AMEvent finishEvent = new AMEvent(AMEventType.FINISH_CLUSTER, "", request.getVersion());
				amStateMachine.sendEvent(finishEvent);
			}
		}
	}

	/**
	 * handle FINISH_CLUSTER event, change am state to finish.
	 */
	public static class FinishCluster extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent>{

		public FinishCluster(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public void transition(AbstractAMStateMachine abstractAMStateMachine, AMEvent amEvent)
				throws InvalidStateTransitionException {
			amService.stopAllNodes();
			doJobFinish(amEvent);
		}

		public void doJobFinish(AMEvent amEvent) throws InvalidStateTransitionException {
			if (eventReporter != null) {
				eventReporter.jobFinish();
			}
			try {
				amMeta.saveAMStatus(AMStatus.AM_FINISH, AMStatus.AM_RUNNING);
			} catch (IOException e) {
				e.printStackTrace();
				throw new InvalidStateTransitionException(getInternalState(), amEvent);
			}
			LOG.info("Job finished, shutting down AM");
			amService.stopService();
		}
	}

	/**
	 * handle register node event and save node information. if all node information collect, send COMPLETE_CLUSTER
	 * event to am state machine.
	 */
	public static class RegisterNode extends AMTransition
			implements SingleArcTransition<AbstractAMStateMachine, AMEvent> {

		public RegisterNode(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public synchronized void transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent)
				throws InvalidStateTransitionException {
			RegisterNodeRequest request = (RegisterNodeRequest) amEvent.getMessage();
			LOG.info("Register Node:" + ProtoUtil.protoToJson(request.getNodeSpec()));
			try {
				if (eventReporter != null) {
					eventReporter.nodeRegister(nodeSpec2Str(request.getNodeSpec()));
				}
				MLClusterDef clusterDef = amMeta.saveNodeSpec(request.getNodeSpec());
				Map<String, Integer> currentJobNumMap = new HashMap<>();
				for (MLJobDef jobDef : clusterDef.getJobList()) {
					currentJobNumMap.put(jobDef.getName(), jobDef.getTasksCount());
				}
				Map<String, Integer> remainJobNumMap = updateRemainJobNum(amEvent);
				boolean flag = true;
				for (Map.Entry<String, Integer> entry : remainJobNumMap.entrySet()) {
					if (currentJobNumMap.getOrDefault(entry.getKey(), 0).intValue()
							== entry.getValue().intValue()) {
						//pass
					} else {
						flag = false;
						break;
					}
				}
				if (flag) {
					long version = request.getVersion();
					AMEvent completeEvent = new AMEvent(AMEventType.COMPLETE_CLUSTER,"", version);
					LOG.info("put complete event to state machine:" + version);
					stateMachine.sendEvent(completeEvent);
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new InvalidStateTransitionException(getInternalState(), amEvent);
			}
		}
	}

	/**
	 * check if node in given cluster.
	 * @param nodeSpec given node info.
	 * @param clusterDef cluster information.
	 * @return true: node in cluster, false: node not in cluster.
	 */
	public static boolean inFlinkCluster(NodeSpec nodeSpec, MLClusterDef clusterDef) {
		if (null == clusterDef) {
			return false;
		}
		boolean res = false;
		for (MLJobDef jobDef : clusterDef.getJobList()) {
			if (jobDef.getName().equals(nodeSpec.getRoleName())
					&& jobDef.containsTasks(nodeSpec.getIndex())) {
				res = true;
				break;
			}
		}
		return res;
	}

	/**
	 * init am state machine at am rpc amService start time.
	 */
	public static class InitAmState extends AMTransition
			implements MultipleArcTransition<AbstractAMStateMachine, AMEvent, AMStatus> {

		public InitAmState(AbstractAMStateMachine stateMachine) {
			super(stateMachine);
		}

		@Override
		public AMStatus transition(AbstractAMStateMachine amStateMachine, AMEvent amEvent) {
			try {
				/**
				 * restore version
				 */
				long version = amMeta.restoreClusterVersion();
				LOG.info("restore am version:" + version);
				if (0 == version) {
					version = System.currentTimeMillis();
				}
				amService.setVersion(version);
				amMeta.saveClusterVersion(version);
				LOG.info("current version:" + version);

				AMStatus amStatus = amMeta.restoreAMStatus();
				if (null == amStatus || AMStatus.AM_UNKNOW == amStatus || AMStatus.UNRECOGNIZED == amStatus) {
					amMeta.saveAMStatus(AMStatus.AM_INIT, AMStatus.AM_UNKNOW);
					return AMStatus.AM_INIT;
				} else if(AMStatus.AM_FAILOVER == amStatus){
					AMEvent restartAmEvent = new AMEvent(AMEventType.RESTART_CLUSTER, "", version);
					amStateMachine.sendEvent(restartAmEvent);
				} else if(AMStatus.AM_FINISH == amStatus){
					if (eventReporter != null) {
						eventReporter.jobKill();
					}
					amService.stopService();
				} else {
					/**
					 * restore client cache
					 * restore monitor
					 */
					MLClusterDef flinkClusterDef = amMeta.restoreClusterDef();
					if (null != flinkClusterDef) {
						MLClusterDef finishClusterDef = amMeta.restoreFinishClusterDef();
						for (MLJobDef jobDef : flinkClusterDef.getJobList()) {
							for (NodeSpec nodeSpec : jobDef.getTasksMap().values()) {
								if (inFlinkCluster(nodeSpec, finishClusterDef)) {
									continue;
								}
								/**
								 * client cache
								 */
								NodeClient client = new NodeClient(nodeSpec.getIp(), nodeSpec.getClientPort());
								String key = AppMasterServer.getNodeClientKey(nodeSpec);
								amService.updateNodeClient(key, client);
								/**
								 * monitor
								 */
								amService.startHeartBeatMonitor(nodeSpec, version);
							}
						}
						LOG.info("recover client cache and monitor!");
					}
				}
				return amStatus;
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage());
			}
		}
	}
}
