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

package com.alibaba.flink.ml.tensorflow2.cluster;

import com.alibaba.flink.ml.cluster.master.AMEventType;
import com.alibaba.flink.ml.cluster.statemachine.InvalidStateTransitionException;
import com.alibaba.flink.ml.cluster.master.AMEvent;
import com.alibaba.flink.ml.cluster.master.AbstractAMStateMachine;
import com.alibaba.flink.ml.cluster.master.AMTransition;
import com.alibaba.flink.ml.cluster.statemachine.transition.SingleArcTransition;
import com.alibaba.flink.ml.proto.*;
import com.alibaba.flink.ml.cluster.role.PsRole;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.util.ProtoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * tensorflow application master special transitions.
 */
public class TFTransitions {
	private static final Logger LOG = LoggerFactory.getLogger(TFTransitions.class);


	/**
	 * tensorflow cluster in batch mode, if worker 0 finish then all cluster finish.
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
			boolean workerZeroFinish = isWorkerZeroFinish(request);
			if (workerZeroFinish && mlContext.isBatchMode()) {
				LOG.info("worker 0 finish and send finish cluster event!");
				AMEvent finishEvent = new AMEvent(AMEventType.FINISH_CLUSTER, "", request.getVersion());
				amStateMachine.sendEvent(finishEvent);
			} else {
				Map<String, Integer> remainJobNumMap = updateRemainJobNum(amEvent);
				int numRemainWorker = remainJobNumMap.getOrDefault(new WorkerRole().name(), 0);
				if (0 == numRemainWorker && mlContext.isStreamMode()) {
					LOG.info("send finish cluster event!");
					AMEvent finishEvent = new AMEvent(AMEventType.FINISH_CLUSTER, "", request.getVersion());
					amStateMachine.sendEvent(finishEvent);
				}
			}
		}

		private static boolean isWorkerZeroFinish(FinishNodeRequest request) {
			boolean workerZeroFinish = false;
			if (request.getNodeSpec().getRoleName().equals(new WorkerRole().name())
					&& 0 == request.getNodeSpec().getIndex()) {
				workerZeroFinish = true;
			}
			return workerZeroFinish;
		}
	}

	/**
	 * tensorflow application master only handle worker,ps node register.
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
				int workerNum = 0;
				int psNum = 0;
				for (MLJobDef jobDef : clusterDef.getJobList()) {
					if (jobDef.getName().equals(new WorkerRole().name())) {
						workerNum = jobDef.getTasksCount();
					} else if (jobDef.getName().equals(new PsRole().name())) {
						psNum = jobDef.getTasksCount();
					}
				}
				Map<String, Integer> remainJobNumMap = updateRemainJobNum(amEvent);
				int remainWorkerNum = remainJobNumMap.get(new WorkerRole().name());
				int remainPsNum = remainJobNumMap.get(new PsRole().name());
				boolean flag = false;
				if (workerNum == remainWorkerNum && psNum == remainPsNum) {
					flag = true;
				}
				if (flag && (!request.getNodeSpec().getRoleName().equals(new TensorBoardRole().name()))) {
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
}
