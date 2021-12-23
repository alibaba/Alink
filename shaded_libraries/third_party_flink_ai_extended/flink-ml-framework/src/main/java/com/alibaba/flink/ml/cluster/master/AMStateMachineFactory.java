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

import com.alibaba.flink.ml.cluster.BaseEventReporter;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.master.meta.AMMeta;
import com.alibaba.flink.ml.cluster.rpc.AppMasterServer;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * a factory to create application master by MLConstants.AM_STATE_MACHINE_CLASS config.
 */
public class AMStateMachineFactory {
	private static final Logger LOG = LoggerFactory.getLogger(AMStateMachineFactory.class);


	/**
	 * create application master state machine.
	 * @param amService application master service.
	 * @param amMeta application master meta data.
	 * @param mlContext application master node runtime context.
	 * @param eventReporter application master status change reporter.
	 * @return application master state machine.
	 * @throws MLException
	 */
	public static AbstractAMStateMachine getAMStateMachine(AMService amService,
			AMMeta amMeta, MLContext mlContext,
			BaseEventReporter eventReporter) throws MLException {
		String impl = mlContext.getProperties().getOrDefault(MLConstants.AM_STATE_MACHINE_CLASS,
				AMStateMachineImpl.class.getCanonicalName());
		LOG.info("state machine class:" + impl);
		try {
			Class cls = Class.forName(impl);
			Preconditions.checkArgument(AbstractAMStateMachine.class.isAssignableFrom(cls),
					"Invalid implementation class " + impl);
			Constructor<AbstractAMStateMachine> constructor = cls
					.getConstructor(AppMasterServer.AppMasterServiceImpl.class,
							AMMeta.class,
							MLContext.class,
							BaseEventReporter.class);
			return constructor.newInstance(amService, amMeta, mlContext, eventReporter);
		} catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException |
				ClassNotFoundException e) {
			throw new MLException("Failed to create AMStateMachine", e);
		}
	}
}
