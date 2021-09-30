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

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.cluster.node.runner.MLRunner;
import com.alibaba.flink.ml.cluster.node.runner.CommonMLRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MLRunnerTest {

	@Test
	public void stopMLRunnerWhenAMUnavailable() throws Exception {
		MLContext mlContext = mock(MLContext.class);
		when(mlContext.getProperties()).thenReturn(Collections.emptyMap());
		MLRunner tfRunner = new CommonMLRunner(mlContext, mock(NodeServer.class));
		final ExecutorService runnerService = Executors.newFixedThreadPool(1, r -> {
			Thread runnerThread = new Thread(r);
			runnerThread.setDaemon(true);
			runnerThread.setName("runner_" + mlContext.getIdentity());
			// r.setUncaughtExceptionHandler(new TFRunnerExceptionHandler());
			return runnerThread;
		});

		Future runnerFuture = runnerService.submit(tfRunner);
		Thread.sleep(2000);
		tfRunner.notifyStop();
		runnerFuture.cancel(true);
		Assert.assertTrue("MLRunner not stopped as expected", runnerFuture.isDone());
		runnerService.shutdownNow();
		runnerService.awaitTermination(5, TimeUnit.SECONDS);
	}
}
