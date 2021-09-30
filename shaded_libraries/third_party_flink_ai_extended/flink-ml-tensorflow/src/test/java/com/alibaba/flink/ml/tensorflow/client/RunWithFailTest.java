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

package com.alibaba.flink.ml.tensorflow.client;

import com.alibaba.flink.ml.tensorflow.storage.DummyStorage;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.SysUtil;
import com.alibaba.flink.ml.util.TestUtil;
import org.apache.curator.test.TestingServer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsInstanceOf;
import org.junit.*;
import org.junit.matchers.JUnitMatchers;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class RunWithFailTest {

	private static Logger LOG = LoggerFactory.getLogger(RunWithFailTest.class);

	private static TestingServer server;
	private static final String simple_print = "simple_print.py";
	private static final String failover = "failover.py";
	private static final String failover2 = "failover2.py";


	@Before
	public void setUp() throws Exception {
		server = new TestingServer(2181, true);
	}

	@After
	public void tearDown() throws Exception {
		server.stop();
	}

	private TFConfig buildTFConfig(String pyFile) {
		return buildTFConfig(pyFile, String.valueOf(System.currentTimeMillis()), 2, 1);
	}

	private TFConfig buildTFConfig(String pyFile, String version, int worker, int ps) {
		System.out.println("buildTFConfig: " + SysUtil._FUNC_());
		System.out.println("Current version:" + version);
		TFConfig config = new TFConfig(worker, ps, null, new String[] { scriptAbsolutePath(pyFile) },
				"map_func", null);
		return config;
	}

	@Test
	public void SimpleStartupTest() throws Exception {
		TFConfig config = buildTFConfig(simple_print, "1", 1, 1);
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TFUtils.train(streamEnv, config);
		JobExecutionResult result = streamEnv.execute();
		System.out.println(result.getNetRuntime());
	}

	@Test
	public void WorkerFailoverTest() throws Exception {
		LOG.info("############ Start failover test.");
		TFConfig config = buildTFConfig(failover, String.valueOf(System.currentTimeMillis()), 2, 1);
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TFUtils.train(streamEnv, null, config);
		JobExecutionResult result = streamEnv.execute();
		System.out.println(result.getNetRuntime());
		LOG.info("############# Finish failover test.");
	}

	@Test
	public void testFailoverWithFinishedNode() throws Exception {
		TFConfig config = buildTFConfig(failover2, String.valueOf(System.currentTimeMillis()), 2, 1);
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TFUtils.train(streamEnv, null, config);
		JobExecutionResult result = streamEnv.execute();
		System.out.println(result.getNetRuntime());
	}

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testJobTimeout() throws Exception {
		TFConfig tfConfig = buildTFConfig(simple_print);
		tfConfig.setPsNum(0);
		tfConfig.setWorkerNum(1);
		// using the dummy storage will make TFNodeServer timeout
		tfConfig.getProperties().put(MLConstants.CONFIG_STORAGE_TYPE, MLConstants.STORAGE_CUSTOM);
		tfConfig.getProperties().put(MLConstants.STORAGE_IMPL_CLASS, DummyStorage.class.getCanonicalName());
		tfConfig.getProperties().put(MLConstants.AM_REGISTRY_TIMEOUT, String.valueOf(Duration.ofSeconds(10).toMillis()));
		tfConfig.getProperties().put(MLConstants.NODE_IDLE_TIMEOUT, String.valueOf(Duration.ofSeconds(10).toMillis()));
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TFUtils.train(streamEnv, null, tfConfig);

		expectedException.expect(ExecutionException.class);
		expectedException.expectCause(IsInstanceOf.instanceOf(JobExecutionException.class));
		expectedException.expectMessage("Job execution failed");
		streamEnv.execute();
	}

	private static String scriptAbsolutePath(String script) {
		return TestUtil.getProjectRootPath() + "/flink-ml-tensorflow/src/test/python/" + script;
	}
}
