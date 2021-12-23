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

package com.alibaba.flink.ml.cluster.node.runner.python;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.flink.ml.TestWithNodeService;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.impl.ByteArrayCodingImpl;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.util.DummyContext;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.TestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class ProcessPythonRunnerTest extends TestWithNodeService {
	private static final Logger Logger = LoggerFactory.getLogger(ProcessPythonRunnerTest.class);


	public MLContext createMLContext(String scriptName) throws Exception {
		String rootPath = TestUtil.getProjectRootPath() + "/flink-ml-framework/src/test/python";

		MLContext context = DummyContext.createDummyMLContext();
		context.setPythonDir(Paths.get(rootPath));
		context.setPythonFiles(new String[] { scriptName });
		context.setFuncName("map_func");
		configureContext(context);
		return context;
	}

	@Test
	public void greeterPythonTest() throws Exception {
		String script = "greeter.py";
		MLContext mlContext = createMLContext(script);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
	}

	@Test
	public void pythonReadFromJavaTest() throws Exception {
		String script = "read_from_java.py";
		MLContext mlContext = createMLContext(script);
		DataExchange<JSONObject, JSONObject> dataExchange = new DataExchange<>(mlContext);
		JSONObject object = new JSONObject();
		object.put("a", "a");
		dataExchange.write(object);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
	}

	@Test
	public void pythonWriteToJavaTest() throws Exception {
		String script = "write_to_java.py";
		MLContext mlContext = createMLContext(script);
		DataExchange<JSONObject, JSONObject> dataExchange = new DataExchange<>(mlContext);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
		JSONObject object = dataExchange.read(true);
		System.out.println("res:" + object.toJSONString());

	}

	@Test
	public void pythonReadBytesFromJavaTest() throws Exception {
		String script = "read_bytes_from_java.py";
		MLContext mlContext = createMLContext(script);
		mlContext.getProperties().put(MLConstants.DECODING_CLASS, ByteArrayCodingImpl.class.getCanonicalName());
		mlContext.getProperties().put(MLConstants.ENCODING_CLASS, ByteArrayCodingImpl.class.getCanonicalName());
		DataExchange<byte[], byte[]> dataExchange = new DataExchange<>(mlContext);
		byte[] object = "aaaaa".getBytes();
		dataExchange.write(object);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
	}

	@Test
	public void pythonWriteBytesToJavaTest() throws Exception {
		String script = "write_bytes_to_java.py";
		MLContext mlContext = createMLContext(script);
		mlContext.getProperties().put(MLConstants.DECODING_CLASS, ByteArrayCodingImpl.class.getCanonicalName());
		mlContext.getProperties().put(MLConstants.ENCODING_CLASS, ByteArrayCodingImpl.class.getCanonicalName());
		DataExchange<byte[], byte[]> dataExchange = new DataExchange<>(mlContext);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
		byte[] object = dataExchange.read(true);
		Logger.info("res:" + new String(object));

	}

	@Test
	public void pythonReadJsonFromJavaTest() throws Exception {
		String script = "read_json_from_java.py";
		MLContext mlContext = createMLContext(script);
		DataExchange<JSONObject, JSONObject> dataExchange = new DataExchange<>(mlContext);
		JSONObject object = new JSONObject();
		object.put("json_read", "json_read");
		dataExchange.write(object);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
	}

	@Test
	public void pythonWriteJsonToJavaTest() throws Exception {
		String script = "write_json_to_java.py";
		MLContext mlContext = createMLContext(script);
		DataExchange<JSONObject, JSONObject> dataExchange = new DataExchange<>(mlContext);
		ProcessPythonRunner runner = new ProcessPythonRunner(mlContext);
		runner.runScript();
		JSONObject object = dataExchange.read(true);
		Logger.info("res:" + object.toJSONString());
	}
}
