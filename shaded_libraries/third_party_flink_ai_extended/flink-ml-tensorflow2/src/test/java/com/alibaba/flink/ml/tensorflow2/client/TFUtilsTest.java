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

package com.alibaba.flink.ml.tensorflow2.client;

import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.operator.coding.RowCSVCoding;
import com.alibaba.flink.ml.operator.source.DebugRowSource;
import com.alibaba.flink.ml.operator.table.descriptor.TableDebugRowDescriptor;
import com.alibaba.flink.ml.operator.util.TypeUtil;
import com.alibaba.flink.ml.tensorflow2.hooks.DebugHook;
import com.alibaba.flink.ml.tensorflow2.util.TFConstants;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.SysUtil;
import com.alibaba.flink.ml.util.TestUtil;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TFUtilsTest {
	private static TestingServer server;
	private static final String pythonPath = TestUtil.getProjectRootPath() + "/flink-ml-tensorflow/src/test/python/";
	private static final String add = pythonPath + "add.py";
	private static final String workerZeroFinishScript = pythonPath + "worker_0_finish.py";
	private static final String addTBScript = pythonPath + "add_withtb.py";
	private static final String inputOutputScript = pythonPath + "input_output.py";
	private static final String tensorboardScript = pythonPath + "tensorboard.py";
	private static final String ckptDir = TestUtil.getProjectRootPath() + "/flink-ml-tensorflow/target/tmp/add_withtb/";

	@Before
	public void setUp() throws Exception {
		server = new TestingServer(2181, true);
	}

	@After
	public void tearDown() throws Exception {
		server.stop();
	}

	@Test
	public void addTrainStream() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		TFConfig config = new TFConfig(2, 1, null, add, "map_func", null);
		TFUtils.train(streamEnv, null, config);

		JobExecutionResult result = streamEnv.execute();
		System.out.println(result.getNetRuntime());
	}

	@Test
	public void addTrainTable() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
		StatementSet statementSet = tableEnv.createStatementSet();

		TFConfig config = new TFConfig(2, 1, null, add, "map_func", null);
		TFUtils.train(streamEnv, tableEnv, statementSet, null, config, null);

		execTableJobCustom(config.getMlConfig(), streamEnv, tableEnv, statementSet);
	}

	@Test
	public void addTrainChiefAloneStream() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		TFConfig config = new TFConfig(2, 1, null, add, "map_func", null);
		config.addProperty(TFConstants.TF_IS_CHIEF_ALONE, "true");

		TFUtils.train(streamEnv, null, config);

		JobExecutionResult result = streamEnv.execute();
		System.out.println(result.getNetRuntime());
	}

	@Test
	public void addTrainChiefAloneTable() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
		StatementSet statementSet = tableEnv.createStatementSet();

		TFConfig config = new TFConfig(2, 1, null, add, "map_func", null);
		config.addProperty(TFConstants.TF_IS_CHIEF_ALONE, "true");
		TFUtils.train(streamEnv, tableEnv, statementSet, null, config, null);

		execTableJobCustom(config.getMlConfig(), streamEnv, tableEnv, statementSet);
	}

	@Test
	public void inputOutputTable() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		TFConfig config = new TFConfig(2, 1, null, inputOutputScript, "map_func", null);
		config.getProperties().put(MLConstants.ENCODING_CLASS, RowCSVCoding.class.getCanonicalName());
		config.getProperties().put(MLConstants.DECODING_CLASS, RowCSVCoding.class.getCanonicalName());
		StringBuilder inputSb = new StringBuilder();

		inputSb.append(com.alibaba.flink.ml.operator.util.DataTypes.INT_32.name()).append(",");
		inputSb.append(com.alibaba.flink.ml.operator.util.DataTypes.INT_64.name()).append(",");
		inputSb.append(com.alibaba.flink.ml.operator.util.DataTypes.FLOAT_32.name()).append(",");
		inputSb.append(com.alibaba.flink.ml.operator.util.DataTypes.FLOAT_64.name()).append(",");
		inputSb.append(com.alibaba.flink.ml.operator.util.DataTypes.STRING.name());

		config.getProperties().put(RowCSVCoding.ENCODE_TYPES, inputSb.toString());
		config.getProperties().put(RowCSVCoding.DECODE_TYPES, inputSb.toString());
		TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
		StatementSet statementSet = tableEnv.createStatementSet();

		tableEnv.connect(new TableDebugRowDescriptor())
				.withSchema(new Schema().schema(TypeUtil.rowTypeInfoToSchema(DebugRowSource.typeInfo)))
				.createTemporaryTable("debug_source");
		Table input = tableEnv.scan("debug_source");
		tableEnv.connect(new TableDebugRowDescriptor())
				.withSchema(new Schema().schema(TypeUtil.rowTypeInfoToSchema(DebugRowSource.typeInfo)))
				.createTemporaryTable("table_row_sink");
		Table table = TFUtils.train(streamEnv, tableEnv, statementSet, input, config,
				TypeUtil.rowTypeInfoToSchema(DebugRowSource.typeInfo));
		statementSet.addInsert("table_row_sink", table);
		execTableJobCustom(config.getMlConfig(), streamEnv, tableEnv, statementSet);
	}

	//@Test
	public void testTensorBoard() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		TFConfig config = new TFConfig(2, 1, null, addTBScript, "map_func", null);
		config.getProperties().put(MLConstants.FLINK_HOOK_CLASSNAMES, DebugHook.class.getCanonicalName());
		config.addProperty(MLConstants.CHECKPOINT_DIR, ckptDir + String.valueOf(System.currentTimeMillis()));
		TFUtils.train(flinkEnv, null, config);

		TFConfig tbConfig = config.deepCopy();
		String[] scripts = { tensorboardScript };
		tbConfig.setPythonFiles(scripts);
		TFUtils.startTensorBoard(flinkEnv, tbConfig);

		JobExecutionResult result = flinkEnv.execute();
	}

	//@Test
	public void testTensorBoardTable() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
		StatementSet statementSet = tableEnv.createStatementSet();

		TFConfig config = new TFConfig(2, 1, null, addTBScript, "map_func", null);
		config.getProperties().put(MLConstants.FLINK_HOOK_CLASSNAMES, DebugHook.class.getCanonicalName());
		config.addProperty(MLConstants.CHECKPOINT_DIR, ckptDir + String.valueOf(System.currentTimeMillis()));
		TFUtils.train(streamEnv, tableEnv, statementSet, null, config, null);

		TFConfig tbConfig = config.deepCopy();
		String[] scripts = { tensorboardScript };
		tbConfig.setPythonFiles(scripts);
		TFUtils.startTensorBoard(streamEnv, tableEnv, statementSet, tbConfig);

		statementSet.execute().getJobClient().get()
				.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
	}

	@Test
	public void testWorkerZeroFinish() throws Exception {
		System.out.println(SysUtil._FUNC_());
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
		StatementSet statementSet = tableEnv.createStatementSet();
		TFConfig config = new TFConfig(3, 2, null, workerZeroFinishScript, "map_func", null);
		TFUtils.train(streamEnv, tableEnv, statementSet, null, config, null);
		execTableJobCustom(config.getMlConfig(), streamEnv, tableEnv, statementSet);
	}

	public static void execTableJobCustom(MLConfig mlConfig, StreamExecutionEnvironment streamEnv,
										  TableEnvironment tableEnv, StatementSet statementSet) throws Exception {
		statementSet.execute().getJobClient().get()
				.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
	}
}