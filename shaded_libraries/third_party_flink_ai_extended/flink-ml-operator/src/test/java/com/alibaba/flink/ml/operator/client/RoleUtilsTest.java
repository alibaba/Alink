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

package com.alibaba.flink.ml.operator.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.operator.coding.RowCSVCoding;
import com.alibaba.flink.ml.operator.sink.DebugJsonSink;
import com.alibaba.flink.ml.operator.source.DebugJsonSource;
import com.alibaba.flink.ml.operator.source.DebugRowSource;
import com.alibaba.flink.ml.operator.table.descriptor.TableDebugRowDescriptor;
import com.alibaba.flink.ml.operator.util.PythonFileUtil;
import com.alibaba.flink.ml.operator.util.TypeUtil;
import com.alibaba.flink.ml.util.DummyContext;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.SysUtil;
import com.alibaba.flink.ml.util.TestUtil;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.ml.operator.client.TableTestUtil.execTableJobCustom;

public class RoleUtilsTest {
    private static TestingServer testingServer;
    private static String rootPath = TestUtil.getProjectRootPath() + "/flink-ml-operator/src/test/python/";
    private Logger LOG = LoggerFactory.getLogger(RoleUtilsTest.class);


    @Before
    public void setUp() throws Exception {
        testingServer = new TestingServer(2181, true);
    }

    @After
    public void tearDown() throws Exception {
        testingServer.stop();
    }

    @Test
    public void greeterJob() throws Exception{
        LOG.info("RUN TEST:" + SysUtil._FUNC_());
        MLConfig mlConfig = DummyContext.createDummyMLConfig();
        mlConfig.setRoleNum(new WorkerRole().name(), 2);
        String[] files = {rootPath + "greeter.py"};
        mlConfig.setPythonFiles(files);
        mlConfig.setFuncName("map_func");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        PythonFileUtil.registerPythonFiles(streamEnv, mlConfig);
        RoleUtils.addAMRole(streamEnv,mlConfig);
        RoleUtils.addRole(streamEnv, ExecutionMode.TRAIN, null, mlConfig, null, new WorkerRole());
        streamEnv.execute();
    }

    @Test
    public void outputJob() throws Exception{
        LOG.info("RUN TEST:" + SysUtil._FUNC_());
        MLConfig mlConfig = DummyContext.createDummyMLConfig();
        mlConfig.setRoleNum(new WorkerRole().name(), 3);
        String[] files = {rootPath + "output_json.py"};
        mlConfig.setPythonFiles(files);
        mlConfig.setFuncName("map_func");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        PythonFileUtil.registerPythonFiles(streamEnv, mlConfig);
        RoleUtils.addAMRole(streamEnv,mlConfig);
        RoleUtils.addRole(streamEnv, ExecutionMode.TRAIN, null, mlConfig, TypeInformation.of(JSONObject.class), new WorkerRole())
                .addSink(new DebugJsonSink()).setParallelism(3);
        streamEnv.execute();
    }

    @Test
    public void inputOutputJob() throws Exception{
        LOG.info("RUN TEST:" + SysUtil._FUNC_());
        MLConfig mlConfig = DummyContext.createDummyMLConfig();
        mlConfig.setRoleNum(new WorkerRole().name(), 3);
        String[] files = {rootPath + "input_output_json.py"};
        mlConfig.setPythonFiles(files);
        mlConfig.setFuncName("map_func");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        PythonFileUtil.registerPythonFiles(streamEnv, mlConfig);
        DataStreamSource<JSONObject> input = streamEnv.addSource(new DebugJsonSource()).setParallelism(3);
        RoleUtils.addAMRole(streamEnv,mlConfig);
        RoleUtils.addRole(streamEnv, ExecutionMode.TRAIN, input, mlConfig, TypeInformation.of(JSONObject.class), new WorkerRole())
        .addSink(new DebugJsonSink()).setParallelism(3);
        streamEnv.execute();
    }

    @Test
    public void greeterJobTable() throws Exception{
        LOG.info("RUN TEST:" + SysUtil._FUNC_());
        MLConfig mlConfig = DummyContext.createDummyMLConfig();
        mlConfig.setRoleNum(new WorkerRole().name(), 2);
        String[] files = {rootPath + "greeter.py"};
        mlConfig.setPythonFiles(files);
        mlConfig.setFuncName("map_func");
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        StatementSet statementSet = tableEnv.createStatementSet();
        PythonFileUtil.registerPythonFiles(streamEnv, mlConfig);
        RoleUtils.addAMRole(tableEnv, statementSet, mlConfig);
        RoleUtils.addRole(tableEnv, statementSet, ExecutionMode.TRAIN, null, mlConfig, null, new WorkerRole());
        execTableJobCustom(mlConfig, streamEnv, tableEnv, statementSet);
    }

    @Test
    public void outputJobTable() throws Exception{
        LOG.info("RUN TEST:" + SysUtil._FUNC_());
        MLConfig mlConfig = DummyContext.createDummyMLConfig();
        mlConfig.setRoleNum(new WorkerRole().name(), 3);
        String[] files = {rootPath + "output_row.py"};
        mlConfig.setPythonFiles(files);
        mlConfig.setFuncName("map_func");
        mlConfig.getProperties().put(MLConstants.ENCODING_CLASS, RowCSVCoding.class.getCanonicalName());
        mlConfig.getProperties().put(MLConstants.DECODING_CLASS, RowCSVCoding.class.getCanonicalName());
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < 4; i++){
            sb.append(com.alibaba.flink.ml.operator.util.DataTypes.STRING.name()).append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        mlConfig.getProperties().put(RowCSVCoding.DECODE_TYPES, sb.toString());

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        StatementSet statementSet = tableEnv.createStatementSet();
        PythonFileUtil.registerPythonFiles(streamEnv, mlConfig);
        RoleUtils.addAMRole(tableEnv, statementSet, mlConfig);
        String[] names = {"a", "b", "c", "d"};
        TypeInformation[] types = {Types.STRING, Types.STRING,Types.STRING, Types.STRING,};
        TableSchema outputSchema = new TableSchema(names, types);
        tableEnv.connect(new TableDebugRowDescriptor())
                .withSchema(new Schema().schema(outputSchema))
                .createTemporaryTable("row_sink");
        Table table = RoleUtils.addRole(tableEnv, statementSet, ExecutionMode.TRAIN, null, mlConfig, outputSchema, new WorkerRole());
        statementSet.addInsert("row_sink", table);

        execTableJobCustom(mlConfig, streamEnv, tableEnv, statementSet);
    }

    @Test
    public void inputOutputJobTable() throws Exception{
        LOG.info("RUN TEST:" + SysUtil._FUNC_());
        MLConfig mlConfig = DummyContext.createDummyMLConfig();
        mlConfig.setRoleNum(new WorkerRole().name(), 3);
        String[] files = {rootPath + "input_output_row.py"};
        mlConfig.setPythonFiles(files);
        mlConfig.setFuncName("map_func");
        mlConfig.getProperties().put(MLConstants.ENCODING_CLASS, RowCSVCoding.class.getCanonicalName());
        mlConfig.getProperties().put(MLConstants.DECODING_CLASS, RowCSVCoding.class.getCanonicalName());
        StringBuilder sb = new StringBuilder();

        sb.append(com.alibaba.flink.ml.operator.util.DataTypes.INT_32.name()).append(",");
        sb.append(com.alibaba.flink.ml.operator.util.DataTypes.INT_64.name()).append(",");
        sb.append(com.alibaba.flink.ml.operator.util.DataTypes.FLOAT_32.name()).append(",");
        sb.append(com.alibaba.flink.ml.operator.util.DataTypes.FLOAT_64.name()).append(",");
        sb.append(com.alibaba.flink.ml.operator.util.DataTypes.STRING.name());

        mlConfig.getProperties().put(RowCSVCoding.ENCODE_TYPES, sb.toString());
        mlConfig.getProperties().put(RowCSVCoding.DECODE_TYPES, sb.toString());

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        StatementSet statementSet = tableEnv.createStatementSet();
        PythonFileUtil.registerPythonFiles(streamEnv, mlConfig);

        tableEnv.connect(new TableDebugRowDescriptor())
                .withSchema(new Schema().schema(TypeUtil.rowTypeInfoToSchema(DebugRowSource.typeInfo)))
                .createTemporaryTable("debug_source");
        Table input = tableEnv.from("debug_source");
        RoleUtils.addAMRole(tableEnv, statementSet, mlConfig);
        tableEnv.connect(new TableDebugRowDescriptor())
                .withSchema(new Schema().schema(TypeUtil.rowTypeInfoToSchema(DebugRowSource.typeInfo)))
                .createTemporaryTable("debug_row_sink");
        Table table = RoleUtils.addRole(tableEnv, statementSet, ExecutionMode.TRAIN, input, mlConfig,
                TypeUtil.rowTypeInfoToSchema(DebugRowSource.typeInfo), new WorkerRole());
        statementSet.addInsert("debug_row_sink", table);

        execTableJobCustom(mlConfig, streamEnv, tableEnv, statementSet);
    }

}