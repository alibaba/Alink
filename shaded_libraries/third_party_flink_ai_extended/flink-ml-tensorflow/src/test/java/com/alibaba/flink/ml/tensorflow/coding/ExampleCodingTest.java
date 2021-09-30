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

package com.alibaba.flink.ml.tensorflow.coding;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.tensorflow.client.TFConfig;
import com.alibaba.flink.ml.tensorflow.util.ExampleCodingConfigUtil;
import com.alibaba.flink.ml.tensorflow.util.TFConstants;
import com.alibaba.flink.ml.util.MLException;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExampleCodingTest {

	@Test
	public void decode() {
	}

	@Test
	public void encode() {
	}

	private MLContext emptyMLContext() throws MLException {
		Map<String, Integer> jobNum = new HashMap<>();
		jobNum.put(new WorkerRole().name(), 1);
		MLConfig mlConfig = new MLConfig(jobNum, new HashMap<String, String>(), "", "", null);
		return new MLContext(ExecutionMode.OTHER, mlConfig, null, 0, null, null);
	}

	@Test
	public void table() {
		TFConfig config = new TFConfig(1, 0, null, new String[]{}, null, null);
		TableSchema inputSchema = new TableSchema(new String[]{"fieldName"}, new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO});
		TableSchema outputSchema = new TableSchema(new String[]{"fieldName"}, new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO});
		ExampleCodingConfigUtil
				.configureExampleCoding(config, inputSchema, outputSchema, ExampleCodingConfig.ObjectType.ROW, Row.class);
		Assert.assertEquals(config.getProperty(TFConstants.INPUT_TF_EXAMPLE_CONFIG), config.getProperty(TFConstants.OUTPUT_TF_EXAMPLE_CONFIG));
	}

	@Test
	public void pojo() throws Exception {
		MLContext mlContext = emptyMLContext();
		JSONObject config = getJsonConfig();
		mlContext.getProperties().put(TFConstants.INPUT_TF_EXAMPLE_CONFIG, config.toJSONString());
		mlContext.getProperties().put(TFConstants.OUTPUT_TF_EXAMPLE_CONFIG, config.toJSONString());

		ExampleCoding coding = new ExampleCoding(mlContext);
		PojoAB testObject = new PojoAB();
		testObject.setA(3);
		float[] b = new float[2];
		b[0] = 1.0f;
		b[1] = 2.0f;
		testObject.setB(b);
		byte[] bytes = coding.encode(testObject);

		Object res = coding.decode(bytes);
		System.out.println("res:" + res);
	}

	@Test
	public void row() throws Exception {
		MLContext mlContext = emptyMLContext();
		JSONObject config = getJsonConfig();
		config.put("objectType", ExampleCodingConfig.ObjectType.ROW.name());
		mlContext.getProperties().put(TFConstants.INPUT_TF_EXAMPLE_CONFIG, config.toJSONString());
		mlContext.getProperties().put(TFConstants.OUTPUT_TF_EXAMPLE_CONFIG, config.toJSONString());

		ExampleCoding coding = new ExampleCoding(mlContext);
		Row testObject = new Row(2);
		testObject.setField(0, 3);
		float[] b = new float[2];
		b[0] = 1.0f;
		b[1] = 2.0f;
		testObject.setField(1, b);
		byte[] bytes = coding.encode(testObject);

		Object res = coding.decode(bytes);
		System.out.println("res:" + res);
	}

	@Test
	public void tuple() throws Exception {
		MLContext mlContext = emptyMLContext();
		JSONObject config = getJsonConfig();
		config.put("objectType", ExampleCodingConfig.ObjectType.TUPLE.name());
		mlContext.getProperties().put(TFConstants.INPUT_TF_EXAMPLE_CONFIG, config.toJSONString());
		mlContext.getProperties().put(TFConstants.OUTPUT_TF_EXAMPLE_CONFIG, config.toJSONString());

		ExampleCoding coding = new ExampleCoding(mlContext);
		float[] b = new float[2];
		b[0] = 1.0f;
		b[1] = 2.0f;
		Tuple testObject = new Tuple2<>(5, b);

		byte[] bytes = coding.encode(testObject);

		Object res = coding.decode(bytes);
		System.out.println("res:" + res);
	}

	protected JSONObject getJsonConfig() {
		JSONObject config = new JSONObject();
		JSONArray n = new JSONArray();
		n.add("a");
		n.add("b");
		config.put("names", n);
		JSONArray t = new JSONArray();
		t.add(DataTypes.INT_32.name());
		t.add(DataTypes.FLOAT_32_ARRAY.name());
		config.put("types", t);
		config.put("objectType", ExampleCodingConfig.ObjectType.POJO.name());
		config.put("objectClass", PojoAB.class.getCanonicalName());
		return config;
	}

}