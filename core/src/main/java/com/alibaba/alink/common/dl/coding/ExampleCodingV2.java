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

package com.alibaba.alink.common.dl.coding;

import com.alibaba.alink.common.dl.data.DataTypesV2;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.Coding;
import com.alibaba.flink.ml.coding.CodingException;
import com.alibaba.flink.ml.tf2.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.tensorflow.proto.example.Example;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.Features;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.ml.tensorflow2.util.TFConstants.INPUT_TF_EXAMPLE_CONFIG;
import static com.alibaba.flink.ml.tensorflow2.util.TFConstants.OUTPUT_TF_EXAMPLE_CONFIG;

/**
 * implement tensorflow Example object transformation with byte array. ExampleCoding inited with ExampleCodingConfig.
 * <p>
 * Modified to adopt {@link ExampleCodingConfigV2} and {@link DataTypesV2} to support more array types.
 */
public class ExampleCodingV2 implements Coding <Object>, Serializable {
	private MLContext mlContext;
	private ExampleCodingConfigV2 inputConfig;
	private ExampleCodingConfigV2 outputConfig;

	public ExampleCodingV2(MLContext mlContext) throws CodingException {
		this.mlContext = mlContext;
		this.inputConfig = new ExampleCodingConfigV2();
		JSONObject jsonObject = JSONObject.parseObject(mlContext.getProperties().get(INPUT_TF_EXAMPLE_CONFIG));
		if (jsonObject != null) {
			this.inputConfig.fromJsonObject(jsonObject);
		}
		this.outputConfig = new ExampleCodingConfigV2();
		JSONObject jsonObjectOutput = JSONObject.parseObject(mlContext.getProperties().get(OUTPUT_TF_EXAMPLE_CONFIG));
		if (jsonObjectOutput != null) {
			this.outputConfig.fromJsonObject(jsonObjectOutput);
		}
	}

	/**
	 * convert byte array to Example.
	 *
	 * @param bytes java object corresponds to byte array.
	 * @return tensorflow Example record.
	 * @throws CodingException
	 */
	@Override
	public Object decode(byte[] bytes) throws CodingException {
		Example example = null;
		try {
			example = Example.parseFrom(bytes);
		} catch (InvalidProtocolBufferException e1) {
			e1.printStackTrace();
			throw new CodingException(e1.getMessage());
		}
		Map <String, Feature> nameToFeature = example.getFeatures().getFeatureMap();
		List <Object> fields = new ArrayList <>(outputConfig.count());
		for (int i = 0; i < outputConfig.count(); i++) {
			String colName = outputConfig.getColName(i);
			DataTypesV2 type = outputConfig.getType(i);
			if (colName != null) {
				Feature f = nameToFeature.get(colName);
				if (f != null) {
					Object o = TFExampleConversionV2.featureToJava(type, f);
					fields.add(o);
				}
			}
		}
		return outputConfig.createResultObject(fields);
	}

	/**
	 * convert Example to byte array.
	 *
	 * @param object tensorflow Example record.
	 * @return java object corresponds to byte array.
	 * @throws CodingException
	 */
	@Override
	public byte[] encode(Object object) throws CodingException {
		Example.Builder exampleBuilder = Example.newBuilder();
		Features.Builder featuresBuilder = exampleBuilder.getFeaturesBuilder();
		for (int i = 0; i < inputConfig.count(); i++) {
			String colName = inputConfig.getColName(i);
			DataTypesV2 dt = inputConfig.getType(i);
			Object val = inputConfig.getField(object, i);
			Feature f = TFExampleConversionV2.javaToFeature(dt, val);
			featuresBuilder.putFeature(colName, f);
		}
		exampleBuilder.setFeatures(featuresBuilder);
		return exampleBuilder.build().toByteArray();
	}
}
