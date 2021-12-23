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

package com.alibaba.flink.ml.tensorflow2.coding;

import com.alibaba.flink.ml.coding.CodingException;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.google.protobuf.ByteString;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * a helper function to do transformation between java object and tensorflow feature object.
 */
public class TFExampleConversion {

	/**
	 * convert java object to tensorflow featue.
	 * @param dt java object data type.
	 * @param val given java object.
	 * @return tensorflow feature.
	 */
	public static Feature javaToFeature(DataTypes dt, Object val) {
		Feature.Builder featureBuilder = Feature.newBuilder();
		switch (dt) {
			case FLOAT_32:
				FloatList.Builder fb = FloatList.newBuilder();
				fb.addValue((Float) val);
				featureBuilder.setFloatList(fb);
				break;
			case INT_8:
			case INT_16:
			case INT_32:
			case INT_64:
			case UINT_8:
			case UINT_16:
			case UINT_32:
			case UINT_64:
				Int64List.Builder ib = Int64List.newBuilder();
				ib.addValue(castAsLong(val));
				featureBuilder.setInt64List(ib);
				break;
			case STRING:
				BytesList.Builder bb = BytesList.newBuilder();
				bb.addValue(castAsBytes(val));
				featureBuilder.setBytesList(bb);
				break;
			case FLOAT_32_ARRAY:
				FloatList.Builder floatArrBuilder = FloatList.newBuilder();
				Float[] floats;
				if (val instanceof Float[]) {
					floats = (Float[]) val;
				} else { // val is instance of float[]
					float[] tmp = (float[]) val;
					floats = new Float[tmp.length];
					for (int i = 0; i < tmp.length; ++i) {
						floats[i] = tmp[i];
					}
				}
				for (float f : floats) {
					floatArrBuilder.addValue(f);
				}
				featureBuilder.setFloatList(floatArrBuilder);
				break;
			default:
				throw new RuntimeException("Unsupported data type for TF");
		}
		return featureBuilder.build();
	}

	/**
	 * convert tensorflow feature to java object.
	 * @param types feature data type.
	 * @param f tensorflow feature object.
	 * @return feature corresponds to java object.
	 * @throws CodingException
	 */
	public static Object featureToJava(DataTypes types, Feature f) throws CodingException {
		switch (types) {
			case FLOAT_32:
				return f.getFloatList().getValue(0);
			case INT_8:
			case INT_16:
			case INT_32:
			case UINT_8:
			case UINT_16:
			case UINT_32:
				long v = f.getInt64List().getValue(0);
				return (int) v;
			case INT_64:
			case UINT_64:
				return f.getInt64List().getValue(0);
			case FLOAT_64:
				return f.getFloatList().getValue(0);
			case STRING:
				ByteString bs = f.getBytesList().getValue(0);
				return bs.toString(StandardCharsets.ISO_8859_1);
			case FLOAT_32_ARRAY:
				List<Float> list = f.getFloatList().getValueList();
				float[] floats = new float[list.size()];
				for (int i = 0; i < floats.length; i++) {
					floats[i] = list.get(i);
				}
				return floats;
		}
		throw new CodingException("Unsupported data type: " + types.toString());
	}

	private static ByteString castAsBytes(Object val) {
		if (val instanceof byte[]) {
			byte[] bytes = (byte[]) val;
			return ByteString.copyFrom(bytes);
		}
		if (val instanceof String) {
			String s = (String) val;
			return ByteString.copyFrom(s, StandardCharsets.ISO_8859_1);
		}
		throw new RuntimeException("Can't cast object " + val + " to bytes.");
	}

	private static long castAsLong(Object val) {
		if (val instanceof Short) {
			return (Short) val;
		}
		if (val instanceof Integer) {
			return (Integer) val;
		}
		if (val instanceof Long) {
			return (Long) val;
		}
		if (val instanceof Boolean) {
			boolean b = (Boolean) val;
			return b ? 1 : 0;
		}
		throw new RuntimeException("Can't cast object " + val + " to long");
	}
}
