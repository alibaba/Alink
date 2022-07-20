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
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.UByteTensor;
import com.alibaba.alink.operator.batch.tensorflow.TensorFlow2BatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TensorFlowBatchOp;
import com.alibaba.flink.ml.coding.CodingException;
import com.alibaba.flink.ml.tf2.shaded.com.google.protobuf.ByteString;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * a helper function to do transformation between java object and tensorflow feature object.
 * <p>
 * Modified to support more array types and tensor types.
 * <p>
 * Note:
 * <p>
 * In Alink, except for {@link TensorFlowBatchOp} and {@link
 * TensorFlow2BatchOp},
 * all training operators only return SavedModels as Strings, and all inference operators are done using Java API.
 * Therefore, {@link TFExampleConversionV2#featureToJava} is actually only called for Strings.
 */
public class TFExampleConversionV2 implements Serializable {

	/**
	 * convert java object to tensorflow feature.
	 *
	 * @param dt  java object data type.
	 * @param val given java object.
	 * @return tensorflow feature.
	 */
	public static Feature javaToFeature(DataTypesV2 dt, Object val) {
		Feature.Builder featureBuilder = Feature.newBuilder();
		FloatList.Builder floatListBuilder = FloatList.newBuilder();
		Int64List.Builder int64ListBuilder = Int64List.newBuilder();

		// When dt is TENSOR, find the exact type first.
		if (DataTypesV2.TENSOR.equals(dt)) {
			if (val instanceof FloatTensor) {
				dt = DataTypesV2.FLOAT_TENSOR;
			} else if (val instanceof DoubleTensor) {
				dt = DataTypesV2.DOUBLE_TENSOR;
			} else if (val instanceof IntTensor) {
				dt = DataTypesV2.INT_TENSOR;
			} else if (val instanceof LongTensor) {
				dt = DataTypesV2.LONG_TENSOR;
			} else if (val instanceof BoolTensor) {
				dt = DataTypesV2.BOOLEAN_TENSOR;
			} else if (val instanceof UByteTensor) {
				dt = DataTypesV2.UBYTE_TENSOR;
			} else if (val instanceof StringTensor) {
				dt = DataTypesV2.STRING_TENSOR;
			} else if (val instanceof ByteTensor) {
				dt = DataTypesV2.BYTE_TENSOR;
			}
		}

		switch (dt) {
			case FLOAT_16:
			case FLOAT_32:
			case FLOAT_64: {
				floatListBuilder.addValue((Float) val);
				featureBuilder.setFloatList(floatListBuilder);
				break;
			}
			case INT_8:
			case INT_16:
			case INT_32:
			case INT_64:
			case UINT_8:
			case UINT_16:
			case UINT_32:
			case UINT_64: {
				int64ListBuilder.addValue(castAsLong(val));
				featureBuilder.setInt64List(int64ListBuilder);
				break;
			}
			case STRING: {
				BytesList.Builder bb = BytesList.newBuilder();
				bb.addValue(castAsBytes(val));
				featureBuilder.setBytesList(bb);
				break;
			}
			case FLOAT_TENSOR: {
				FloatTensor floatTensor = (FloatTensor) val;
				long size = floatTensor.size();
				floatTensor = floatTensor.reshape(new Shape(size));
				for (long i = 0; i < size; i += 1) {
					floatListBuilder.addValue(floatTensor.getFloat(i));
				}
				featureBuilder.setFloatList(floatListBuilder);
				break;
			}
			case DOUBLE_TENSOR: {
				DoubleTensor doubleTensor = (DoubleTensor) val;
				long size = doubleTensor.size();
				doubleTensor = doubleTensor.reshape(new Shape(size));
				for (long i = 0; i < size; i += 1) {
					floatListBuilder.addValue((float) doubleTensor.getDouble(i));
				}
				featureBuilder.setFloatList(floatListBuilder);
				break;
			}
			case INT_TENSOR: {
				IntTensor intTensor = (IntTensor) val;
				long size = intTensor.size();
				intTensor = intTensor.reshape(new Shape(size));
				for (long i = 0; i < size; i += 1) {
					int64ListBuilder.addValue(intTensor.getInt(i));
				}
				featureBuilder.setInt64List(int64ListBuilder);
				break;
			}
			case LONG_TENSOR: {
				LongTensor longTensor = (LongTensor) val;
				long size = longTensor.size();
				longTensor = longTensor.reshape(new Shape(size));
				for (long i = 0; i < size; i += 1) {
					int64ListBuilder.addValue(longTensor.getLong(i));
				}
				featureBuilder.setInt64List(int64ListBuilder);
				break;
			}
			case BOOLEAN_TENSOR: {
				BoolTensor boolTensor = (BoolTensor) val;
				long size = boolTensor.size();
				boolTensor = boolTensor.reshape(new Shape(size));
				for (long i = 0; i < size; i += 1) {
					int64ListBuilder.addValue(boolTensor.getBoolean(i) ? 1 : 0);
				}
				featureBuilder.setInt64List(int64ListBuilder);
				break;
			}
			case UBYTE_TENSOR: {
				UByteTensor ubyteTensor = (UByteTensor) val;
				long size = ubyteTensor.size();
				ubyteTensor = ubyteTensor.reshape(new Shape(size));
				for (long i = 0; i < size; i += 1) {
					int64ListBuilder.addValue(ubyteTensor.getUByte(i));
				}
				featureBuilder.setInt64List(int64ListBuilder);
				break;
			}
			case STRING_TENSOR: {
				StringTensor stringTensor = (StringTensor) val;
				long size = stringTensor.size();
				stringTensor = stringTensor.reshape(new Shape(size));
				BytesList.Builder bb = BytesList.newBuilder();
				for (long i = 0; i < size; i += 1) {
					bb.addValue(castAsBytes(stringTensor.getString(i)));
				}
				featureBuilder.setBytesList(bb);
				break;
			}
			case BYTE_TENSOR:
			default:
				throw new AkUnsupportedOperationException("Unsupported data type for TF: " + dt);
		}
		return featureBuilder.build();
	}

	/**
	 * convert tensorflow feature to java object.
	 *
	 * @param types feature data type.
	 * @param f     tensorflow feature object.
	 * @return feature corresponds to java object.
	 * @throws CodingException
	 */
	public static Object featureToJava(DataTypesV2 types, Feature f) throws CodingException {
		switch (types) {
			case FLOAT_32:
				return f.getFloatList().getValue(0);
			case FLOAT_64:
				return (double) f.getFloatList().getValue(0);
			case INT_8:
				return (byte) f.getInt64List().getValue(0);
			case UINT_8:
			case INT_16:
				return (short) f.getInt64List().getValue(0);
			case INT_32:
			case UINT_16:
				long v = f.getInt64List().getValue(0);
				return (int) v;
			case UINT_32:
			case INT_64:
				return f.getInt64List().getValue(0);
			case STRING:
				ByteString bs = f.getBytesList().getValue(0);
				return bs.toString(StandardCharsets.UTF_8);
			default:
				throw new CodingException("Unsupported data type: " + types.toString());
		}
	}

	private static ByteString castAsBytes(Object val) {
		if (val instanceof byte[]) {
			byte[] bytes = (byte[]) val;
			return ByteString.copyFrom(bytes);
		}
		if (val instanceof String) {
			String s = (String) val;
			return ByteString.copyFrom(s, StandardCharsets.UTF_8);
		}
		throw new AkIllegalArgumentException("Can't cast object " + val + " to bytes.");
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
		throw new AkIllegalArgumentException("Can't cast object " + val + " to long");
	}
}
