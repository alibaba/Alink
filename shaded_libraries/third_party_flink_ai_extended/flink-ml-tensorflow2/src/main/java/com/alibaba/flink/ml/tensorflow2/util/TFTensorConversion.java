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

package com.alibaba.flink.ml.tensorflow2.util;

import com.google.common.base.Preconditions;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.proto.framework.DataType;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.TInt64;

import java.util.Arrays;

// TODO: support more kinds of tensors

/**
 * a util function: transformation between tensorflow tensor and java object.
 */
public class TFTensorConversion {

	private TFTensorConversion() {
	}

	/**
	 * convert java objects to tensorflow tensor.
	 * @param objects given java objects.
	 * @param tensorInfo target tensor type information.
	 * @return result tensorflow tensor.
	 */
	public static Tensor<?> toTensor(Object[] objects, TensorInfo tensorInfo) {
		DataType dtype = tensorInfo.getDtype();
		switch (dtype) {
			case DT_INT8:
			case DT_INT16:
			case DT_INT32: {
				return TInt32.tensorOf(StdArrays.ndCopyOf(((int[][]) objects)));
			}
			case DT_INT64: {
				return TInt64.tensorOf(StdArrays.ndCopyOf(((long[][]) objects)));
			}
			case DT_FLOAT: {
				return TFloat32.tensorOf(StdArrays.ndCopyOf(((float[][]) objects)));
			}
			case DT_DOUBLE: {
				return TFloat64.tensorOf(StdArrays.ndCopyOf(((double[][]) objects)));
			}
			default:
				throw new UnsupportedOperationException(
						"Type can't be converted to tensor: " + dtype.name());
		}
	}

	/**
	 * convert tensorflow tensor to java objects.
	 * @param tensor given tensorflow tensor.
	 * @return java objects corresponded to given tensor.
	 */
	public static Object[] fromTensor(Tensor<?> tensor) {
		Preconditions.checkArgument(tensor.shape().numDimensions() == 1, "Can only convert tensors with shape long[]");
		final int size = (int) tensor.shape().size(0);
		Object[] res = new Object[size];
		if (TInt32.DTYPE.equals(tensor.dataType())) {
			int[] data = StdArrays.array1dCopyOf((IntNdArray) tensor.data());
			for (int i = 0; i < size; i += 1) {
				res[i] = data[i];
			}
		} else if (TFloat32.DTYPE.equals(tensor.dataType())) {
			float[] data = StdArrays.array1dCopyOf((FloatNdArray) tensor.data());
			for (int i = 0; i < size; i += 1) {
				res[i] = data[i];
			}
		} else if (TInt64.DTYPE.equals(tensor.dataType())) {
			long[] data = StdArrays.array1dCopyOf((LongNdArray) tensor.data());
			for (int i = 0; i < size; i += 1) {
				res[i] = data[i];
			}
		} else if (TFloat64.DTYPE.equals(tensor.dataType())) {
			double[] data = StdArrays.array1dCopyOf((DoubleNdArray) tensor.data());
			for (int i = 0; i < size; i += 1) {
				res[i] = data[i];
			}
		} else {
			throw new UnsupportedOperationException(
				"Type can't be converted from tensor: " + tensor.dataType().name());
		}
		return res;
	}

	private static int getCapacity(long[] shape) {
		if (shape == null || shape.length == 0) {
			return 0;
		}
		long res = shape[0];
		for (int i = 1; i < shape.length; i++) {
			res *= shape[i];
		}
		Preconditions.checkArgument(res >= 0 && res <= Integer.MAX_VALUE, "Invalid shape: " + Arrays.toString(shape));
		return (int) res;
	}
}
