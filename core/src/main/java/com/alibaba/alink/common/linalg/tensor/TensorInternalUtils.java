package com.alibaba.alink.common.linalg.tensor;

import org.apache.flink.annotation.Internal;

import org.tensorflow.ndarray.BooleanNdArray;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.NdArray;

/**
 * This class is only for internal usage, which provides access for some protected members and methods of {@link Tensor}
 * for classes in plugin.
 */
@Internal
public class TensorInternalUtils {
	public static <DT> NdArray <DT> getTensorData(Tensor <DT> tensor) {
		return tensor.getData();
	}

	public static FloatTensor createFloatTensor(FloatNdArray array) {
		return new FloatTensor(array);
	}

	public static DoubleTensor createDoubleTensor(DoubleNdArray array) {
		return new DoubleTensor(array);
	}

	public static IntTensor createIntTensor(IntNdArray array) {
		return new IntTensor(array);
	}

	public static LongTensor createLongTensor(LongNdArray array) {
		return new LongTensor(array);
	}

	public static BoolTensor createBoolTensor(BooleanNdArray array) {
		return new BoolTensor(array);
	}

	public static ByteTensor createByteTensor(ByteNdArray array) {
		return new ByteTensor(array);
	}

	public static UByteTensor createUByteTensor(ByteNdArray array) {
		return new UByteTensor(array);
	}

	public static StringTensor createStringTensor(NdArray <String> array) {
		return new StringTensor(array);
	}
}
