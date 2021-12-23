package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.common.linalg.tensor.UByteTensor;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.BooleanNdArray;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.TInt64;
import org.tensorflow.types.TString;
import org.tensorflow.types.TUint8;

/**
 * Conversion between Alink {@link com.alibaba.alink.common.linalg.tensor.Tensor} and TensorFlow {@link Tensor}.
 */
public class TFTensorConversionUtils {

	/**
	 * Convert Alink tensor to TF tensor.
	 *
	 * @param tensor Alink tensor.
	 * @return TF tensor
	 */
	public static Tensor <?> toTFTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?> tensor) {
		switch (tensor.getType()) {
			case FLOAT:
				return TFloat32.tensorOf(TensorInternalUtils.getTensorData((FloatTensor) tensor));
			case DOUBLE:
				return TFloat64.tensorOf(TensorInternalUtils.getTensorData((DoubleTensor) tensor));
			case INT:
				return TInt32.tensorOf(TensorInternalUtils.getTensorData((IntTensor) tensor));
			case LONG:
				return TInt64.tensorOf(TensorInternalUtils.getTensorData((LongTensor) tensor));
			case BOOLEAN:
				return TBool.tensorOf(TensorInternalUtils.getTensorData((BoolTensor) tensor));
			case UBYTE:
				return TUint8.tensorOf(TensorInternalUtils.getTensorData((UByteTensor) tensor));
			case STRING:
				return TString.tensorOf(TensorInternalUtils.getTensorData((StringTensor) tensor));
			case BYTE:
			default:
				throw new UnsupportedOperationException("Unsupported tensor type: " + tensor.getType());
		}
	}

	/**
	 * Convert TF tensor to Alink tensor.
	 *
	 * @param tensor TF tensor.
	 * @return Alink tensor
	 */
	public static com.alibaba.alink.common.linalg.tensor.Tensor <?> fromTFTensor(Tensor <?> tensor) {
		if (TFloat32.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createFloatTensor((FloatNdArray) (tensor.data()));
		} else if (TFloat64.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createDoubleTensor((DoubleNdArray) (tensor.data()));
		} else if (TInt32.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createIntTensor((IntNdArray) (tensor.data()));
		} else if (TInt64.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createLongTensor((LongNdArray) (tensor.data()));
		} else if (TBool.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createBoolTensor((BooleanNdArray) (tensor.data()));
		} else if (TUint8.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createUByteTensor((ByteNdArray) (tensor.data()));
		} else if (TString.DTYPE.equals(tensor.dataType())) {
			return TensorInternalUtils.createStringTensor((NdArray <String>) (tensor.data()));
		}
		throw new UnsupportedOperationException("Unsupported tensor type: " + tensor.dataType());
	}
}
