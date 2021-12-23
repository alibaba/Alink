package com.alibaba.alink.common.dl.utils.tftensorconv;

import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.dl.utils.TFTensorConversionUtils;
import com.alibaba.alink.common.dl.utils.TF2TensorUtils;
import org.tensorflow.Tensor;
import org.tensorflow.proto.framework.DataType;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.TInt64;
import org.tensorflow.types.TString;

import java.lang.reflect.Array;
import java.util.List;

import static com.alibaba.alink.common.linalg.tensor.Tensor.stack;
import static com.alibaba.alink.common.linalg.tensor.Tensor.unstack;

/**
 * Convert {@link com.alibaba.alink.common.linalg.tensor.Tensor} from/to {@link Tensor}.
 */
public class TensorTFTensorConversionImpl
	implements TFTensorConversion <com.alibaba.alink.common.linalg.tensor.Tensor <?>> {

	private final static TensorTFTensorConversionImpl instance = new TensorTFTensorConversionImpl();

	private TensorTFTensorConversionImpl() {
	}

	public static TensorTFTensorConversionImpl getInstance() {
		return instance;
	}

	@Override
	public long[] parseShapeFromValue(com.alibaba.alink.common.linalg.tensor.Tensor <?> v, int nDims) {
		return v.shape();
	}

	@Override
	public Tensor <?> parseBatchTensors(List <com.alibaba.alink.common.linalg.tensor.Tensor <?>> values,
										TensorInfo tensorInfo) {
		int dtype = tensorInfo.getDtypeValue();
		long[] shape = TF2TensorUtils.getTensorShape(tensorInfo);
		adjustShapeFromValues(shape, values, true);

		com.alibaba.alink.common.linalg.tensor.Tensor <?>[] arr = values.toArray(
			new com.alibaba.alink.common.linalg.tensor.Tensor <?>[0]);

		switch (dtype) {
			case DataType.DT_STRING_VALUE: {
				return parseStringTensor(arr, shape);
			}
			case DataType.DT_BOOL_VALUE: {
				return parseBoolTensor(arr, shape);
			}
			case DataType.DT_FLOAT_VALUE: {
				return parseFloatTensor(arr, shape);
			}
			case DataType.DT_DOUBLE_VALUE: {
				return parseDoubleTensor(arr, shape);
			}
			case DataType.DT_INT32_VALUE: {
				return parseIntTensor(arr, shape);
			}
			case DataType.DT_INT64_VALUE: {
				return parseLongTensor(arr, shape);
			}
			default: {
				throw new UnsupportedOperationException("Not support dtype: " + DataType.forNumber(dtype));
			}
		}
	}

	/**
	 * Encode tensor to a list of Alink tensors.
	 *
	 * @param tensor Tensor
	 * @param batchAxis the batch axis
	 * @return a list of tensors.
	 */
	@Override
	public com.alibaba.alink.common.linalg.tensor.Tensor <?>[] encodeBatchTensor(Tensor <?> tensor, int batchAxis) {
		if (TString.DTYPE.equals(tensor.dataType())) {
			StringTensor stringTensor = (StringTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			return unstack(stringTensor, batchAxis, null);
		} else if (TBool.DTYPE.equals(tensor.dataType())) {
			BoolTensor boolTensor = (BoolTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			return unstack(boolTensor, batchAxis, null);
		} else if (TInt32.DTYPE.equals(tensor.dataType())) {
			IntTensor intTensor = (IntTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			return unstack(intTensor, batchAxis, null);
		} else if (TInt64.DTYPE.equals(tensor.dataType())) {
			LongTensor longTensor = (LongTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			return unstack(longTensor, batchAxis, null);
		} else if (TFloat32.DTYPE.equals(tensor.dataType())) {
			FloatTensor floatTensor = (FloatTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			return unstack(floatTensor, batchAxis, null);
		} else if (TFloat64.DTYPE.equals(tensor.dataType())) {
			DoubleTensor doubleTensor = (DoubleTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			return unstack(doubleTensor, batchAxis, null);
		}
		throw new UnsupportedOperationException("Unsupported dtype: " + tensor.dataType());
	}

	@SuppressWarnings("unchecked")
	<T> T[] castArrayType(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors, Class <T[]> clazz) {
		int newLength = tensors.length;
		Class <?> componentType = clazz.getComponentType();
		T[] copy = (clazz == (Object) Object[].class)
			? (T[]) new Object[newLength]
			: (T[]) Array.newInstance(componentType, newLength);
		for (int i = 0; i < newLength; i += 1) {
			if (FloatTensor.class.isAssignableFrom(componentType)) {
				copy[i] = (T) FloatTensor.of(tensors[i]);
			} else if (DoubleTensor.class.isAssignableFrom(componentType)) {
				copy[i] = (T) DoubleTensor.of(tensors[i]);
			} else if (LongTensor.class.isAssignableFrom(componentType)) {
				copy[i] = (T) LongTensor.of(tensors[i]);
			} else {
				copy[i] = (T) tensors[i];
			}
		}
		return copy;
	}

	@Override
	public Tensor <TString> parseStringTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors,
											  long[] shape) {
		StringTensor stackedTensor = new StringTensor(new com.alibaba.alink.common.linalg.tensor.Shape(shape));
		stack(castArrayType(tensors, StringTensor[].class), 0, stackedTensor);
		//noinspection unchecked
		return (Tensor <TString>) TFTensorConversionUtils.toTFTensor(stackedTensor);
	}

	@Override
	public Tensor <TBool> parseBoolTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors, long[] shape) {
		BoolTensor stackedTensor = new BoolTensor(new com.alibaba.alink.common.linalg.tensor.Shape(shape));
		stack(castArrayType(tensors, BoolTensor[].class), 0, stackedTensor);
		//noinspection unchecked
		return (Tensor <TBool>) TFTensorConversionUtils.toTFTensor(stackedTensor);
	}

	@Override
	public Tensor <TFloat32> parseFloatTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors,
											  long[] shape) {
		FloatTensor stackedTensor = new FloatTensor(new com.alibaba.alink.common.linalg.tensor.Shape(shape));
		stack(castArrayType(tensors, FloatTensor[].class), 0, stackedTensor);
		//noinspection unchecked
		return (Tensor <TFloat32>) TFTensorConversionUtils.toTFTensor(stackedTensor);
	}

	@Override
	public Tensor <TInt32> parseIntTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors, long[] shape) {
		IntTensor stackedTensor = new IntTensor(new com.alibaba.alink.common.linalg.tensor.Shape(shape));
		stack(castArrayType(tensors, IntTensor[].class), 0, stackedTensor);
		//noinspection unchecked
		return (Tensor <TInt32>) TFTensorConversionUtils.toTFTensor(stackedTensor);
	}

	@Override
	public Tensor <TInt64> parseLongTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors, long[] shape) {
		LongTensor stackedTensor = new LongTensor(new com.alibaba.alink.common.linalg.tensor.Shape(shape));
		stack(castArrayType(tensors, LongTensor[].class), 0, stackedTensor);
		//noinspection unchecked
		return (Tensor <TInt64>) TFTensorConversionUtils.toTFTensor(stackedTensor);
	}

	@Override
	public Tensor <TFloat64> parseDoubleTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors,
											   long[] shape) {
		DoubleTensor stackedTensor = new DoubleTensor(new com.alibaba.alink.common.linalg.tensor.Shape(shape));
		stack(castArrayType(tensors, DoubleTensor[].class), 0, stackedTensor);
		//noinspection unchecked
		return (Tensor <TFloat64>) TFTensorConversionUtils.toTFTensor(stackedTensor);
	}
}
