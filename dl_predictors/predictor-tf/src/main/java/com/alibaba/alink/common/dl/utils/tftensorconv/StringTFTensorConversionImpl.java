package com.alibaba.alink.common.dl.utils.tftensorconv;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.dl.utils.TFTensorConversionUtils;
import com.alibaba.alink.common.dl.utils.TF2TensorUtils;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.proto.framework.DataType;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.TInt64;
import org.tensorflow.types.TString;
import org.tensorflow.types.family.TType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.common.linalg.tensor.Tensor.unstack;

/**
 * Convert String from/to {@link Tensor}.
 * <p>
 * The string is composited by all values joined with {@link StringTFTensorConversionImpl#SEP_CHAR}, and can be prepended
 * with shape string surrounded by {@link StringTFTensorConversionImpl#SHAPE_CHAR}.
 */
public class StringTFTensorConversionImpl implements TFTensorConversion <String> {
	public static final String SHAPE_CHAR = "$";
	public static final String SEP_CHAR = " ";

	private final static StringTFTensorConversionImpl instance = new StringTFTensorConversionImpl();

	private StringTFTensorConversionImpl() {
	}

	public static StringTFTensorConversionImpl getInstance() {
		return instance;
	}

	@Override
	public long[] parseShapeFromValue(String v, int nDims) {
		long[] defaultShape = new long[nDims];
		Arrays.fill(defaultShape, -1);
		if (!v.startsWith(SHAPE_CHAR)) {
			return defaultShape;
		}

		int pos = v.lastIndexOf(SHAPE_CHAR);    // TODO: fix possible existence in data part
		Preconditions.checkArgument(pos > 0);
		String shapeStr = v.substring(1, pos).trim();
		if (shapeStr.isEmpty()) {
			return defaultShape;
		}

		String[] dimSizeStrings = shapeStr.split("(,|\\s+)");    // split by commas or whitespaces.
		Preconditions.checkArgument(dimSizeStrings.length == nDims,
			String.format("Shape str [%s] is not consistent with #dims %d.", shapeStr, nDims));
		return Arrays.stream(dimSizeStrings).mapToLong(Long::parseLong).toArray();
	}

	@Override
	public Tensor <?> parseBatchTensors(List<String> values, TensorInfo tensorInfo) {
		int batchSize = values.size();
		int dtype = tensorInfo.getDtypeValue();
		long[] shape = TF2TensorUtils.getTensorShape(tensorInfo);
		adjustShapeFromValues(shape, values, true);

		String[] allValueStrings;
		if (dtype != DataType.DT_STRING_VALUE) {
			allValueStrings = values.stream()
				.flatMap(d -> Arrays.stream(extractValueStrings(d)))
				.toArray(String[]::new);
		} else {
			long numElements = 1;
			for (int i = 0; i < shape.length; i += 1) {
				if (shape[i] == -1) {
					shape[i] = 1;
				}
				numElements *= shape[i];
			}
			Preconditions.checkArgument(
				numElements == batchSize,
				String.format("String tensor can only have 1 element, but current tensor's shape is [%s].",
					Arrays.toString(shape))
			);
			allValueStrings = values.toArray(new String[0]);
		}

		// If there is only a dimension with size -1, we set it manually.
		{
			Integer undeterminedDim = null;
			int dimSize = allValueStrings.length;
			for (int i = 0; i < shape.length; i += 1) {
				if (shape[i] == -1) {
					if (null == undeterminedDim) {
						undeterminedDim = i;
					} else {
						throw new RuntimeException(
							"There are more than 1 dimensions are of size -1: " + Arrays.toString(shape));
					}
				} else {
					dimSize /= shape[i];
				}
			}
			if (null != undeterminedDim) {
				shape[undeterminedDim] = dimSize;
			}
		}

		switch (dtype) {
			case DataType.DT_STRING_VALUE: {
				return parseStringTensor(allValueStrings, shape);
			}
			case DataType.DT_BOOL_VALUE: {
				return parseBoolTensor(allValueStrings, shape);
			}
			case DataType.DT_FLOAT_VALUE: {
				return parseFloatTensor(allValueStrings, shape);
			}
			case DataType.DT_DOUBLE_VALUE: {
				return parseDoubleTensor(allValueStrings, shape);
			}
			case DataType.DT_INT32_VALUE: {
				return parseIntTensor(allValueStrings, shape);
			}
			case DataType.DT_INT64_VALUE: {
				return parseLongTensor(allValueStrings, shape);
			}
			default: {
				throw new UnsupportedOperationException("Not support dtype: " + DataType.forNumber(dtype));
			}
		}
	}

	@Override
	public String[] encodeBatchTensor(Tensor <?> tensor, int batchAxis) {
		long[] shape = tensor.shape().asArray();
		long batchSize = shape[batchAxis];

		if (TString.DTYPE.equals(tensor.dataType())) {
			StringTensor stringTensor = (StringTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			StringTensor[] stringTensors = unstack(stringTensor, batchAxis, null);
			return Arrays.stream(stringTensors)
				.map(TFTensorConversionUtils::toTFTensor)
				.map(TF2TensorUtils::squeezeTensor)
				.map(d -> StdArrays.array1dCopyOf((TString) d.data(), String.class))
				.map(d -> String.join(SEP_CHAR, d))
				.toArray(String[]::new);
		} else if (TBool.DTYPE.equals(tensor.dataType())) {
			BoolTensor boolTensor = (BoolTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			BoolTensor[] boolTensors = unstack(boolTensor, batchAxis, null);
			return Arrays.stream(boolTensors)
				.map(TFTensorConversionUtils::toTFTensor)
				.map(TF2TensorUtils::squeezeTensor)
				.map(d -> StdArrays.array1dCopyOf((TBool) d.data()))
				.map(d -> Booleans.join(SEP_CHAR, d))
				.toArray(String[]::new);
		} else if (TInt32.DTYPE.equals(tensor.dataType())) {
			IntTensor intTensor = (IntTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			IntTensor[] intTensors = unstack(intTensor, batchAxis, null);
			return Arrays.stream(intTensors)
				.map(TFTensorConversionUtils::toTFTensor)
				.map(TF2TensorUtils::squeezeTensor)
				.map(d -> StdArrays.array1dCopyOf((TInt32) d.data()))
				.map(d -> Ints.join(SEP_CHAR, d))
				.toArray(String[]::new);
		} else if (TInt64.DTYPE.equals(tensor.dataType())) {
			LongTensor longTensor = (LongTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			LongTensor[] longTensors = unstack(longTensor, batchAxis, null);
			return Arrays.stream(longTensors)
				.map(TFTensorConversionUtils::toTFTensor)
				.map(TF2TensorUtils::squeezeTensor)
				.map(d -> StdArrays.array1dCopyOf((TInt64) d.data()))
				.map(d -> Longs.join(SEP_CHAR, d))
				.toArray(String[]::new);
		} else if (TFloat32.DTYPE.equals(tensor.dataType())) {
			FloatTensor floatTensor = (FloatTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			FloatTensor[] floatTensors = unstack(floatTensor, batchAxis, null);
			return Arrays.stream(floatTensors)
				.map(TFTensorConversionUtils::toTFTensor)
				.map(TF2TensorUtils::squeezeTensor)
				.map(d -> StdArrays.array1dCopyOf((TFloat32) d.data()))
				.map(d -> Floats.join(SEP_CHAR, d))
				.toArray(String[]::new);
		} else if (TFloat64.DTYPE.equals(tensor.dataType())) {
			DoubleTensor doubleTensor = (DoubleTensor) TFTensorConversionUtils.fromTFTensor(tensor);
			DoubleTensor[] doubleTensors = unstack(doubleTensor, batchAxis, null);
			return Arrays.stream(doubleTensors)
				.map(TFTensorConversionUtils::toTFTensor)
				.map(TF2TensorUtils::squeezeTensor)
				.map(d -> StdArrays.array1dCopyOf((TFloat64) d.data()))
				.map(d -> Doubles.join(SEP_CHAR, d))
				.toArray(String[]::new);
		}
		throw new UnsupportedOperationException("Unsupported dtype: " + tensor.dataType());
	}

	private static <T extends TType> Tensor <T> reshapeTensor(Tensor <T> tensor, long[] shape) {
		return Tensor.of(tensor.dataType(), Shape.of(shape), tensor.rawData());
	}

	/**
	 * String tensor can only have 1 element, i.e. tensor.shape.size() == 1
	 */
	@Override
	public Tensor <TString> parseStringTensor(String[] values, long[] shape) {
		Tensor <TString> tensor = TString.tensorOf(StandardCharsets.UTF_8, StdArrays.ndCopyOf(values));
		return reshapeTensor(tensor, shape);
	}

	@Override
	public Tensor <TBool> parseBoolTensor(String[] valueStrings, long[] shape) {
		boolean[] data = new boolean[valueStrings.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = Boolean.parseBoolean(valueStrings[i]);
		}
		Tensor <TBool> tensor = TBool.tensorOf(StdArrays.ndCopyOf(data));
		return reshapeTensor(tensor, shape);
	}

	@Override
	public Tensor <TFloat32> parseFloatTensor(String[] valueStrings, long[] shape) {
		float[] data = new float[valueStrings.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = Float.parseFloat(valueStrings[i]);
		}
		Tensor <TFloat32> tensor = TFloat32.tensorOf(StdArrays.ndCopyOf(data));
		return reshapeTensor(tensor, shape);
	}

	@Override
	public Tensor <TInt32> parseIntTensor(String[] valueStrings, long[] shape) {
		int[] data = new int[valueStrings.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = Integer.parseInt(valueStrings[i]);
		}
		Tensor <TInt32> tensor = TInt32.tensorOf(StdArrays.ndCopyOf(data));
		return reshapeTensor(tensor, shape);
	}

	@Override
	public Tensor <TInt64> parseLongTensor(String[] valueStrings, long[] shape) {
		long[] data = new long[valueStrings.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = Long.parseLong(valueStrings[i]);
		}
		Tensor <TInt64> tensor = TInt64.tensorOf(StdArrays.ndCopyOf(data));
		return reshapeTensor(tensor, shape);
	}

	@Override
	public Tensor <TFloat64> parseDoubleTensor(String[] valueStrings, long[] shape) {
		double[] data = new double[valueStrings.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = Double.parseDouble(valueStrings[i]);
		}
		Tensor <TFloat64> tensor = TFloat64.tensorOf(StdArrays.ndCopyOf(data));
		return reshapeTensor(tensor, shape);
	}

	private static String[] extractValueStrings(String s) {
		if (s.startsWith(SHAPE_CHAR)) {
			s = s.substring(s.lastIndexOf(SHAPE_CHAR) + 1);
		}
		if (StringUtils.isNullOrWhitespaceOnly(s)) {
			return new String[0];
		}
		return s.split("(,|\\s+)");
	}
}
