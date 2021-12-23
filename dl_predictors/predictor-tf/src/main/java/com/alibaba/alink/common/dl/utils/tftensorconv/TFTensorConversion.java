package com.alibaba.alink.common.dl.utils.tftensorconv;

import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.ArrayUtils;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.proto.framework.TensorShapeProto;
import org.tensorflow.proto.framework.TensorShapeProto.Dim;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.TInt64;
import org.tensorflow.types.TString;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Convert values from/to {@link Tensor}
 * <p>
 * Different types of values can be supported, like {@link String}, Java arrays, and {@link
 * com.alibaba.alink.common.linalg.Vector}.
 *
 * @param <V> type of values
 */
public interface TFTensorConversion<V> {

	long[] parseShapeFromValue(V v, int nDims);

	/**
	 * Parse shape information from `values`, fill all -1's in `shape` with actual values.
	 *
	 * @param shape    the shape provided from TensorInfo, may contain multiple -1's
	 * @param values   a batch of values expected to have same lengths in all dimensions.
	 * @param checkAll check where the batch of values have same shapes, and whether the given shape is consistent with
	 */
	default void adjustShapeFromValues(long[] shape, List<V> values, boolean checkAll) {
		int batchSize = values.size();
		Preconditions.checkArgument(batchSize > 0, "Batch size must > 0.");

		if (shape[0] == -1) {
			shape[0] = batchSize;
		}
		Preconditions.checkArgument(shape[0] == batchSize, "Batch size is not equal to the first dimension in shape.");

		long[] valueShape = parseShapeFromValue(values.get(0), shape.length - 1);
		for (int i = 0; i < valueShape.length; i += 1) {
			if (shape[i + 1] == -1) {
				shape[i + 1] = valueShape[i];
			}
			if (valueShape[i] != -1 && shape[i + 1] != valueShape[i]) {
				throw new RuntimeException(
					String.format("The shape of value %s if not consistent with given shape %s.",
						Arrays.toString(valueShape), Arrays.toString(shape)));
			}
		}

		if (checkAll) {
			for (int i = 1; i < values.size(); i += 1) {
				long[] arrShape = parseShapeFromValue(values.get(i), shape.length - 1);
				if (!Arrays.equals(valueShape, arrShape)) {
					throw new RuntimeException(String.format("Values have different shapes [%s] and [%s].",
						Arrays.toString(valueShape), Arrays.toString(arrShape)));
				}
			}
		}
	}

	/**
	 * Parse each value in `values` to a TF tensor, then stack them along axis 0.
	 *
	 * @param values
	 * @param tensorInfo
	 * @return
	 */
	Tensor <?> parseBatchTensors(List <V> values, TensorInfo tensorInfo);

	default Tensor <?> parseTensor(V v, TensorInfo tensorInfo) {
		TensorInfo.Builder tensorInfoBuilder = tensorInfo.toBuilder();
		tensorInfoBuilder.setTensorShape(
			TensorShapeProto.newBuilder()
				.addDim(Dim.newBuilder().setSize(1).build())
				.addAllDim(tensorInfo.getTensorShape().getDimList()).build()
		);
		tensorInfo = tensorInfoBuilder.build();
		Tensor <?> tensor = parseBatchTensors(Collections.singletonList(v), tensorInfo);
		long[] shape = ArrayUtils.remove(tensor.shape().asArray(), 0);
		return Tensor.of(tensor.dataType(), Shape.of(shape), tensor.rawData());
	}

	Tensor <TString> parseStringTensor(V[] values, long[] shape);

	Tensor <TBool> parseBoolTensor(V[] valueStrings, long[] shape);

	Tensor <TFloat32> parseFloatTensor(V[] valueStrings, long[] shape);

	Tensor <TFloat64> parseDoubleTensor(V[] valueStrings, long[] shape);

	Tensor <TInt32> parseIntTensor(V[] valueStrings, long[] shape);

	Tensor <TInt64> parseLongTensor(V[] valueStrings, long[] shape);

	default V[] encodeBatchTensor(Tensor <?> tensor) {
		return encodeBatchTensor(tensor, 0);
	}

	/**
	 * Unstack `tensor` along the `batchAxis` to a list of tensors, then encode each tensor to a value of type V.
	 *
	 * @param tensor
	 * @param batchAxis
	 * @return
	 */
	V[] encodeBatchTensor(Tensor <?> tensor, int batchAxis);

	default V encodeTensor(Tensor <?> tensor) {
		return encodeTensor(tensor, 0);
	}

	/**
	 * Encode `tensor` to a value of type V.
	 *
	 * @param tensor
	 * @param batchAxis
	 * @return
	 */
	default V encodeTensor(Tensor <?> tensor, int batchAxis) {
		long[] shape = ArrayUtils.add(tensor.shape().asArray(), batchAxis, 1L);
		tensor = Tensor.of(tensor.dataType(), Shape.of(shape), tensor.rawData());
		return encodeBatchTensor(tensor, batchAxis)[0];
	}
}
