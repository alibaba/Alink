package com.alibaba.alink.common.dl.utils;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.dl.utils.tftensorconv.StringTFTensorConversionImpl;
import com.alibaba.alink.common.dl.utils.tftensorconv.TensorTFTensorConversionImpl;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.proto.framework.TensorShapeProto.Dim;
import org.tensorflow.types.family.TType;

import java.util.List;

public class TF2TensorUtils {

	@SuppressWarnings({"unchecked"})
	public static Tensor <?> parseBatchTensors(List <?> values, TensorInfo tensorInfo) {
		Preconditions.checkArgument(!values.isEmpty(), "Values cannot be empty.");
		Class <?> clazz = values.get(0).getClass();
		if (clazz.equals(String.class)) {
			return StringTFTensorConversionImpl.getInstance().parseBatchTensors((List <String>) values, tensorInfo);
		} else if (com.alibaba.alink.common.linalg.tensor.Tensor.class.isAssignableFrom(clazz)) {
			return TensorTFTensorConversionImpl.getInstance().parseBatchTensors(
				(List <com.alibaba.alink.common.linalg.tensor.Tensor <?>>) values, tensorInfo);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	public static Tensor <?> parseTensor(Object v, TensorInfo tensorInfo) {
		Class <?> clazz = v.getClass();
		if (clazz.equals(String.class)) {
			return StringTFTensorConversionImpl.getInstance().parseTensor((String) v, tensorInfo);
		} else if (com.alibaba.alink.common.linalg.tensor.Tensor.class.isAssignableFrom(clazz)) {
			return TensorTFTensorConversionImpl.getInstance().parseTensor(
				(com.alibaba.alink.common.linalg.tensor.Tensor <?>) v, tensorInfo);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Squeeze all dimensions except the first dimension is reshaped to have a length of `batchSize`.
	 *
	 * @param tensor    Tensor
	 * @param batchSize
	 * @param <T>
	 * @return
	 */
	public static <T extends TType> Tensor <T> squeezeTensor(Tensor <T> tensor, long batchSize) {
		return Tensor.of(tensor.dataType(), Shape.of(batchSize, tensor.shape().size() / batchSize), tensor.rawData());
	}

	public static <T extends TType> Tensor <T> squeezeTensor(Tensor <T> tensor) {
		return Tensor.of(tensor.dataType(), Shape.of(tensor.shape().size()), tensor.rawData());
	}

	public static long[] getTensorShape(TensorInfo tensorInfo) {
		return tensorInfo.getTensorShape().getDimList().stream()
			.mapToLong(Dim::getSize).toArray();
	}
}
