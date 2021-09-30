package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.DoubleDataBuffer;
import org.tensorflow.types.TFloat64;

public class TFTensorConversionUtilsTest {

	@Test
	public void testToTFTensor() {
		DoubleTensor doubleTensor = new DoubleTensor(new double[][] {{1., 2.}, {3., 4.}});
		Tensor <TFloat64> tensor = (Tensor <TFloat64>) TFTensorConversionUtils.toTFTensor(doubleTensor);
		Assert.assertEquals("DOUBLE", tensor.dataType().name());
		Assert.assertArrayEquals(new long[] {2, 2}, tensor.shape().asArray());

		double[] arr = new double[4];
		DoubleDataBuffer buffer = DataBuffers.of(arr, false, false);
		tensor.data().read(buffer);
		Assert.assertArrayEquals(new double[] {1., 2., 3., 4.}, arr, 1e-9);
	}

	@Test
	public void testFromTFTensor() {
		Tensor <TFloat64> tensor = TFloat64.tensorOf(StdArrays.ndCopyOf(new double[][] {{1., 2.}, {3., 4.}}));
		DoubleTensor doubleTensor = (DoubleTensor) TFTensorConversionUtils.fromTFTensor(tensor);
		Assert.assertEquals(DataType.DOUBLE, doubleTensor.getType());
		Assert.assertArrayEquals(new long[] {2, 2}, doubleTensor.shape());

		double[] arr = new double[4];
		DoubleDataBuffer buffer = DataBuffers.of(arr, false, false);
		TensorInternalUtils.getTensorData(doubleTensor).read(buffer);
		Assert.assertArrayEquals(new double[] {1., 2., 3., 4.}, arr, 1e-9);
	}
}
