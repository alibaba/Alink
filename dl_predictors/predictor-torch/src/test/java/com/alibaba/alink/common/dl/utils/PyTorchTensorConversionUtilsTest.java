package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import org.junit.Assert;
import org.junit.Test;
import org.pytorch.DType;
import org.pytorch.Tensor;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.DoubleDataBuffer;

public class PyTorchTensorConversionUtilsTest {

	@Test
	public void testToPyTorchTensor() {
		DoubleTensor doubleTensor = new DoubleTensor(new double[][] {{1., 2.}, {3., 4.}});
		Tensor tensor = PyTorchTensorConversionUtils.toPTTensor(doubleTensor);
		Assert.assertEquals(DType.FLOAT64, tensor.dtype());
		Assert.assertArrayEquals(new long[] {2, 2}, tensor.shape());
		Assert.assertArrayEquals(new double[] {1., 2., 3., 4.}, tensor.getDataAsDoubleArray(), 1e-9);
	}

	@Test
	public void testFromPyTorchTensor() {
		Tensor tensor = Tensor.fromBlob(new double[] {1., 2., 3., 4.}, new long[] {2, 2});
		DoubleTensor doubleTensor = (DoubleTensor) PyTorchTensorConversionUtils.fromPyTorchTensor(tensor);
		Assert.assertEquals(DataType.DOUBLE, doubleTensor.getType());
		Assert.assertArrayEquals(new long[] {2, 2}, doubleTensor.shape());

		double[] arr = new double[4];
		DoubleDataBuffer buffer = DataBuffers.of(arr, false, false);
		TensorInternalUtils.getTensorData(doubleTensor).read(buffer);
		Assert.assertArrayEquals(new double[] {1., 2., 3., 4.}, arr, 1e-9);
	}
}
