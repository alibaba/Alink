package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.common.linalg.tensor.UByteTensor;
import org.pytorch.Tensor;
import org.tensorflow.ndarray.buffer.DataBuffers;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

public class PyTorchTensorConversionUtils {

	/**
	 * Convert Alink tensor to PyTorch tensor.
	 *
	 * @param tensor Alink tensor.
	 * @return PyTorch tensor
	 */
	public static Tensor toPTTensor(com.alibaba.alink.common.linalg.tensor.Tensor <?> tensor) {
		long[] shape = tensor.shape();
		int size = Math.toIntExact(tensor.size());
		switch (tensor.getType()) {
			case FLOAT:
				FloatTensor floatTensor = (FloatTensor) tensor;
				FloatBuffer floatBuffer = Tensor.allocateFloatBuffer(size);
				TensorInternalUtils.getTensorData(floatTensor).read(DataBuffers.of(floatBuffer));
				return Tensor.fromBlob(floatBuffer, shape);
			case DOUBLE:
				DoubleTensor doubleTensor = (DoubleTensor) tensor;
				DoubleBuffer doubleBuffer = Tensor.allocateDoubleBuffer(size);
				TensorInternalUtils.getTensorData(doubleTensor).read(DataBuffers.of(doubleBuffer));
				return Tensor.fromBlob(doubleBuffer, shape);
			case INT:
				IntTensor intTensor = (IntTensor) tensor;
				IntBuffer intBuffer = Tensor.allocateIntBuffer(size);
				TensorInternalUtils.getTensorData(intTensor).read(DataBuffers.of(intBuffer));
				return Tensor.fromBlob(intBuffer, shape);
			case LONG:
				LongTensor longTensor = (LongTensor) tensor;
				LongBuffer longBuffer = Tensor.allocateLongBuffer(size);
				TensorInternalUtils.getTensorData(longTensor).read(DataBuffers.of(longBuffer));
				return Tensor.fromBlob(longBuffer, shape);
			case BYTE:
				ByteTensor byteTensor = (ByteTensor) tensor;
				ByteBuffer byteBuffer = Tensor.allocateByteBuffer(size);
				TensorInternalUtils.getTensorData(byteTensor).read(DataBuffers.of(byteBuffer));
				return Tensor.fromBlob(byteBuffer, shape);
			case UBYTE:
				UByteTensor ubyteTensor = (UByteTensor) tensor;
				ByteBuffer ubyteBuffer = Tensor.allocateByteBuffer(size);
				TensorInternalUtils.getTensorData(ubyteTensor).read(DataBuffers.of(ubyteBuffer));
				return Tensor.fromBlobUnsigned(ubyteBuffer, shape);
			case BOOLEAN:
			case STRING:
			default:
				throw new UnsupportedOperationException("Unsupported PyTorch tensor type: " + tensor.getType());
		}
	}

	/**
	 * Convert PyTorch tensor to Alink tensor.
	 *
	 * @param tensor PyTorch tensor.
	 * @return Alink tensor
	 */
	public static com.alibaba.alink.common.linalg.tensor.Tensor <?> fromPyTorchTensor(Tensor tensor) {
		long[] shape = tensor.shape();
		switch (tensor.dtype()) {
			case UINT8: {
				byte[] data = tensor.getDataAsUnsignedByteArray();
				UByteTensor ubyteTensor = new UByteTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(ubyteTensor).write(DataBuffers.of(data));
				return ubyteTensor;
			}
			case INT8: {
				byte[] data = tensor.getDataAsByteArray();
				ByteTensor byteTensor = new ByteTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(byteTensor).write(DataBuffers.of(data));
				return byteTensor;
			}
			case INT32: {
				int[] data = tensor.getDataAsIntArray();
				IntTensor intTensor = new IntTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(intTensor).write(DataBuffers.of(data));
				return intTensor;
			}
			case FLOAT32: {
				float[] data = tensor.getDataAsFloatArray();
				FloatTensor floatTensor = new FloatTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(floatTensor).write(DataBuffers.of(data));
				return floatTensor;
			}
			case INT64: {
				long[] data = tensor.getDataAsLongArray();
				LongTensor longTensor = new LongTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(longTensor).write(DataBuffers.of(data));
				return longTensor;
			}
			case FLOAT64: {
				double[] data = tensor.getDataAsDoubleArray();
				DoubleTensor doubleTensor = new DoubleTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(doubleTensor).write(DataBuffers.of(data));
				return doubleTensor;
			}
			default: {
				throw new RuntimeException("Unsupported type " + tensor.dtype());
			}
		}
	}
}
