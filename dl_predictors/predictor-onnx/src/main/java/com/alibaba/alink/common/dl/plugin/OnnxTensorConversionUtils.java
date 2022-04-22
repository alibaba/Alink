package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import ai.onnxruntime.OnnxJavaType;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.TensorInfo;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Conversion between Alink {@link Tensor} and ONNX {@link OnnxTensor}.
 */
public class OnnxTensorConversionUtils {

	private static final int INT_SIZE_BYTES = 4;
	private static final int FLOAT_SIZE_BYTES = 4;
	private static final int LONG_SIZE_BYTES = 8;
	private static final int DOUBLE_SIZE_BYTES = 8;

	private static final Map <DataType, OnnxJavaType> TYPES_MAPPING = new HashMap <>();

	static {
		TYPES_MAPPING.put(DataType.FLOAT, OnnxJavaType.FLOAT);
		TYPES_MAPPING.put(DataType.DOUBLE, OnnxJavaType.DOUBLE);
		TYPES_MAPPING.put(DataType.BYTE, OnnxJavaType.INT8);
		TYPES_MAPPING.put(DataType.INT, OnnxJavaType.INT32);
		TYPES_MAPPING.put(DataType.LONG, OnnxJavaType.INT64);
		TYPES_MAPPING.put(DataType.BOOLEAN, OnnxJavaType.BOOL);
		TYPES_MAPPING.put(DataType.STRING, OnnxJavaType.STRING);
	}

	private static ByteBuffer allocateByteBuffer(int numBytes) {
		return ByteBuffer.allocateDirect(numBytes).order(ByteOrder.nativeOrder());
	}

	/**
	 * Convert Alink tensor to ONNX tensor.
	 *
	 * @param tensor     Alink tensor.
	 * @param tensorInfo ONNX Tensor info.
	 * @return ONNX tensor
	 */
	public static OnnxTensor toOnnxTensor(OrtEnvironment env, Tensor <?> tensor, TensorInfo tensorInfo)
		throws OrtException {
		DataType type = tensor.getType();
		Preconditions.checkArgument(
			tensorInfo.type.equals(TYPES_MAPPING.get(type)),
			String.format("Cannot convert a %s Alink tensor to a %s ONNX tensor.", type, tensorInfo.type)
		);
		long[] shape = tensor.shape();
		int size = Math.toIntExact(tensor.size());
		switch (type) {
			case FLOAT:
				FloatTensor floatTensor = (FloatTensor) tensor;
				FloatBuffer floatBuffer = allocateByteBuffer(size * FLOAT_SIZE_BYTES).asFloatBuffer();
				TensorInternalUtils.getTensorData(floatTensor).read(DataBuffers.of(floatBuffer));
				return OnnxTensor.createTensor(env, floatBuffer, shape);
			case DOUBLE:
				DoubleTensor doubleTensor = (DoubleTensor) tensor;
				DoubleBuffer doubleBuffer = allocateByteBuffer(size * DOUBLE_SIZE_BYTES).asDoubleBuffer();
				TensorInternalUtils.getTensorData(doubleTensor).read(DataBuffers.of(doubleBuffer));
				return OnnxTensor.createTensor(env, doubleBuffer, shape);
			case INT:
				IntTensor intTensor = (IntTensor) tensor;
				IntBuffer intBuffer = allocateByteBuffer(size * INT_SIZE_BYTES).asIntBuffer();
				TensorInternalUtils.getTensorData(intTensor).read(DataBuffers.of(intBuffer));
				return OnnxTensor.createTensor(env, intBuffer, shape);
			case LONG:
				LongTensor longTensor = (LongTensor) tensor;
				LongBuffer longBuffer = allocateByteBuffer(size * LONG_SIZE_BYTES).asLongBuffer();
				TensorInternalUtils.getTensorData(longTensor).read(DataBuffers.of(longBuffer));
				return OnnxTensor.createTensor(env, longBuffer, shape);
			case BYTE:
				ByteTensor byteTensor = (ByteTensor) tensor;
				ByteBuffer byteBuffer = allocateByteBuffer(size);
				TensorInternalUtils.getTensorData(byteTensor).read(DataBuffers.of(byteBuffer));
				return OnnxTensor.createTensor(env, byteBuffer, shape);
			case STRING:
				StringTensor stringTensor = (StringTensor) tensor.flatten(0, -1);
				String[] strings = StdArrays.array1dCopyOf(
					TensorInternalUtils.getTensorData(stringTensor), String.class);
				return OnnxTensor.createTensor(env, strings, shape);
			case UBYTE:
			case BOOLEAN:
			default:
				throw new UnsupportedOperationException("Unsupported ONNX tensor type: " + type);
		}
	}

	/**
	 * Convert ONNX tensor to Alink tensor.
	 *
	 * @param tensor ONNX tensor.
	 * @return Alink tensor
	 */
	public static Tensor <?> fromOnnxTensor(OnnxTensor tensor) throws OrtException {
		long[] shape = tensor.getInfo().getShape();
		switch (tensor.getInfo().type) {
			case INT8: {
				ByteTensor byteTensor = new ByteTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(byteTensor).write(DataBuffers.of(tensor.getByteBuffer()));
				return byteTensor;
			}
			case INT32: {
				IntTensor intTensor = new IntTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(intTensor).write(DataBuffers.of(tensor.getIntBuffer()));
				return intTensor;
			}
			case INT64: {
				LongTensor longTensor = new LongTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(longTensor).write(DataBuffers.of(tensor.getLongBuffer()));
				return longTensor;
			}
			case FLOAT: {
				FloatTensor floatTensor = new FloatTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(floatTensor).write(DataBuffers.of(tensor.getFloatBuffer()));
				return floatTensor;
			}
			case DOUBLE: {
				DoubleTensor doubleTensor = new DoubleTensor(new Shape(shape));
				TensorInternalUtils.getTensorData(doubleTensor).write(DataBuffers.of(tensor.getDoubleBuffer()));
				return doubleTensor;
			}
			case STRING: {
				switch (shape.length) {
					case 0:
						return new StringTensor((String) tensor.getValue());
					case 1:
						return new StringTensor((String[]) tensor.getValue());
					case 2:
						return new StringTensor((String[][]) tensor.getValue());
					case 3:
						return new StringTensor((String[][][]) tensor.getValue());
					default:
						throw new RuntimeException("Not supported StringTensor of #dimension > 3");
				}
			}
			case BOOL: {
			}
			case INT16: {
			}
			default: {
				throw new RuntimeException("Unsupported type " + tensor.getInfo().type);
			}
		}
	}
}
