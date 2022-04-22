package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import ai.onnxruntime.OnnxJavaType;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.TensorInfo;
import ai.onnxruntime.TensorInfo.OnnxTensorType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.StdArrays;

import java.nio.IntBuffer;
import java.util.Random;

public class OnnxTensorConversionUtilsTest {

	private static final Random random = new Random(0);
	private static final OrtEnvironment env = OrtEnvironment.getEnvironment();
	private static final double eps = 1e-12;
	private static final char[] charsets = new char[] {' ', 'a', 'b', 'A', 'B', '1', '!'};

	@Test
	public void testToOnnxTensorInt() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		int[][][] arr = new int[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextInt();
				}
			}
		}
		IntTensor tensor = new IntTensor(arr);

		TensorInfo tensorInfo = TensorInfo.constructFromJavaArray(arr);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.INT32);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {n, m, l});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		int[][][] actual = (int[][][]) onnxTensor.getValue();

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k]);
				}
			}
		}
	}

	@Test
	public void testFromOnnxTensorInt() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		int[][][] arr = new int[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextInt();
				}
			}
		}
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, arr);
		IntTensor tensor = (IntTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		int[][][] actual = StdArrays.array3dCopyOf((IntNdArray) TensorInternalUtils.getTensorData(tensor));

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k]);
				}
			}
		}
	}

	@Test
	public void testToOnnxTensorLong() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		long[][][] arr = new long[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextLong();
				}
			}
		}
		LongTensor tensor = new LongTensor(arr);

		TensorInfo tensorInfo = TensorInfo.constructFromJavaArray(arr);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.INT64);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {n, m, l});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		long[][][] actual = (long[][][]) onnxTensor.getValue();

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k]);
				}
			}
		}
	}

	@Test
	public void testFromOnnxTensorLong() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		long[][][] arr = new long[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextLong();
				}
			}
		}
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, arr);
		LongTensor tensor = (LongTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		long[][][] actual = StdArrays.array3dCopyOf((LongNdArray) TensorInternalUtils.getTensorData(tensor));

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k]);
				}
			}
		}
	}

	@Test
	public void testToOnnxTensorFloat() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		float[][][] arr = new float[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextFloat();
				}
			}
		}
		FloatTensor tensor = new FloatTensor(arr);

		TensorInfo tensorInfo = TensorInfo.constructFromJavaArray(arr);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.FLOAT);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {n, m, l});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		float[][][] actual = (float[][][]) onnxTensor.getValue();

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k], eps);
				}
			}
		}
	}

	@Test
	public void testFromOnnxTensorFloat() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		float[][][] arr = new float[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextFloat();
				}
			}
		}
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, arr);
		FloatTensor tensor = (FloatTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		float[][][] actual = StdArrays.array3dCopyOf((FloatNdArray) TensorInternalUtils.getTensorData(tensor));

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k], eps);
				}
			}
		}
	}

	@Test
	public void testToOnnxTensorDouble() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		double[][][] arr = new double[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextDouble();
				}
			}
		}
		DoubleTensor tensor = new DoubleTensor(arr);

		TensorInfo tensorInfo = TensorInfo.constructFromJavaArray(arr);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.DOUBLE);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_DOUBLE);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {n, m, l});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		double[][][] actual = (double[][][]) onnxTensor.getValue();

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k], eps);
				}
			}
		}
	}

	@Test
	public void testFromOnnxTensorDouble() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		double[][][] arr = new double[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextDouble();
				}
			}
		}
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, arr);
		DoubleTensor tensor = (DoubleTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		double[][][] actual = StdArrays.array3dCopyOf((DoubleNdArray) TensorInternalUtils.getTensorData(tensor));

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k], eps);
				}
			}
		}
	}

	@Test
	public void testToOnnxTensorByte() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		byte[][][] arr = new byte[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = (byte) random.nextInt();
				}
			}
		}
		ByteTensor tensor = new ByteTensor(arr);

		TensorInfo tensorInfo = TensorInfo.constructFromJavaArray(arr);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.INT8);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT8);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {n, m, l});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		byte[][][] actual = (byte[][][]) onnxTensor.getValue();

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k]);
				}
			}
		}
	}

	@Test
	public void testFromOnnxTensorByte() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		byte[][][] arr = new byte[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = (byte) random.nextInt();
				}
			}
		}
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, arr);
		ByteTensor tensor = (ByteTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		byte[][][] actual = StdArrays.array3dCopyOf((ByteNdArray) TensorInternalUtils.getTensorData(tensor));

		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], actual[i][j][k]);
				}
			}
		}
	}

	@Test
	public void testToOnnxTensorString() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		String[][][] arr = new String[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
				}
			}
		}
		StringTensor tensor = new StringTensor(arr);

		TensorInfo tensorInfo = TensorInfo.constructFromJavaArray(arr);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.STRING);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_STRING);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {n, m, l});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		String[][][] actual = (String[][][]) onnxTensor.getValue();
		Assert.assertArrayEquals(arr, actual);
	}

	@Test
	public void testFromOnnxTensorString() throws OrtException {
		int n = 10;
		int m = 15;
		int l = 18;
		String[][][] arr = new String[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
				}
			}
		}
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, arr);
		StringTensor tensor = (StringTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		String[][][] actual = StdArrays.array3dCopyOf(TensorInternalUtils.getTensorData(tensor), String.class);
		Assert.assertArrayEquals(arr, actual);
	}

	@Test
	public void testToOnnxTensorRank0() throws OrtException {
		int v = random.nextInt();
		IntTensor intTensor = new IntTensor(v);

		TensorInfo tensorInfo = TensorInfo.constructFromBuffer(IntBuffer.wrap(new int[]{v}),
			new long[]{}, OnnxJavaType.INT32);
		Assert.assertEquals(tensorInfo.type, OnnxJavaType.INT32);
		Assert.assertEquals(tensorInfo.onnxType, OnnxTensorType.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32);
		Assert.assertArrayEquals(tensorInfo.getShape(), new long[] {});

		OnnxTensor onnxTensor = OnnxTensorConversionUtils.toOnnxTensor(env, intTensor, tensorInfo);
		int actual = (int) onnxTensor.getValue();
		Assert.assertEquals(v, actual);
	}

	@Test
	public void testFromOnnxTensorRank0() throws OrtException {
		int v = random.nextInt();
		OnnxTensor onnxTensor = OnnxTensor.createTensor(env, IntBuffer.wrap(new int[] {v}), new long[] {});
		IntTensor intTensor = (IntTensor) OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		Assert.assertArrayEquals(intTensor.shape(), new long[] {});
		Assert.assertEquals(v, intTensor.getInt());
	}
}
