package com.alibaba.alink.common.dl.utils.tftensorconv;

import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.proto.framework.DataType;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.proto.framework.TensorShapeProto;
import org.tensorflow.proto.framework.TensorShapeProto.Dim;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.TInt64;
import org.tensorflow.types.TString;

import java.util.Random;

public class TensorTFTensorConversionTest {

	static TensorInfo createTensorInfo(DataType dataType, long[] shape) {
		TensorShapeProto.Builder tensorShapeProtoBuilder = TensorShapeProto.newBuilder();
		for (long l : shape) {
			tensorShapeProtoBuilder.addDim(
				Dim.newBuilder().setSize(l).build()
			);
		}
		return TensorInfo.newBuilder()
			.setDtype(dataType)
			.setTensorShape(tensorShapeProtoBuilder)
			.build();
	}

	@Test
	public void testFloatTensor() {
		Random random = new Random(2021);
		float[][] arr = new float[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				arr[i][j] = random.nextFloat();
			}
		}
		FloatTensor data = new FloatTensor(arr);
		Tensor <TFloat32> std = TFloat32.tensorOf(StdArrays.ndCopyOf(arr));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_FLOAT, new long[] {3, 4});

		//noinspection unchecked
		Tensor <TFloat32> tensor = (Tensor <TFloat32>) TensorTFTensorConversionImpl.getInstance().parseTensor(data,
			tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));

		FloatTensor encoded = (FloatTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getFloat(i, j), data.getFloat(i, j), 1e-6F);
			}
		}
	}

	@Test
	public void testDoubleTensor() {
		Random random = new Random(2021);
		double[][] arr = new double[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				arr[i][j] = random.nextDouble();
			}
		}
		DoubleTensor data = new DoubleTensor(arr);

		Tensor <TFloat64> std = TFloat64.tensorOf(StdArrays.ndCopyOf(arr));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_DOUBLE, new long[] {3, 4});

		//noinspection unchecked
		Tensor <TFloat64> tensor = (Tensor <TFloat64>) TensorTFTensorConversionImpl.getInstance().parseTensor(data,
			tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));

		DoubleTensor encoded = (DoubleTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getDouble(i, j), data.getDouble(i, j), 1e-6);
			}
		}
	}

	@Test
	public void testIntTensor() {
		Random random = new Random(2021);
		int[][] arr = new int[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				arr[i][j] = random.nextInt();
			}
		}
		IntTensor data = new IntTensor(arr);

		Tensor <TInt32> std = TInt32.tensorOf(StdArrays.ndCopyOf(arr));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_INT32, new long[] {3, 4});

		//noinspection unchecked
		Tensor <TInt32> tensor = (Tensor <TInt32>) TensorTFTensorConversionImpl.getInstance().parseTensor(data, tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));

		IntTensor encoded = (IntTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getInt(i, j), data.getInt(i, j));
			}
		}
	}

	@Test
	public void testLongTensor() {
		Random random = new Random(2021);
		long[][] arr = new long[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				arr[i][j] = random.nextLong();
			}
		}
		LongTensor data = new LongTensor(arr);

		Tensor <TInt64> std = TInt64.tensorOf(StdArrays.ndCopyOf(arr));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_INT64, new long[] {3, 4});

		//noinspection unchecked
		Tensor <TInt64> tensor = (Tensor <TInt64>) TensorTFTensorConversionImpl.getInstance().parseTensor(data, tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));

		LongTensor encoded = (LongTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getLong(i, j), data.getLong(i, j));
			}
		}
	}

	@Test
	public void testBoolTensor() {
		Random random = new Random(2021);
		boolean[][] arr = new boolean[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				arr[i][j] = random.nextBoolean();
			}
		}
		BoolTensor data = new BoolTensor(arr);

		Tensor <TBool> std = TBool.tensorOf(StdArrays.ndCopyOf(arr));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_BOOL, new long[] {3, 4});

		//noinspection unchecked
		Tensor <TBool> tensor = (Tensor <TBool>) TensorTFTensorConversionImpl.getInstance().parseTensor(data, tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));

		BoolTensor encoded = (BoolTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getBoolean(i, j), data.getBoolean(i, j));
			}
		}
	}

	@Test
	public void testStringTensor() {
		String[][] arr = new String[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				arr[i][j] = RandomStringUtils.random(6);
			}
		}
		StringTensor data = new StringTensor(arr);

		Tensor <TString> std = TString.tensorOf(StdArrays.ndCopyOf(arr));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_STRING, new long[] {3, 4});

		//noinspection unchecked
		Tensor <TString> tensor = (Tensor <TString>) TensorTFTensorConversionImpl.getInstance().parseTensor(data,
			tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data(), String.class),
			StdArrays.array2dCopyOf(tensor.data(), String.class));

		StringTensor encoded = (StringTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getString(i, j), data.getString(i, j));
			}
		}
	}
}
