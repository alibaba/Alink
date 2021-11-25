package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.dl.utils.tftensorconv.StringTFTensorConversionImpl;
import com.alibaba.alink.common.dl.utils.tftensorconv.TensorTFTensorConversionImpl;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.google.common.primitives.Floats;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.proto.framework.DataType;
import org.tensorflow.proto.framework.TensorInfo;
import org.tensorflow.proto.framework.TensorShapeProto;
import org.tensorflow.proto.framework.TensorShapeProto.Dim;
import org.tensorflow.types.TFloat32;

import java.util.Random;

public class TF2TensorUtilsTest {

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
	public void testParseFromString() {
		Random random = new Random(2021);
		float[][] data = new float[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextFloat();
			}
		}

		Tensor <TFloat32> std = TFloat32.tensorOf(StdArrays.ndCopyOf(data));
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_FLOAT, new long[] {3, 4});

		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(std);
		Assert.assertEquals(s,
			"0.6187223 0.9288096 0.6796649 0.37785554 0.22008651 0.47162366 0.74732524 0.068377554 0.47145623 0"
				+ ".075398505 0.47539717 0.902691");

		//noinspection unchecked
		Tensor <TFloat32> tensor = (Tensor <TFloat32>) TF2TensorUtils.parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));
	}

	@Test
	public void testParseFromTensor() {
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
		Tensor <TFloat32> tensor = (Tensor <TFloat32>) TF2TensorUtils.parseTensor(data, tensorInfo);
		Assert.assertArrayEquals(std.shape().asArray(), tensor.shape().asArray());
		Assert.assertArrayEquals(StdArrays.array2dCopyOf(std.data()), StdArrays.array2dCopyOf(tensor.data()));

		FloatTensor encoded = (FloatTensor) TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				Assert.assertEquals(encoded.getFloat(i, j), data.getFloat(i, j), 1e-6f);
			}
		}
	}

	@Test
	public void squeezeTensor() {
		Random random = new Random(2021);
		float[][] data = new float[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextFloat();
			}
		}
		Tensor <TFloat32> std = TFloat32.tensorOf(StdArrays.ndCopyOf(data));
		Tensor <TFloat32> squeezed = TF2TensorUtils.squeezeTensor(std, 6);
		Assert.assertEquals(
			StringTFTensorConversionImpl.getInstance().encodeTensor(std),
			StringTFTensorConversionImpl.getInstance().encodeTensor(squeezed)
		);
		Assert.assertArrayEquals(squeezed.shape().asArray(), new long[] {6, 2});
	}

	@Test
	public void testSqueezeTensor() {
		Random random = new Random(2021);
		float[][] data = new float[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextFloat();
			}
		}
		Tensor <TFloat32> std = TFloat32.tensorOf(StdArrays.ndCopyOf(data));
		Tensor <TFloat32> squeezed = TF2TensorUtils.squeezeTensor(std);
		Assert.assertArrayEquals(Floats.concat(data), StdArrays.array1dCopyOf(squeezed.data()), 1e-6F);
	}

	@Test
	public void getTensorShape() {
		TensorInfo tensorInfo = createTensorInfo(DataType.DT_FLOAT, new long[] {3, 4});
		Assert.assertArrayEquals(TF2TensorUtils.getTensorShape(tensorInfo), new long[] {3, 4});
	}
}
