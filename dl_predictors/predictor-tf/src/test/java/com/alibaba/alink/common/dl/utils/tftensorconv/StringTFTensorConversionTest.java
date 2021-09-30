package com.alibaba.alink.common.dl.utils.tftensorconv;

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

import java.util.Arrays;
import java.util.Random;

public class StringTFTensorConversionTest {

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
		float[][] data = new float[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextFloat();
			}
		}

		Tensor <TFloat32> tensor = TFloat32.tensorOf(StdArrays.ndCopyOf(data));
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		Assert.assertEquals(s,
			"0.6187223 0.9288096 0.6796649 0.37785554 0.22008651 0.47162366 0.74732524 0.068377554 0.47145623 0"
				+ ".075398505 0.47539717 0.902691");

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_FLOAT, new long[] {3, 4});
		//noinspection unchecked
		tensor = (Tensor <TFloat32>) StringTFTensorConversionImpl.getInstance().parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data()));
	}

	@Test
	public void testDoubleTensor() {
		Random random = new Random(2021);
		double[][] data = new double[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextDouble();
			}
		}

		Tensor <TFloat64> tensor = TFloat64.tensorOf(StdArrays.ndCopyOf(data));
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		Assert.assertEquals(s,
			"0.6187223781467913 0.6796649601736005 0.2200865517798617 0.7473252723869324 0.47145623081026244 0"
				+ ".47539722774462423 0.26526664838992675 0.629733365216445 0.09871473196875946 0.7199513315785697 0"
				+ ".4170698881096283 0.06087239007362011");

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_DOUBLE, new long[] {3, 4});
		//noinspection unchecked
		tensor = (Tensor <TFloat64>) StringTFTensorConversionImpl.getInstance().parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data()));
	}

	@Test
	public void testIntTensor() {
		Random random = new Random(2021);
		int[][] data = new int[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextInt();
			}
		}

		Tensor <TInt32> tensor = TInt32.tensorOf(StdArrays.ndCopyOf(data));
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		Assert.assertEquals(s,
			"-1637574948 -305760475 -1375828539 1622877329 945264524 2025608375 -1085229635 293679583 2024889149 "
				+ "323834171 2041815496 -417938888");

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_INT32, new long[] {3, 4});
		//noinspection unchecked
		tensor = (Tensor <TInt32>) StringTFTensorConversionImpl.getInstance().parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data()));
	}

	@Test
	public void testLongTensor() {
		Random random = new Random(2021);
		long[][] data = new long[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextLong();
			}
		}

		Tensor <TInt64> tensor = TInt64.tensorOf(StdArrays.ndCopyOf(data));
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		Assert.assertEquals(s,
			"-7033330846714661083 -5909138578285583215 4059880218674615479 -4661025790681337377 8696832673304105275 "
				+ "8769530779368079928 4893305952133419428 -6830213997095553143 1820965478894519531 "
				+ "-5165986238437667120 7693581524700960308 1122897221963077854");

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_INT64, new long[] {3, 4});
		//noinspection unchecked
		tensor = (Tensor <TInt64>) StringTFTensorConversionImpl.getInstance().parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data()));
	}

	@Test
	public void testBoolTensor() {
		Random random = new Random(2021);
		boolean[][] data = new boolean[3][4];
		for (int i = 0; i < 3; i += 1) {
			for (int j = 0; j < 4; j += 1) {
				data[i][j] = random.nextBoolean();
			}
		}

		Tensor <TBool> tensor = TBool.tensorOf(StdArrays.ndCopyOf(data));
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		Assert.assertEquals(s, "true true true false false false true false false false false true");

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_BOOL, new long[] {3, 4});
		//noinspection unchecked
		tensor = (Tensor <TBool>) StringTFTensorConversionImpl.getInstance().parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data()));
	}

	@Test
	public void testStringTensor() {
		String[][] data = new String[1][1];
		data[0][0] = "Hello World!";

		Tensor <TString> tensor = TString.tensorOf(StdArrays.ndCopyOf(data));
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		Assert.assertEquals(s, "Hello World!");

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_STRING, new long[] {1, 1});
		//noinspection unchecked
		tensor = (Tensor <TString>) StringTFTensorConversionImpl.getInstance().parseTensor(s, tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data(), String.class));
	}

	@Test
	public void testStringBatchTensor() {
		String[][] data = new String[2][1];
		data[0][0] = "Hello World!";
		data[1][0] = "Hello World2!";

		Tensor <TString> tensor = TString.tensorOf(StdArrays.ndCopyOf(data));
		String[] s = StringTFTensorConversionImpl.getInstance().encodeBatchTensor(tensor);
		System.out.println(Arrays.toString(s));
		Assert.assertArrayEquals(s, new String[]{"Hello World!", "Hello World2!"});

		TensorInfo tensorInfo = createTensorInfo(DataType.DT_STRING, new long[] {-1, -1});
		//noinspection unchecked
		tensor = (Tensor <TString>) StringTFTensorConversionImpl.getInstance().parseBatchTensors(Arrays.asList(s), tensorInfo);
		Assert.assertArrayEquals(data, StdArrays.array2dCopyOf(tensor.data(), String.class));
	}
}
