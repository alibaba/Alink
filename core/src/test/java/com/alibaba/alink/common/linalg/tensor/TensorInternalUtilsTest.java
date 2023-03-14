package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tensorflow.ndarray.BooleanNdArray;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.StdArrays;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

@RunWith(Parameterized.class)
public class TensorInternalUtilsTest extends AlinkTestBase {

	private final Object arr;
	private final Tensor <?> tensor;
	private final DataType dataType;
	private final String b64str;
	private final Function <NdArray <?>, Object> ndarray2arr;

	public TensorInternalUtilsTest(Object arr, Tensor <?> tensor,
								   DataType dataType, String b64str,
								   Function <NdArray <?>, Object> ndarray2arr) {
		this.arr = arr;
		this.tensor = tensor;
		this.dataType = dataType;
		this.b64str = b64str;
		this.ndarray2arr = ndarray2arr;
	}

	@Parameters
	public static Collection <Object[]> data() {
		List <Object[]> items = new ArrayList <>();

		Random random = new Random(0);
		int n = 2;
		int m = 3;
		int l = 4;
		int[][][] intArr = new int[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					intArr[i][j][k] = random.nextInt();
				}
			}
		}
		items.add(new Object[] {intArr, new IntTensor(intArr), DataType.INT,
			"YLQguzhR2dR6y5M9vnA5m/bJLaM68B1Pt3DpjAMl9B0+uviYbacSyCvNTVVL8LVAI8KbYk3p75wvkx78WA+a+wgbEuEHsegF8rT18PHQDC0PYmNGcJIcUFhn/yD2qDNe",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((IntNdArray) d)});

		random = new Random(0);
		long[][][] longArr = new long[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					longArr[i][j][k] = random.nextLong();
				}
			}
		}
		items.add(new Object[] {longArr, new LongTensor(longArr), DataType.LONG,
			"OFHZ1F+0ILu+cDmbecuTPTrwHU/2yS2jAyX0Hbdw6YxtpxLIPbr4mEvwtUArzU1VTenvnCLCm2JYD5r7LpMe/Aex6AUIGxLh8dAMLfK09fBwkhxQD2JjRvaoM15YZ/8gOFWGtJevhyW04FoAH+/yBV+PXAIII/eLengdkEKc6Pbn8rxAy+a/GmwOMUmklwmgykLSA5vwKWmW3gj65Ga8xi4jMLAAAqD9RDAg3pVKu3zWOB/7G60Jv5DE7ZGT2q+7",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((LongNdArray) d)});

		random = new Random(0);
		float[][][] floatArr = new float[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					floatArr[i][j][k] = random.nextFloat();
				}
			}
		}
		items.add(new Object[] {floatArr, new FloatTensor(floatArr), DataType.FLOAT,
			"tCA7P1HZVD8sT3Y+cDkbP8ktIz/gO54+cOkMPyih7z26+Bg/pxJIP5qbqj7ga4E+hDfFPunvHD+THnw/D5p7PxsSYT8gFr08tPVwP0AzND7Exow+JDmgPpz9Az5QZ7w+",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((FloatNdArray) d)});

		random = new Random(0);
		double[][][] doubleArr = new double[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					doubleArr[i][j][k] = random.nextDouble();
				}
			}
		}
		items.add(new Object[] {doubleArr, new DoubleTensor(doubleArr), DataType.DOUBLE,
			"icqmjhZk5z8ULmez5cnOP4HveDq5ZeQ/KKHvEC6d4T87lUBGFx/jPwRfC0RzU9U/lP7OifCm2D960Nxn0oPvP4hFL2BDIuw/h2ZombYe7j8myQGF2JjRPxx1xquzf8A/pMqQ1tfDwj+g4FoAvMuXP3rkEmDkfuE/w+uAjBPd7j+4PC/Q5r+6P3OISfIyAeQ/LCQ9IHxK2j/0RtDfjNfoPxmBgUUAtO8/BAPirdIu3z/G+dinNeHnPyRuj1T7dec/",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((DoubleNdArray) d)});

		random = new Random(0);
		byte[][][] byteArr = new byte[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					byteArr[i][j][k] = (byte) random.nextInt();
				}
			}
		}
		items.add(new Object[] {byteArr, new ByteTensor(byteArr), DataType.BYTE,
			"YDh6vvY6twM+bStLI00vWAgH8vEPcFj2",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((ByteNdArray) d)});

		random = new Random(0);
		boolean[][][] booleanArr = new boolean[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					booleanArr[i][j][k] = random.nextBoolean();
				}
			}
		}
		items.add(new Object[] {booleanArr, new BoolTensor(booleanArr), DataType.BOOLEAN,
			"W+MF",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((BooleanNdArray) d)});

		random = new Random(0);
		String[][][] stringArr = new String[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					stringArr[i][j][k] = RandomStringUtils.random(10, 0, 0, true, true, null, random);
				}
			}
		}
		//noinspection unchecked
		items.add(new Object[] {stringArr, new StringTensor(stringArr), DataType.STRING,
			"AAAAGAAAAAo1NzBXaENHdnhMAAAAClFlN3laU3dwQnQAAAAKUmx0ZnRNeFFTaQAAAApFMnBiZGxmSlhBAAAACmZjYWU4RmJzSXIAAAAKTFBVUFFoa2g0YgAAAAo5RmlQYUVNUG1tAAAACmJWU0NKUG9rRm8AAAAKenBxMXJWeFdxWQAAAApISlYxMFp4bGV3AAAACjBzQmxjajdJeGYAAAAKaEVFSEsyeEtWcAAAAApZTFNSVjJjTU53AAAACkNUYThUN2xRajQAAAAKWDN4MkszbEZFNwAAAApjU0E3b1hwOTFmAAAACjR2VUFvS2RCbGgAAAAKUlFOc052b0NNcgAAAAowM3ZlVjVoSlF4AAAACjRQTTVyZHhub3IAAAAKRTFwTHlRMElxNAAAAApPNE1kQk9UZlFnAAAACkJTVlgzZFNuWVYAAAAKa1VRaTBqUlN5Sg==",
			(Function <NdArray <?>, Object>) d -> StdArrays.array3dCopyOf((NdArray <String>) d, String.class)});

		return items;
	}

	@Test
	public void testTensorToBytes() {
		byte[] bytes = TensorInternalUtils.tensorToBytes(tensor);
		System.out.println(Base64.encodeBase64String(bytes));
		Assert.assertEquals(b64str, Base64.encodeBase64String(bytes));
	}

	@Test
	public void testBytesToTensor() {
		Tensor <?> resultTensor = TensorInternalUtils.bytesToTensor(
			Base64.decodeBase64(b64str), dataType, Shape.of(tensor.shape()));
		Assert.assertArrayEquals(tensor.shape(), resultTensor.shape());
		Object actualArr = ndarray2arr.apply(TensorInternalUtils.getTensorData(resultTensor));
		Assert.assertArrayEquals((Object[]) arr, (Object[]) actualArr);
	}
}
