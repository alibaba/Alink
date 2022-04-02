package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class StringTensorTest extends AlinkTestBase {

	private static final Random random = new Random(0);
	private static final char[] charsets = new char[]{' ', 'a', 'b', 'A', 'B', '1', '!'};

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		StringTensor tensor = new StringTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertNull(tensor.getString(i, j));
			}
		}
	}

	@Test
	public void testFromScalar() {
		String v = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
		StringTensor tensor = new StringTensor(v);
		Assert.assertEquals(v, tensor.getString());
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		String[] arr = new String[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
		}
		StringTensor tensor = new StringTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getString(i));
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		String[][] arr = new String[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
			}
		}
		StringTensor tensor = new StringTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getString(i, j));
			}
		}
	}

	@Test
	public void testFrom3DArray() {
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
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getString(i, j, k));
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		StringTensor tensor = new StringTensor(new Shape(3, 4));
		String v = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
		long[] coordinates = new long[] {2, 2};
		tensor.setString(v, coordinates);
		Assert.assertEquals(v, tensor.getString(coordinates));
	}

	@Test
	public void testSerDeNull() {
		StringTensor tensor;
		tensor = new StringTensor(new Shape(3, 4));
		Assert.assertEquals("STRING#3,4#-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 #", TensorUtil.toString(tensor));

		StringTensor tensor2 = (StringTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testSerDe() {
		int n = 3;
		int m = 4;
		String[][] arr = new String[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
			}
		}
		StringTensor tensor = new StringTensor(arr);
		String expected = "STRING#3,4#10 10 10 10 10 10 10 10 10 10 10 10 #b!ab BbBb1  ! A Bb ba !b1!bbBAAa aA AAB bA   ! 1a! A1  bbBAAB1!!  AA1!!1!!1  A AB!bA! A B! bB1ab!1 b!A!  b!!A AbB!A!A!ba  !aabb1!BB ";
		Assert.assertEquals(expected, TensorUtil.toString(tensor));

		StringTensor tensor2 = (StringTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testScalarSerDe() {
		String v = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
		StringTensor tensor = new StringTensor(v);
		Assert.assertEquals("STRING##10 #B!BBa A1b  ", TensorUtil.toString(tensor));

		StringTensor tensor2 = (StringTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		String[][] arr = new String[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
			}
		}
		StringTensor tensor = new StringTensor(arr);
		StringTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}

	@Test
	public void testScalarReshape() {
		String v = RandomStringUtils.random(10, 0, charsets.length, false, false, charsets, random);
		StringTensor tensor = new StringTensor(v);
		StringTensor reshaped = tensor.reshape(new Shape(1, 1, 1, 1));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
