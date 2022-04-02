package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ByteTensorTest extends AlinkTestBase {

	private static final Random random = new Random(0);

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		ByteTensor tensor = new ByteTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(0, tensor.getByte(i, j));
			}
		}
	}

	@Test
	public void testFromScalar() {
		byte v = (byte) random.nextInt();
		ByteTensor tensor = new ByteTensor(v);
		Assert.assertEquals(v, tensor.getByte());
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		byte[] arr = new byte[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = (byte) random.nextInt();
		}
		ByteTensor tensor = new ByteTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getByte(i));
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		byte[][] arr = new byte[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = (byte) random.nextInt();
			}
		}
		ByteTensor tensor = new ByteTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getByte(i, j));
			}
		}
	}

	@Test
	public void testFrom3DArray() {
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
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getByte(i, j, k));
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		ByteTensor tensor = new ByteTensor(new Shape(3, 4));
		byte v = 3;
		long[] coordinates = new long[] {2, 2};
		tensor.setByte(v, coordinates);
		Assert.assertEquals(v, tensor.getByte(coordinates));
	}

	@Test
	public void testSerDe() {
		ByteTensor tensor;
		tensor = new ByteTensor(new Shape(3, 4));
		Assert.assertEquals("BYTE#3,4#0 0 0 0 0 0 0 0 0 0 0 0 ", TensorUtil.toString(tensor));

		ByteTensor tensor2 = (ByteTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testScalarSerDe() {
		byte v = (byte) random.nextInt();
		ByteTensor tensor = new ByteTensor(v);
		Assert.assertEquals("BYTE##96 ", TensorUtil.toString(tensor));

		ByteTensor tensor2 = (ByteTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		byte[][] arr = new byte[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = (byte) random.nextInt();
			}
		}
		ByteTensor tensor = new ByteTensor(arr);
		ByteTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}

	@Test
	public void testScalarReshape() {
		byte v = (byte) random.nextInt();
		ByteTensor tensor = new ByteTensor(v);
		ByteTensor reshaped = tensor.reshape(new Shape(1, 1, 1, 1));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
