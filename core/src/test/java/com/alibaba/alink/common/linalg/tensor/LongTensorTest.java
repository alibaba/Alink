package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class LongTensorTest extends AlinkTestBase {

	private static final Random random = new Random(0);

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		LongTensor tensor = new LongTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(0, tensor.getLong(i, j));
			}
		}
	}

	@Test
	public void testFromScalar() {
		long v = random.nextLong();
		LongTensor tensor = new LongTensor(v);
		Assert.assertEquals(v, tensor.getLong());
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		long[] arr = new long[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = random.nextLong();
		}
		LongTensor tensor = new LongTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getLong(i));
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		long[][] arr = new long[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextLong();
			}
		}
		LongTensor tensor = new LongTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getLong(i, j));
			}
		}
	}

	@Test
	public void testFrom3DArray() {
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
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getLong(i, j, k));
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		LongTensor tensor = new LongTensor(new Shape(3, 4));
		long v = 3;
		long[] coordinates = new long[] {2, 2};
		tensor.setLong(v, coordinates);
		Assert.assertEquals(v, tensor.getLong(coordinates));
	}

	@Test
	public void testSerDe() {
		LongTensor tensor;
		tensor = new LongTensor(new Shape(3, 4));
		Assert.assertEquals("LONG#3,4#0 0 0 0 0 0 0 0 0 0 0 0 ", TensorUtil.toString(tensor));

		LongTensor tensor2 = (LongTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testScalarSerDe() {
		long v = random.nextLong();
		LongTensor tensor = new LongTensor(v);
		Assert.assertEquals("LONG##-4962768465676381896 ", TensorUtil.toString(tensor));

		LongTensor tensor2 = (LongTensor) TensorUtil.getTensor(TensorUtil.toString(tensor));
		System.out.println(TensorUtil.toString(tensor2));
		Assert.assertEquals(TensorUtil.toString(tensor), TensorUtil.toString(tensor2));
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		long[][] arr = new long[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextLong();
			}
		}
		LongTensor tensor = new LongTensor(arr);
		LongTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}

	@Test
	public void testScalarReshape() {
		long v = random.nextLong();
		LongTensor tensor = new LongTensor(v);
		LongTensor reshaped = tensor.reshape(new Shape(1, 1, 1, 1));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
