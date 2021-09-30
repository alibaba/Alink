package com.alibaba.alink.common.linalg.tensor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class BoolTensorTest {

	private static final Random random = new Random(0);

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		BoolTensor tensor = new BoolTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(false, tensor.getBoolean(i, j));
			}
		}
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		boolean[] arr = new boolean[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = random.nextBoolean();
		}
		BoolTensor tensor = new BoolTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getBoolean(i));
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		boolean[][] arr = new boolean[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextBoolean();
			}
		}
		BoolTensor tensor = new BoolTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getBoolean(i, j));
			}
		}
	}

	@Test
	public void testFrom3DArray() {
		int n = 10;
		int m = 15;
		int l = 18;
		boolean[][][] arr = new boolean[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextBoolean();
				}
			}
		}
		BoolTensor tensor = new BoolTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getBoolean(i, j, k));
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		BoolTensor tensor = new BoolTensor(new Shape(3, 4));
		boolean v = true;
		long[] coordinates = new long[] {2, 2};
		tensor.setBoolean(v, coordinates);
		Assert.assertEquals(v, tensor.getBoolean(coordinates));
	}

	@Test
	public void testSerDe() {
		BoolTensor tensor;
		tensor = new BoolTensor(new Shape(3, 4));
		Assert.assertEquals("BOOLEAN#3,4#false false false false false false false false false false false false ", tensor.toString());

		BoolTensor tensor2 = (BoolTensor) TensorUtil.getTensor(tensor.toString());
		System.out.println(tensor2.toString());
		Assert.assertEquals(tensor.toString(), tensor2.toString());
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		boolean[][] arr = new boolean[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextBoolean();
			}
		}
		BoolTensor tensor = new BoolTensor(arr);
		BoolTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
