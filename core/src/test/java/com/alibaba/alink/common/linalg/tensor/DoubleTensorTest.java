package com.alibaba.alink.common.linalg.tensor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class DoubleTensorTest {

	private static final Random random = new Random(0);
	private static final double eps = 1e-12;

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		DoubleTensor tensor = new DoubleTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(0., tensor.getDouble(i, j), eps);
			}
		}
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		double[] arr = new double[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = random.nextDouble();
		}
		DoubleTensor tensor = new DoubleTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getDouble(i), eps);
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		double[][] arr = new double[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextDouble();
			}
		}
		DoubleTensor tensor = new DoubleTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getDouble(i, j), eps);
			}
		}
	}

	@Test
	public void testFrom3DArray() {
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
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getDouble(i, j, k), eps);
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		DoubleTensor tensor = new DoubleTensor(new Shape(3, 4));
		double v = 3.5;
		long[] coordinates = new long[] {2, 2};
		tensor.setDouble(v, coordinates);
		Assert.assertEquals(v, tensor.getDouble(coordinates), eps);
	}

	@Test
	public void testSerDe() {
		DoubleTensor tensor;
		tensor = new DoubleTensor(new Shape(3, 4));
		Assert.assertEquals("DOUBLE#3,4#0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 ", tensor.toString());

		DoubleTensor tensor2 = (DoubleTensor) TensorUtil.getTensor(tensor.toString());
		System.out.println(tensor2.toString());
		Assert.assertEquals(tensor.toString(), tensor2.toString());
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		double[][] arr = new double[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextDouble();
			}
		}
		DoubleTensor tensor = new DoubleTensor(arr);
		DoubleTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
