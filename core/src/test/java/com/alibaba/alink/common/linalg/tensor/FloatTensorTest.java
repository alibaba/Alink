package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class FloatTensorTest extends AlinkTestBase {

	private static final Random random = new Random(0);
	private static final double eps = 1e-12;

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		FloatTensor tensor = new FloatTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(0., tensor.getFloat(i, j), eps);
			}
		}
	}

	@Test
	public void testFromScalar() {
		float v = random.nextFloat();
		FloatTensor tensor = new FloatTensor(v);
		Assert.assertEquals(v, tensor.getFloat(), eps);
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		float[] arr = new float[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = random.nextFloat();
		}
		FloatTensor tensor = new FloatTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getFloat(i), eps);
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		float[][] arr = new float[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextFloat();
			}
		}
		FloatTensor tensor = new FloatTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getFloat(i, j), eps);
			}
		}
	}

	@Test
	public void testFrom3DArray() {
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
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getFloat(i, j, k), eps);
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		FloatTensor tensor = new FloatTensor(new Shape(3, 4));
		float v = 3.5f;
		long[] coordinates = new long[] {2, 2};
		tensor.setFloat(v, coordinates);
		Assert.assertEquals(v, tensor.getFloat(coordinates), eps);
	}

	@Test
	public void testSerDe() {
		FloatTensor tensor;
		tensor = new FloatTensor(new Shape(3, 4));
		Assert.assertEquals("FLOAT#3,4#0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 ", tensor.toString());

		FloatTensor tensor2 = (FloatTensor) TensorUtil.getTensor(tensor.toString());
		System.out.println(tensor2.toString());
		Assert.assertEquals(tensor.toString(), tensor2.toString());
	}

	@Test
	public void testScalarSerDe() {
		float v = random.nextFloat();
		FloatTensor tensor = new FloatTensor(v);
		Assert.assertEquals("FLOAT##0.73096776 ", tensor.toString());

		FloatTensor tensor2 = (FloatTensor) TensorUtil.getTensor(tensor.toString());
		System.out.println(tensor2.toString());
		Assert.assertEquals(tensor.toString(), tensor2.toString());
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		float[][] arr = new float[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextFloat();
			}
		}
		FloatTensor tensor = new FloatTensor(arr);
		FloatTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}

	@Test
	public void testScalarReshape() {
		float v = random.nextFloat();
		FloatTensor tensor = new FloatTensor(v);
		FloatTensor reshaped = tensor.reshape(new Shape(1, 1, 1, 1));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
