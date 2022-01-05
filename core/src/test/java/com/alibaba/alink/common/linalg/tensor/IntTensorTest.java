package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class IntTensorTest extends AlinkTestBase {

	private static final Random random = new Random(0);

	@Test
	public void testFromShape() {
		int n = 10;
		int m = 15;
		IntTensor tensor = new IntTensor(new Shape(n, m));
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(0, tensor.getInt(i, j));
			}
		}
	}

	@Test
	public void testFromScalar() {
		int v = random.nextInt();
		IntTensor tensor = new IntTensor(v);
		Assert.assertEquals(v, tensor.getInt());
	}

	@Test
	public void testFrom1DArray() {
		int n = 10;
		int[] arr = new int[n];
		for (int i = 0; i < n; i += 1) {
			arr[i] = random.nextInt();
		}
		IntTensor tensor = new IntTensor(arr);
		for (int i = 0; i < n; i += 1) {
			Assert.assertEquals(arr[i], tensor.getInt(i));
		}
	}

	@Test
	public void testFrom2DArray() {
		int n = 10;
		int m = 15;
		int[][] arr = new int[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextInt();
			}
		}
		IntTensor tensor = new IntTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				Assert.assertEquals(arr[i][j], tensor.getInt(i, j));
			}
		}
	}

	@Test
	public void testFrom3DArray() {
		int n = 10;
		int m = 15;
		int l = 18;
		int[][][] arr = new int[n][m][l];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					arr[i][j][k] = random.nextInt();
				}
			}
		}
		IntTensor tensor = new IntTensor(arr);
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				for (int k = 0; k < l; k += 1) {
					Assert.assertEquals(arr[i][j][k], tensor.getInt(i, j, k));
				}
			}
		}
	}

	@Test
	public void testSetGet() {
		IntTensor tensor = new IntTensor(new Shape(3, 4));
		int v = 3;
		long[] coordinates = new long[] {2, 2};
		tensor.setInt(v, coordinates);
		Assert.assertEquals(v, tensor.getInt(coordinates));
	}

	@Test
	public void testSerDe() {
		IntTensor tensor;
		tensor = new IntTensor(new Shape(3, 4));
		Assert.assertEquals("INT#3,4#0 0 0 0 0 0 0 0 0 0 0 0 ", tensor.toString());

		IntTensor tensor2 = (IntTensor) TensorUtil.getTensor(tensor.toString());
		System.out.println(tensor2.toString());
		Assert.assertEquals(tensor.toString(), tensor2.toString());
	}

	@Test
	public void testScalarSerDe() {
		int v = random.nextInt();
		IntTensor tensor = new IntTensor(v);
		Assert.assertEquals("INT##-1155484576 ", tensor.toString());

		IntTensor tensor2 = (IntTensor) TensorUtil.getTensor(tensor.toString());
		System.out.println(tensor2.toString());
		Assert.assertEquals(tensor.toString(), tensor2.toString());
	}

	@Test
	public void testReshape() {
		int n = 2;
		int m = 6;
		int[][] arr = new int[n][m];
		for (int i = 0; i < n; i += 1) {
			for (int j = 0; j < m; j += 1) {
				arr[i][j] = random.nextInt();
			}
		}
		IntTensor tensor = new IntTensor(arr);
		IntTensor reshaped = tensor.reshape(new Shape(2, 3, 2));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}

	@Test
	public void testScalarReshape() {
		int v = random.nextInt();
		IntTensor tensor = new IntTensor(v);
		IntTensor reshaped = tensor.reshape(new Shape(1, 1, 1, 1));
		Assert.assertArrayEquals(tensor.getValueStrings(), reshaped.getValueStrings());
	}
}
