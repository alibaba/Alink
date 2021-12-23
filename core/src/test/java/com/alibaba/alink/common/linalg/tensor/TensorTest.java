package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.linalg.Vector;
import org.junit.Assert;
import org.junit.Test;

public class TensorTest {

	private static final float FLOAT_EPS = 0.00001f;

	private static final FloatTensor A = new FloatTensor(
		new float[][][] {
			new float[][] {
				new float[] {0.f, 0.1f},
				new float[] {1.f, 1.1f},
				new float[] {2.f, 2.1f}
			},
			new float[][] {
				new float[] {3.f, 3.1f},
				new float[] {4.f, 4.1f},
				new float[] {5.f, 5.1f}
			},
			new float[][] {
				new float[] {6.f, 6.1f},
				new float[] {7.f, 7.1f},
				new float[] {8.f, 8.1f}
			},
		}
	);

	private static final FloatTensor B = new FloatTensor(
		new float[][] {
			new float[] {0.f, 0.1f},
			new float[] {1.f, 1.1f},
			new float[] {2.f, 2.1f}
		}
	);

	private static final FloatTensor C = new FloatTensor(
		new float[][] {
			new float[] {3.f, 3.1f},
			new float[] {4.f, 4.1f},
			new float[] {5.f, 5.1f}
		}
	);

	@Test
	public void stack() {
		FloatTensor a = new FloatTensor(new Shape(4, 3, 4));
		FloatTensor b = new FloatTensor(new Shape(4, 3, 4));
		FloatTensor c = Tensor.stack(new FloatTensor[] {a, b}, 1, null);

		Assert.assertArrayEquals(new long[] {4L, 2L, 3L, 4L}, c.shape());
	}

	@Test
	public void stackDim0() {
		FloatTensor a = new FloatTensor(new Shape(4, 3, 4));
		FloatTensor b = new FloatTensor(new Shape(4, 3, 4));
		FloatTensor c = Tensor.stack(new FloatTensor[] {a, b}, 0, null);

		Assert.assertArrayEquals(new long[] {2L, 4L, 3L, 4L}, c.shape());
	}

	@Test
	public void stackActual() {

		FloatTensor a = Tensor.stack(new FloatTensor[] {B, C}, 1, null);

		Assert.assertEquals(0.f, a.getFloat(0, 0, 0), FLOAT_EPS);
		Assert.assertEquals(0.1f, a.getFloat(0, 0, 1), FLOAT_EPS);
		Assert.assertEquals(3.f, a.getFloat(0, 1, 0), FLOAT_EPS);
		Assert.assertEquals(3.1f, a.getFloat(0, 1, 1), FLOAT_EPS);
		Assert.assertEquals(1.f, a.getFloat(1, 0, 0), FLOAT_EPS);
		Assert.assertEquals(1.1f, a.getFloat(1, 0, 1), FLOAT_EPS);
		Assert.assertEquals(4.f, a.getFloat(1, 1, 0), FLOAT_EPS);
		Assert.assertEquals(4.1f, a.getFloat(1, 1, 1), FLOAT_EPS);
		Assert.assertEquals(2.f, a.getFloat(2, 0, 0), FLOAT_EPS);
		Assert.assertEquals(2.1f, a.getFloat(2, 0, 1), FLOAT_EPS);
		Assert.assertEquals(5.f, a.getFloat(2, 1, 0), FLOAT_EPS);
		Assert.assertEquals(5.1f, a.getFloat(2, 1, 1), FLOAT_EPS);
	}

	@Test
	public void unstack() {
		FloatTensor a = new FloatTensor(new Shape(4, 3, 2, 4));

		FloatTensor[] b = Tensor.unstack(a, 2, null);

		Assert.assertEquals(2, b.length);
		Assert.assertArrayEquals(new long[] {4, 3, 4}, b[0].shape());
		Assert.assertArrayEquals(new long[] {4, 3, 4}, b[1].shape());
	}

	@Test
	public void unstackActual() {

		FloatTensor[] b = Tensor.unstack(A, 1, null);

		Assert.assertEquals(0.f, b[0].getFloat(0, 0), FLOAT_EPS);
		Assert.assertEquals(0.1f, b[0].getFloat(0, 1), FLOAT_EPS);
		Assert.assertEquals(3.f, b[0].getFloat(1, 0), FLOAT_EPS);
		Assert.assertEquals(3.1f, b[0].getFloat(1, 1), FLOAT_EPS);
		Assert.assertEquals(6.f, b[0].getFloat(2, 0), FLOAT_EPS);
		Assert.assertEquals(6.1f, b[0].getFloat(2, 1), FLOAT_EPS);

		Assert.assertEquals(1.f, b[1].getFloat(0, 0), FLOAT_EPS);
		Assert.assertEquals(1.1f, b[1].getFloat(0, 1), FLOAT_EPS);
		Assert.assertEquals(4.f, b[1].getFloat(1, 0), FLOAT_EPS);
		Assert.assertEquals(4.1f, b[1].getFloat(1, 1), FLOAT_EPS);
		Assert.assertEquals(7.f, b[1].getFloat(2, 0), FLOAT_EPS);
		Assert.assertEquals(7.1f, b[1].getFloat(2, 1), FLOAT_EPS);

		Assert.assertEquals(2.f, b[2].getFloat(0, 0), FLOAT_EPS);
		Assert.assertEquals(2.1f, b[2].getFloat(0, 1), FLOAT_EPS);
		Assert.assertEquals(5.f, b[2].getFloat(1, 0), FLOAT_EPS);
		Assert.assertEquals(5.1f, b[2].getFloat(1, 1), FLOAT_EPS);
		Assert.assertEquals(8.f, b[2].getFloat(2, 0), FLOAT_EPS);
		Assert.assertEquals(8.1f, b[2].getFloat(2, 1), FLOAT_EPS);
	}

	@Test
	public void flatten() {
		Tensor <?> flattened = A.flatten(0, -1);
		Assert.assertEquals(1, flattened.shape().length);
		Assert.assertEquals(18, flattened.shape()[0]);

		flattened = A.flatten(1, 2);
		Assert.assertEquals(2, flattened.shape().length);
		Assert.assertEquals(3, flattened.shape()[0]);
		Assert.assertEquals(6, flattened.shape()[1]);
	}

	@Test
	public void toVector() {
		Vector v = DoubleTensor.of(A).toVector();
		Assert.assertEquals(18, v.size());
		Assert.assertEquals(0.0, v.get(0), FLOAT_EPS);
		Assert.assertEquals(0.1, v.get(1), FLOAT_EPS);
		Assert.assertEquals(8.0, v.get(16), FLOAT_EPS);
		Assert.assertEquals(8.1, v.get(17), FLOAT_EPS);
		Assert.assertEquals(72.9, v.normL1(), FLOAT_EPS);
	}

	@Test
	public void vectorConverter() {
		Assert.assertEquals(
			A,
			FloatTensor
				.of(
					TensorUtil.getTensor(
						DoubleTensor.of(TensorUtil.getTensor(A.toString())).toVector()
					)
				)
				.reshape(new Shape(A.shape()))
		);
	}

	@Test
	public void sum() {
		System.out.println(B);
		System.out.println(B.sum(-2, false));
		Assert.assertEquals(
			TensorUtil.getTensor("FLOAT#3,2#3.0 3.3 12.0 12.299999 21.0 21.3 "),
			A.sum(-2, false)
		);
		Assert.assertEquals(
			TensorUtil.getTensor("FLOAT#3,2#9.0 9.299999 12.0 12.299999 15.0 15.3 "),
			A.sum(0, false)
		);
		Assert.assertEquals(
			TensorUtil.getTensor("FLOAT#3,3#0.1 2.1 4.1 6.1 8.1 10.1 12.1 14.1 16.1 "),
			A.sum(2, false)
		);
		Assert.assertEquals(
			TensorUtil.getTensor("FLOAT#3,1,2#3.0 3.3 12.0 12.299999 21.0 21.3 "),
			A.sum(-2, true)
		);
		Assert.assertEquals(
			TensorUtil.getTensor("FLOAT#1,3,2#9.0 9.299999 12.0 12.299999 15.0 15.3 "),
			A.sum(0, true)
		);
		Assert.assertEquals(
			TensorUtil.getTensor("FLOAT#3,3,1#0.1 2.1 4.1 6.1 8.1 10.1 12.1 14.1 16.1 "),
			A.sum(2, true)
		);
	}

	@Test
	public void cat() {
		Assert.assertEquals(
			TensorUtil.getTensor(
				"FLOAT#9,3,2#0.0 0.1 1.0 1.1 2.0 2.1 3.0 3.1 4.0 4.1 5.0 5.1 6.0 6.1 7.0 7.1 8.0 8.1 0.0 0.1 1.0 1.1 2"
					+ ".0 2.1 3.0 3.1 4.0 4.1 5.0 5.1 6.0 6.1 7.0 7.1 8.0 8.1 0.0 0.1 1.0 1.1 2.0 2.1 3.0 3.1 4.0 4.1 "
					+ "5.0 5.1 6.0 6.1 7.0 7.1 8.0 8.1"),
			Tensor.cat(new FloatTensor[] {A, A, A}, 0, null)
		);
		Assert.assertEquals(
			TensorUtil.getTensor(
				"FLOAT#3,3,6#0.0 0.1 0.0 0.1 0.0 0.1 1.0 1.1 1.0 1.1 1.0 1.1 2.0 2.1 2.0 2.1 2.0 2.1 3.0 3.1 3.0 3.1 3"
					+ ".0 3.1 4.0 4.1 4.0 4.1 4.0 4.1 5.0 5.1 5.0 5.1 5.0 5.1 6.0 6.1 6.0 6.1 6.0 6.1 7.0 7.1 7.0 7.1 "
					+ "7.0 7.1 8.0 8.1 8.0 8.1 8.0 8.1"),
			Tensor.cat(new FloatTensor[] {A, A, A}, 2, null)
		);
		Assert.assertEquals(
			TensorUtil.getTensor(
				"FLOAT#3,9,2#0.0 0.1 1.0 1.1 2.0 2.1 0.0 0.1 1.0 1.1 2.0 2.1 0.0 0.1 1.0 1.1 2.0 2.1 3.0 3.1 4.0 4.1 5"
					+ ".0 5.1 3.0 3.1 4.0 4.1 5.0 5.1 3.0 3.1 4.0 4.1 5.0 5.1 6.0 6.1 7.0 7.1 8.0 8.1 6.0 6.1 7.0 7.1 "
					+ "8.0 8.1 6.0 6.1 7.0 7.1 8.0 8.1"),
			Tensor.cat(new FloatTensor[] {A, A, A}, -2, null)
		);
	}
}