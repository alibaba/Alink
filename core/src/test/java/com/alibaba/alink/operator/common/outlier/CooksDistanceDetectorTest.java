package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class CooksDistanceDetectorTest extends AlinkTestBase {

	@Test
	public void test() {
		double[][] yData = new double[][] {
			{4},
			{2},
			{3},
			{0},
			{-1},
			{-2},
			{-5}
		};

		double[][] xData = new double[][] {
			{-3, 1},
			{-2, 1},
			{-1, 1},
			{0, 1},
			{1, 1},
			{2, 1},
			{3, 1}
		};

		DenseMatrix X = new DenseMatrix(xData);
		DenseMatrix Y = new DenseMatrix(yData);

		double[] d = CooksDistanceDetector.cooksDistance(X, Y);

		Assert.assertArrayEquals(d, new double[] {0.092, 0.266, 0.313, 0.002, 0.009, 0.128, 0.829}, 10e-3);
	}

}