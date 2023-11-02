package com.alibaba.alink.operator.common.optim;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.optim.activeSet.SqpUtil;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class SQPUtilTest extends AlinkTestBase {
	@Test
	public void testConcatMatrixRow() {
		double[][] first = new double[][] {
			{1, 2},
			{3, 4}
		};
		double[][] second = new double[][] {
			{11, 12},
			{13, 14},
			{5, 6}
		};
		DenseMatrix res = SqpUtil.concatMatrixRow(new DenseMatrix(first), new DenseMatrix(second));
		for (double[] row : res.getArrayCopy2D()) {
			for (double v : row) {
				System.out.println(v);
			}
			System.out.println("##");
		}
	}

	@Test
	public void testGenerateMaxVector() {
		DenseVector v = new DenseVector(new double[] {1, 2, 3, 4});
		for (double datum : SqpUtil.generateMaxVector(v, 2.5).getData()) {
			System.out.println(datum);
		}
	}

	@Test
	public void testCopyVec() {
		DenseVector v = new DenseVector(new double[] {0, 1, 2, 3, 4});
		for (double vv : SqpUtil.copyVec(v, 1, 3).getData()) {
			System.out.println(vv);
		}
	}

	@Test
	public void testFillMatrix() {
		double[][] source = new double[][] {{1, 2}, {3, 4}};
		double[][] target = new double[3][3];
		SqpUtil.fillMatrix(target, 0, 0, source);
		System.out.println(new DenseMatrix(target).toString());
	}

}