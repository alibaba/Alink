package com.alibaba.alink.operator.common.optim.activeSet;

import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class QpProblemTest extends AlinkTestBase {
	@Test
	public void test1() {
		DenseMatrix hessian = new DenseMatrix(new double[][] {{1, -1}, {-1, 2}});
		DenseVector c = new DenseVector(new double[] {-6, -2});
		DenseMatrix equalMatrix = new DenseMatrix();
		DenseVector be = new DenseVector();
		DenseMatrix inequalMatrix = new DenseMatrix(new double[][] {{-2, -1}, {1, -1}, {-1, -2}, {1, 0}, {0, 1}});
		DenseVector bi = new DenseVector(new double[] {-3, -1, -2, 0, 0});
		DenseVector x0 = new DenseVector(new double[] {0.5, 0});
		Tuple3 <DenseVector, DenseVector, Integer> res = QpProblem.qpact(hessian, c, equalMatrix, be, inequalMatrix,
			bi,
			x0);

	}

	@Test
	public void test2() {
		DenseMatrix hessian = new DenseMatrix(new double[][] {{2, -1}, {-1, 4}});
		DenseVector c = new DenseVector(new double[] {-1, -10});
		DenseMatrix equalMatrix = new DenseMatrix();
		DenseVector be = new DenseVector();
		DenseMatrix inequalMatrix = new DenseMatrix(new double[][] {{-3, -2}, {1, 0}, {0, 1}});
		DenseVector bi = new DenseVector(new double[] {-6, 0, 0});
		DenseVector x0 = new DenseVector(new double[] {0, 0});
		Tuple3 <DenseVector, DenseVector, Integer> res = QpProblem.qpact(hessian, c, equalMatrix, be, inequalMatrix,
			bi,
			x0);

	}

	@Test
	public void test3() {
		DenseMatrix hessian = new DenseMatrix(new double[][] {{1, -1}, {-1, 2}});
		DenseVector c = new DenseVector(new double[] {-6, -2});
		DenseMatrix equalMatrix = new DenseMatrix(new double[][] {{1, 0},});
		DenseVector be = new DenseVector(new double[] {1.,});
		DenseMatrix inequalMatrix = new DenseMatrix(new double[][] {{1, -1}, {-1, -2}, {1, 0}, {0, 1}});
		DenseVector bi = new DenseVector(new double[] {-1, -2, 0, 0});
		DenseVector x0 = new DenseVector(new double[] {1., 0});
		Tuple3 <DenseVector, DenseVector, Integer> res = QpProblem.qpact(hessian, c, equalMatrix, be, inequalMatrix,
			bi,
			x0);

	}

	@Test
	public void test4() {
		DenseMatrix hessian = new DenseMatrix(new double[][] {{2, 2}, {2, 4}});
		DenseVector c = new DenseVector();
		DenseMatrix equalMatrix = new DenseMatrix();
		DenseVector be = new DenseVector();
		DenseMatrix inequalMatrix = new DenseMatrix();
		DenseVector bi = new DenseVector();
		DenseVector x0 = new DenseVector(new double[] {0, 0});
		Tuple3 <DenseVector, DenseVector, Integer> res = QpProblem.qpact(hessian, c, equalMatrix, be, inequalMatrix,
			bi,
			x0);
	}

	@Test
	public void test5() {
		DenseMatrix hessian = new DenseMatrix(new double[][] {{2, 2}, {2, 4}});
		DenseVector c = new DenseVector(new double[] {-6, -2});
		DenseVector x0 = new DenseVector(new double[] {1., 0});
		QpProblem.qpact(hessian, c, null, null, null, null, x0);
	}

}