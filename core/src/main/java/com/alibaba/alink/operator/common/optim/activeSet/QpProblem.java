package com.alibaba.alink.operator.common.optim.activeSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.util.Arrays;

public class QpProblem {
	public static Tuple3 <DenseVector, DenseVector, Integer> qpact(DenseMatrix hessian, DenseVector grad,
																   DenseMatrix equalMatrix, DenseVector equalConstant,
																   DenseMatrix inequalMatrix,
																   DenseVector inequalConstant,
																   DenseVector x0) {
		int dim = x0.size();
		int hesRow = hessian.numRows();
		int hesCol = hessian.numCols();
		if (hesRow == 0 || hesCol == 0) {
			hessian = DenseMatrix.zeros(dim, dim);
		}
		int gradSize = grad.size();
		if (gradSize == 0) {
			grad = DenseVector.zeros(dim);
		}

		double epsilon = 1e-9;
		double err = 1e-6;
		double iter = 0;
		int maxIter = 1000;
		int exitFlag = 0;
		DenseVector x = x0;

		if (equalConstant == null) {
			equalConstant = new DenseVector();
		}
		if (equalMatrix == null) {
			equalMatrix = new DenseMatrix();
		}
		if (inequalConstant == null) {
			inequalConstant = new DenseVector();
		}
		if (inequalMatrix == null) {
			inequalMatrix = new DenseMatrix();
		}
		int equalNum = equalConstant.size();
		int inequalNum = inequalConstant.size();
		DenseVector lambdaK = DenseVector.zeros(inequalNum + equalNum);
		int[] index = new int[inequalNum];
		Arrays.fill(index, 1);
		double[][] inequalMatrixData = inequalMatrix.getArrayCopy2D();
		for (int i = 0; i < inequalNum; i++) {
			if (BLAS.dot(inequalMatrixData[i], x.getData()) > inequalConstant.get(i) + epsilon) {
				index[i] = 0;
			}
		}
		while (iter <= maxIter) {
			int indexSum = equalNum;
			for (int i = 0; i < inequalNum; i++) {
				indexSum += index[i];
			}
			double[][] constraintsData = new double[indexSum][dim];
			if (equalNum > 0) {
				SqpUtil.fillMatrix(constraintsData, 0, 0, equalMatrix.getArrayCopy2D());
			}
			int begin = equalNum;
			for (int j = 0; j < inequalNum; j++) {
				if (index[j] > 0) {
					System.arraycopy(inequalMatrix.getRow(j), 0, constraintsData[begin], 0, dim);
					begin++;
				}
			}
			DenseMatrix constraintsMatrix = new DenseMatrix(constraintsData);
			DenseVector gk = hessian.multiplies(x).plus(grad);
			int m1 = indexSum;
			DenseVector[] vectors = subProblem(hessian, gk, constraintsMatrix, DenseVector.zeros(m1));
			DenseVector dk = vectors[0];
			lambdaK = vectors[1];
			if (dk.normL1() < err) {
				//y is the min value of lambda, while jk is the index of the lambda.
				double y = 0;
				int jk = 0;
				if (lambdaK.size() > equalNum) {
					Tuple2 <Double, Integer> tuple2 = SqpUtil.findMin(lambdaK, equalNum, m1 - equalNum);
					y = tuple2.f0;
					jk = tuple2.f1 + 1;
				}
				if (y > 0) {
					exitFlag = 0;
				} else {
					exitFlag = 1;
					int indexTempSum = 0;
					for (int i = 0; i < inequalNum; i++) {
						indexTempSum += index[i];
						//seems equalNum no need to plus.
						if (index[i] == 1 & (indexTempSum) == jk) {
							index[i] = 0;
							break;
						}
					}
				}
				iter++;
			} else {
				exitFlag = 1;
				//calculate the step length
				double alpha = 1;
				double step = 1;
				int activeIndex = 0;
				for (int i = 0; i < inequalNum; i++) {
					double[] inequalRow = inequalMatrix.getRow(i);
					if (index[i] == 0 && BLAS.dot(inequalRow, dk.getData()) < 0) {
						double tempStep = (inequalConstant.get(i) - BLAS.dot(inequalRow, x.getData())) /
							BLAS.dot(inequalRow, dk.getData());
						if (tempStep < step) {
							step = tempStep;
							activeIndex = i;
						}
					}
				}
				if (alpha > step) {
					alpha = step;
				}
				x = x.plus(dk.scale(alpha));
				if (step < 1) {
					index[activeIndex] = 1;
				}
			}
			if (exitFlag == 0) {
				break;
			}
			iter++;
		}
		return Tuple3.of(x, lambdaK, exitFlag);
	}

	//this is the sub problem which only considers equality constraints.
	//may use lapack to solve this problem.
	public static DenseVector[] subProblem(DenseMatrix hessian, DenseVector gradient, DenseMatrix equalMatrix,
										   DenseVector equalConstant) {
		//may throw exceptions.
		DenseMatrix ginvH = hessian.inverse();
		int equalNum = equalMatrix.numRows();
		DenseVector lambda;
		DenseVector x;
		if (equalNum > 0) {
			DenseMatrix rb = equalMatrix.multiplies(ginvH);
			DenseMatrix temp = (rb.multiplies(equalMatrix.transpose())).inverse();
			DenseMatrix B = temp.multiplies(rb);
			DenseVector temp2 = B.multiplies(gradient);
			lambda = temp2.plus(temp.multiplies(equalConstant));
			DenseMatrix G = ginvH.minus(ginvH.multiplies(equalMatrix.transpose()).multiplies(temp).multiplies(rb));
			x = B.transpose().multiplies(equalConstant).minus(G.multiplies(gradient));
		} else {
			x = ginvH.multiplies(gradient).scale(-1);
			lambda = DenseVector.zeros(1);
		}
		return new DenseVector[] {x, lambda};
	}
}
