package com.alibaba.alink.operator.common.optim.activeSet;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

public class SqpUtil {

	public static void fillMatrix(double[][] targetMatrix, int rowStart, int colStart, double[][] values) {
		int rowLength = values.length;
		if (rowLength != 0) {
			int colLength = values[0].length;
			if (colLength != 0) {
				for (int i = 0; i < rowLength; i++) {
					System.arraycopy(values[i], 0, targetMatrix[rowStart + i], colStart, colLength);
				}
			}
		}
	}

	//    public static DenseVector slice(DenseVector dv, int start, int length) {
	//        double[] d = dv.getData();
	//        double[] e = new double[length];
	//        System.arraycopy(d, start, e, 0, length);
	//        return new DenseVector(e);
	//    }

	public static DenseMatrix concatMatrixRow(DenseMatrix equalMatrix, DenseMatrix inequalMatrix) {
		int r1 = equalMatrix.numRows();
		int r2 = inequalMatrix.numRows();
		int c = equalMatrix.numCols();
		double[][] res = new double[r1 + r2][c];
		fillMatrix(res, 0, 0, equalMatrix.getArrayCopy2D());
		fillMatrix(res, r1, 0, inequalMatrix.getArrayCopy2D());
		return new DenseMatrix(res);
	}

	public static DenseVector generateMaxVector(DenseVector x, double value) {
		double[] data = x.getData();
		int length = data.length;
		for (int i = 0; i < length; i++) {
			data[i] = Math.max(data[i], value);
		}
		return x;
	}

	public static DenseVector copyVec(DenseVector vector, int start, int length) {
		double[] res = new double[length];
		System.arraycopy(vector.getData(), start, res, 0, length);
		return new DenseVector(res);
	}

	public static Tuple2 <Double, Integer> findMin(DenseVector vector, int start, int length) {
		double min = Double.MAX_VALUE;
		int index = 0;
		for (int i = start; i < (start + length); i++) {
			if (vector.get(i) < min) {
				min = vector.get(i);
				index = i;
			}
		}
		return Tuple2.of(min, index - start);
	}
}
