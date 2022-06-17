package com.alibaba.alink.operator.common.statistics.basicstat;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

/**
 * Util of vector operations.
 *
 * <p>- DenseVector extension.
 *
 * <p>- DenseMatrix extension.
 */
public class VectorUtil {

	/**
	 * return left + normL1(right)
	 * it will change left, right will not be change
	 */
	public static DenseVector plusNormL1(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = sum(leftData[i], Math.abs(rightData[i]));
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = sum(Math.abs(data[i]), leftData[i]);
			}

			return new DenseVector(data);
		}
	}

	/**
	 * return left + right * right.
	 * it will change left, right will not be change.
	 */
	public static DenseVector plusSum2(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = sum(leftData[i], rightData[i] * rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = sum(data[i] * data[i], leftData[i]);
			}
			for (int i = left.size(); i < right.size(); i++) {
				data[i] = data[i] * data[i];
			}

			return new DenseVector(data);
		}
	}

	/**
	 * return left + right
	 * it will change left, right will not be change.
	 */
	public static DenseVector plusEqual(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = sum(leftData[i], rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = sum(data[i], leftData[i]);
			}

			return new DenseVector(data);
		}
	}

	/**
	 * return min(left, right)
	 * it will change left, right will not be change.
	 */
	public static DenseVector minEqual(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = min(leftData[i], rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = min(leftData[i], rightData[i]);
			}

			return new DenseVector(data);
		}
	}

	/**
	 * return max(left,right)
	 * it will change left, right will not be change.
	 */
	public static DenseVector maxEqual(DenseVector left, DenseVector right) {
		double[] leftData = left.getData();
		double[] rightData = right.getData();
		if (left.size() >= right.size()) {
			for (int i = 0; i < right.size(); i++) {
				leftData[i] = max(leftData[i], rightData[i]);
			}
			return left;
		} else {
			double[] data = rightData.clone();
			for (int i = 0; i < left.size(); i++) {
				data[i] = max(leftData[i], rightData[i]);
			}

			return new DenseVector(data);
		}
	}

	/**
	 * return left + right
	 * row and col of right matrix is  equal with or Less than left matrix.
	 * it will change left, right will not be change.
	 */
	public static DenseMatrix plusEqual(DenseMatrix left, DenseMatrix right) {
		for (int i = 0; i < right.numRows(); i++) {
			for (int j = 0; j < right.numCols(); j++) {
				left.add(i, j, right.get(i, j));
			}
		}
		return left;
	}

	/**
	 * deal with nan.
	 */
	private static double sum(double left, double right) {
		Boolean leftNan = Double.isNaN(left);
		Boolean rightNan = Double.isNaN(right);
		if (leftNan && rightNan) {
			return left;
		} else if (leftNan && !rightNan) {
			return right;
		} else if (!leftNan && rightNan) {
			return left;
		} else {
			return left + right;
		}
	}

	/**
	 * deal with nan.
	 */
	private static double min(double left, double right) {
		Boolean leftNan = Double.isNaN(left);
		Boolean rightNan = Double.isNaN(right);
		if (leftNan && rightNan) {
			return left;
		} else if (leftNan && !rightNan) {
			return right;
		} else if (!leftNan && rightNan) {
			return left;
		} else {
			return Math.min(left, right);
		}
	}

	/**
	 * deal with nan.
	 */
	private static double max(double left, double right) {
		Boolean leftNan = Double.isNaN(left);
		Boolean rightNan = Double.isNaN(right);
		if (leftNan && rightNan) {
			return left;
		} else if (leftNan && !rightNan) {
			return right;
		} else if (!leftNan && rightNan) {
			return left;
		} else {
			return Math.max(left, right);
		}
	}
}
