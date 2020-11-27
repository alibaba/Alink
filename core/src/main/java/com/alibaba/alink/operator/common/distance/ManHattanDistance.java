package com.alibaba.alink.operator.common.distance;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.util.Arrays;

/**
 * ManHattan Distance of two points is the sum of the absolute differences of their Cartesian coordinates.
 * <p>
 * https://en.wikipedia.org/wiki/Taxicab_geometry
 * <p>
 * ManHatton Distance is also known as L1 distance, city block distance, taxicab distance.
 * <p>
 * Given two vectors a and b, ManHattan Distance = |a - b|_1, where |*|_1 means the the sum of the absolute differences.
 */
public class ManHattanDistance extends FastDistance {
	private static final long serialVersionUID = -196338747621743163L;
	private static int LABEL_SIZE = 1;

	@Override
	public double calc(double[] vec1, double[] vec2) {
		return calc(vec1, 0, vec2, 0, vec1.length);
	}

	@Override
	public double calc(Vector vec1, Vector vec2) {
		return MatVecOp.sumAbsDiff(vec1, vec2);
	}

	private static double calc(double[] data1, int start1, double[] data2, int start2, int len) {
		double sum = 0.;
		for (int i = 0; i < len; i++) {
			sum += Math.abs(data1[start1 + i] - data2[start2 + i]);
		}
		return sum;
	}

	@Override
	public void updateLabel(FastDistanceData data) {
		if (data instanceof FastDistanceVectorData) {
			FastDistanceVectorData vectorData = (FastDistanceVectorData) data;
			double[] values = (vectorData.vector instanceof DenseVector) ? ((DenseVector) vectorData.vector).getData
				() :
				((SparseVector) vectorData.vector).getValues();
			double d = 0;
			for (double v : values) {
				d += Math.abs(v);
			}
			if (vectorData.label == null || vectorData.label.size() != LABEL_SIZE) {
				vectorData.label = new DenseVector(LABEL_SIZE);
			}
			vectorData.label.set(0, d);
		} else if (data instanceof FastDistanceSparseData) {
			FastDistanceSparseData sparseData = (FastDistanceSparseData) data;
			if (sparseData.label == null || sparseData.label.numCols() != sparseData.vectorNum
				|| sparseData.label.numRows() != LABEL_SIZE) {
				sparseData.label = new DenseMatrix(LABEL_SIZE, sparseData.vectorNum);
			}
			double[] vectorLabel = sparseData.label.getData();
			int[][] indices = sparseData.getIndices();
			double[][] values = sparseData.getValues();
			for (int i = 0; i < indices.length; i++) {
				if (null != indices[i]) {
					for (int j = 0; j < indices[i].length; j++) {
						vectorLabel[indices[i][j]] += Math.abs(values[i][j]);
					}
				}
			}
		}
	}

	@Override
	double calc(FastDistanceVectorData left, FastDistanceVectorData right) {
		return calc(left.vector, right.vector);
	}

	@Override
	void calc(FastDistanceVectorData vector, FastDistanceMatrixData matrix, double[] res) {
		Vector vec = vector.getVector();
		if (vec instanceof DenseVector) {
			double[] vecData = ((DenseVector) vec).getData();
			double[] matrixData = matrix.getVectors().getData();
			int vectorSize = vecData.length;
			for (int i = 0; i < matrix.getVectors().numCols(); i++) {
				res[i] = calc(vecData, 0, matrixData, i * vectorSize, vectorSize);
			}
		} else {
			int[] indices = ((SparseVector) vec).getIndices();
			double[] values = ((SparseVector) vec).getValues();
			DenseMatrix denseMatrix = matrix.getVectors();
			double[] matrixData = denseMatrix.getData();
			int cnt = 0;
			for (int i = 0; i < denseMatrix.numCols(); i++) {
				int p1 = 0;
				for (int j = 0; j < denseMatrix.numRows(); j++) {
					if (p1 < indices.length && indices[p1] == j) {
						res[i] += Math.abs(values[p1] - matrixData[cnt++]);
						p1++;
					} else {
						res[i] += Math.abs(matrixData[cnt++]);
					}
				}
			}
		}
	}

	@Override
	void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res) {
		int vectorSize = right.vectors.numRows();
		int cnt = 0;
		int leftCnt = 0;
		double[] resData = res.getData();
		for (int i = 0; i < res.numCols(); i++) {
			int rightCnt = 0;
			for (int j = 0; j < res.numRows(); j++) {
				resData[cnt++] = calc(right.vectors.getData(), rightCnt, left.vectors.getData(), leftCnt, vectorSize);
				rightCnt += vectorSize;
			}
			leftCnt += vectorSize;
		}
	}

	@Override
	void calc(FastDistanceVectorData left, FastDistanceSparseData right, double[] res) {
		Arrays.fill(res, 0.0);
		int[][] rightIndices = right.getIndices();
		double[][] rightValues = right.getValues();

		if (left.getVector() instanceof DenseVector) {
			double[] leftData = ((DenseVector) left.getVector()).getData();
			for (int i = 0; i < leftData.length; i++) {
				if (null != rightIndices[i]) {
					for (int j = 0; j < rightIndices[i].length; j++) {
						res[rightIndices[i][j]] = res[rightIndices[i][j]] - Math.abs(rightValues[i][j]) - Math.abs(
							leftData[i]) + Math.abs(rightValues[i][j] - leftData[i]);
					}
				}
			}
		} else {
			SparseVector vector = (SparseVector) left.getVector();
			int[] indices = vector.getIndices();
			double[] values = vector.getValues();
			for (int i = 0; i < indices.length; i++) {
				if (null != rightIndices[indices[i]]) {
					for (int j = 0; j < rightIndices[indices[i]].length; j++) {
						res[rightIndices[indices[i]][j]] = res[rightIndices[indices[i]][j]] - Math.abs(
							rightValues[indices[i]][j]) - Math.abs(values[i]) + Math.abs(
							rightValues[indices[i]][j] - values[i]);
					}
				}
			}
		}
		double[] rightLabel = right.getLabel().getData();
		double leftLabel = left.label.get(0);
		for (int i = 0; i < res.length; i++) {
			res[i] += rightLabel[i] + leftLabel;
		}
	}

	@Override
	void calc(FastDistanceSparseData left, FastDistanceSparseData right, double[] res) {
		Arrays.fill(res, 0.0);
		int[][] leftIndices = left.getIndices();
		int[][] rightIndices = right.getIndices();
		double[][] leftValues = left.getValues();
		double[][] rightValues = right.getValues();

		Preconditions.checkArgument(leftIndices.length == rightIndices.length, "VectorSize not equal!");
		for (int i = 0; i < leftIndices.length; i++) {
			int[] leftIndicesList = leftIndices[i];
			int[] rightIndicesList = rightIndices[i];
			double[] leftValuesList = leftValues[i];
			double[] rightValuesList = rightValues[i];
			if (null != leftIndicesList) {
				for (int j = 0; j < leftIndicesList.length; j++) {
					double leftValue = leftValuesList[j];
					int startIndex = leftIndicesList[j] * right.vectorNum;
					if (null != rightIndicesList) {
						for (int k = 0; k < rightIndicesList.length; k++) {
							res[startIndex + rightIndicesList[k]] -= (Math.abs(rightValuesList[k]) + Math.abs
								(leftValue)
								-
								Math.abs(rightValuesList[k] - leftValue));
						}
					}
				}
			}
		}
		int leftCnt = 0;
		int rightCnt = 0;
		int numRow = right.vectorNum;
		double[] leftSum = left.label.getData();
		double[] rightSum = right.label.getData();
		for (int i = 0; i < res.length; i++) {
			if (rightCnt == numRow) {
				rightCnt = 0;
				leftCnt++;
			}
			res[i] += rightSum[rightCnt++] + leftSum[leftCnt];
		}
	}
}
