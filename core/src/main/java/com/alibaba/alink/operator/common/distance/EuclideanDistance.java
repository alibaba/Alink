package com.alibaba.alink.operator.common.distance;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.util.Arrays;

/**
 * Euclidean distance is the "ordinary" straight-line distance between two points in Euclidean space.
 * <p>
 * https://en.wikipedia.org/wiki/Euclidean_distance
 * <p>
 * Given two vectors a and b, Euclidean Distance = ||a - b||, where ||*|| means the L2 norm of the vector.
 */
public class EuclideanDistance extends FastDistance {
	private static final long serialVersionUID = -4458480857602286201L;
	/**
	 * Label size.
	 */
	private static int LABEL_SIZE = 1;

	/**
	 * Calculate the Euclidean distance between two arrays.
	 *
	 * @param array1 array1
	 * @param array2 array2
	 * @return the distance
	 */
	@Override
	public double calc(double[] array1, double[] array2) {
		double s = 0.;
		for (int i = 0; i < array1.length; i++) {
			double d = array1[i] - array2[i];
			s += d * d;
		}
		return Math.sqrt(s);
	}

	/**
	 * Calculate the distance between vec1 and vec2.
	 *
	 * @param vec1 vector1
	 * @param vec2 vector2
	 * @return the distance.
	 */
	@Override
	public double calc(Vector vec1, Vector vec2) {
		return Math.sqrt(MatVecOp.sumSquaredDiff(vec1, vec2));
	}

	/**
	 * For Euclidean distance, distance = sqrt((a - b)^2) = (sqrt(a^2 + b^2 - 2ab)) So we can pre-calculate the L2 norm
	 * square of the vector, and when we need to calculate the distance with another vector, only dot product is
	 * calculated. For FastDistanceVectorData, the label is a one-dimension vector. For FastDistanceMatrixData, the
	 * label is a 1 X n DenseMatrix, n is the number of vectors saved in the matrix.
	 *
	 * @param data FastDistanceData.
	 */
	@Override
	public void updateLabel(FastDistanceData data) {
		if (data instanceof FastDistanceVectorData) {
			FastDistanceVectorData vectorData = (FastDistanceVectorData) data;
			double d = MatVecOp.dot(vectorData.vector, vectorData.vector);
			if (vectorData.label == null || vectorData.label.size() != LABEL_SIZE) {
				vectorData.label = new DenseVector(LABEL_SIZE);
			}
			vectorData.label.set(0, d);
		} else if (data instanceof FastDistanceMatrixData) {
			FastDistanceMatrixData matrix = (FastDistanceMatrixData) data;
			int vectorSize = matrix.vectors.numRows();
			int numVectors = matrix.vectors.numCols();
			if (matrix.label == null || matrix.label.numCols() != numVectors || matrix.label.numRows() != LABEL_SIZE) {
				matrix.label = new DenseMatrix(LABEL_SIZE, numVectors);
			}
			double[] label = matrix.label.getData();
			double[] matrixData = matrix.vectors.getData();
			Arrays.fill(label, 0.0);
			int labelCnt = 0;
			int cnt = 0;
			while (cnt < matrixData.length) {
				int endIndex = cnt + vectorSize;
				while (cnt < endIndex) {
					label[labelCnt] += matrixData[cnt] * matrixData[cnt];
					cnt++;
				}
				labelCnt++;
			}
		} else {
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
						vectorLabel[indices[i][j]] += (values[i][j] * values[i][j]);
					}
				}
			}
		}
	}

	/**
	 * distance = sqrt((a - b)^2) = (sqrt(a^2 + b^2 - 2ab))
	 *
	 * @param left  single vector with label(L2 norm square)
	 * @param right single vector with label(L2 norm square)
	 * @return the distance
	 */
	@Override
	double calc(FastDistanceVectorData left, FastDistanceVectorData right) {
		return Math.sqrt(Math.abs(left.label.get(0) + right.label.get(0) - 2 * left.vector.dot(right.vector)));
	}

	/**
	 * distance = sqrt((a - b)^2) = (sqrt(a^2 + b^2 - 2ab))
	 *
	 * @param leftVector   single vector with label(L2 norm square)
	 * @param rightVectors vectors with labels(L2 norm square array)
	 * @param res          the distances between leftVector and all the vectors in rightVectors.
	 */
	@Override
	void calc(FastDistanceVectorData leftVector, FastDistanceMatrixData rightVectors, double[] res) {
		double[] normL2Square = rightVectors.label.getData();
		BLAS.gemv(-2.0, rightVectors.vectors, true, leftVector.vector, 0.0, new DenseVector(res));
		double vecLabel = leftVector.label.get(0);
		for (int i = 0; i < res.length; i++) {
			res[i] = Math.sqrt(Math.abs(res[i] + vecLabel + normL2Square[i]));
		}
	}

	/**
	 * distance = sqrt((a - b)^2) = (sqrt(a^2 + b^2 - 2ab))
	 *
	 * @param left  vectors with labels(L2 norm square array)
	 * @param right vectors with labels(L2 norm square array)
	 * @param res   the distances between all the vectors in left and all the vectors in right.
	 */
	@Override
	void calc(FastDistanceMatrixData left, FastDistanceMatrixData right, DenseMatrix res) {
		int numRow = right.vectors.numCols();
		BLAS.gemm(-2.0, right.vectors, true, left.vectors, false, 0.0, res);
		double[] leftNormL2Square = left.label.getData();
		double[] rightNormL2Square = right.label.getData();
		double[] data = res.getData();
		int leftCnt = 0;
		int rightCnt = 0;
		for (int i = 0; i < data.length; i++) {
			if (rightCnt == numRow) {
				rightCnt = 0;
				leftCnt++;
			}
			data[i] = Math.sqrt(Math.abs(data[i] + rightNormL2Square[rightCnt++] + leftNormL2Square[leftCnt]));
		}
	}

	@Override
	void calc(FastDistanceVectorData left, FastDistanceSparseData right, double[] res) {
		Arrays.fill(res, 0.0);
		int[][] rightIndices = right.getIndices();
		double[][] rightValues = right.getValues();

		if (left.vector instanceof DenseVector) {
			double[] vector = ((DenseVector) left.vector).getData();
			for (int i = 0; i < vector.length; i++) {
				if (null != rightIndices[i]) {
					for (int j = 0; j < rightIndices[i].length; j++) {
						res[rightIndices[i][j]] -= rightValues[i][j] * vector[i];
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
						res[rightIndices[indices[i]][j]] -= rightValues[indices[i]][j] * values[i];
					}
				}
			}
		}
		double vecLabel = left.label.get(0);
		double[] normL2Square = right.getLabel().getData();
		for (int i = 0; i < res.length; i++) {
			res[i] = Math.sqrt(Math.abs(vecLabel + normL2Square[i] + 2 * res[i]));
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
							res[startIndex + rightIndicesList[k]] -= 2 * rightValuesList[k] * leftValue;
						}
					}
				}
			}
		}
		int leftCnt = 0;
		int rightCnt = 0;
		int numRow = right.vectorNum;
		double[] leftNormL2Square = left.label.getData();
		double[] rightNormL2Square = right.label.getData();
		for (int i = 0; i < res.length; i++) {
			if (rightCnt == numRow) {
				rightCnt = 0;
				leftCnt++;
			}
			res[i] = Math.sqrt(Math.abs(res[i] + rightNormL2Square[rightCnt++] + leftNormL2Square[leftCnt]));
		}
	}

}
