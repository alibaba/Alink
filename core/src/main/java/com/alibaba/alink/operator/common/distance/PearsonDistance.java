package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

import java.util.Arrays;

/**
 * Note: After updateLabel, the vector itself could be changed.
 */
public class PearsonDistance extends CosineDistance {
	private static final long serialVersionUID = 6706414118581906920L;

	/**
	 * Calculate the Cosine distance between two arrays.
	 *
	 * @param array1 array1
	 * @param array2 array2
	 * @return the distance
	 */
	@Override
	public double calc(double[] array1, double[] array2) {
		minusAvg(array1);
		minusAvg(array2);
		double dot = BLAS.dot(array1, array2);
		double cross = Math.sqrt(BLAS.dot(array1, array1) * BLAS.dot(array2, array2));
		return 1.0 - (cross > 0.0 ? dot / cross : 0.0);
	}

	private static void minusAvg(double[] array) {
		double avg = Arrays.stream(array).sum() / array.length;
		for (int i = 0; i < array.length; i++) {
			array[i] -= avg;
		}
	}

	private static void minusAvg(Vector vector) {
		if (vector instanceof DenseVector) {
			double[] data = ((DenseVector) vector).getData();
			minusAvg(data);
		} else {
			double[] data = ((SparseVector) vector).getValues();
			minusAvg(data);
		}
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
		minusAvg(vec1);
		minusAvg(vec2);
		double dot = MatVecOp.dot(vec1, vec2);
		double cross = vec1.normL2() * vec2.normL2();
		return 1.0 - (cross > 0.0 ? dot / cross : 0.0);
	}

	/**
	 * For Cosine distance, distance = 1 - a * b / ||a|| / ||b|| So we can tranform the vector the unit vector, and
	 * when
	 * we need to calculate the distance with another vector, only dot product is calculated.
	 *
	 * @param data FastDistanceData.
	 */
	@Override
	public void updateLabel(FastDistanceData data) {
		if (data instanceof FastDistanceVectorData) {
			FastDistanceVectorData vectorData = (FastDistanceVectorData) data;
			minusAvg(vectorData.vector);
			double vectorLabel = Math.sqrt(MatVecOp.dot(vectorData.vector, vectorData.vector));
			if (vectorLabel > 0) {
				vectorData.vector.scaleEqual(1.0 / vectorLabel);
			}
		} else if (data instanceof FastDistanceMatrixData) {
			FastDistanceMatrixData matrix = (FastDistanceMatrixData) data;
			int vectorSize = matrix.vectors.numRows();
			double[] matrixData = matrix.vectors.getData();

			int cnt = 0;
			while (cnt < matrixData.length) {
				int endIndex = cnt + vectorSize;
				double sqSum = 0.;
				double sum = 0.;
				while (cnt < endIndex) {
					sum += matrixData[cnt];
					sqSum += matrixData[cnt] * matrixData[cnt];
					cnt++;
				}
				double avg = sum / vectorSize;
				double den = Math.sqrt(sqSum - vectorSize * avg * avg);
				if (den > 0) {
					for (int i = cnt - vectorSize; i < cnt; i++) {
						matrixData[i] = (matrixData[i] - avg) / den;
					}
				}
			}
		} else {
			FastDistanceSparseData sparseData = (FastDistanceSparseData) data;
			double[] sqSum = new double[sparseData.vectorNum];
			double[] sum = new double[sparseData.vectorNum];
			int[] cnt = new int[sparseData.vectorNum];
			int[][] indices = sparseData.getIndices();
			double[][] values = sparseData.getValues();
			for (int i = 0; i < indices.length; i++) {
				if (null != indices[i]) {
					for (int j = 0; j < indices[i].length; j++) {
						sum[indices[i][j]] += values[i][j];
						sqSum[indices[i][j]] += (values[i][j] * values[i][j]);
						cnt[indices[i][j]]++;
					}
				}
			}
			for (int i = 0; i < sqSum.length; i++) {
				sum[i] /= cnt[i];
				sqSum[i] = Math.sqrt(sqSum[i] - cnt[i] * sum[i] * sum[i]);
			}
			for (int i = 0; i < indices.length; i++) {
				if (null != indices[i]) {
					for (int j = 0; j < indices[i].length; j++) {
						if (sqSum[indices[i][j]] > 0) {
							values[i][j] = (values[i][j] - sum[indices[i][j]]) / sqSum[indices[i][j]];
						}
					}
				}
			}
		}
	}
}
