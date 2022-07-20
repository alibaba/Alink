package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.util.List;

/**
 * Model data of the Gaussian Mixture model.
 */
public class GmmModelData {
	/**
	 * Number of clusters.
	 */
	public int k;

	/**
	 * Feature dimension.
	 */
	public int dim;

	/**
	 * Name of the vector column of the training data.
	 */
	public String vectorCol;

	/**
	 * Cluster summaries of each clusters.
	 */
	public List <GmmClusterSummary> data;

	/**
	 * Get the matrix element position in the compact format.
	 */
	public static int getElementPositionInCompactMatrix(int i, int j, int n) {
		AkPreconditions.checkArgument(i <= j,
			new AkIllegalArgumentException("i should be smaller than/equal to j"));
		return (1 + j) * j / 2 + i;
	}

	/**
	 * Expand the compressed covariance matrix to a full matrix.
	 *
	 * @param cov Compressed covariance matrix by storing the lower triangle part.
	 * @param n   Feature size.
	 * @return The expanded covariance matrix.
	 */
	public static DenseMatrix expandCovarianceMatrix(DenseVector cov, int n) {
		DenseMatrix mat = new DenseMatrix(n, n);
		for (int i = 0; i < n; i++) {
			for (int j = i; j < n; j++) {
				double v = cov.get(getElementPositionInCompactMatrix(i, j, n));
				mat.set(i, j, v);
				if (i != j) {
					mat.set(j, i, v);
				}
			}
		}
		return mat;
	}
}
