package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
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
    public List<ClusterSummary> data;

    /**
     * Summary of a Gaussian Mixture Model cluster.
     */
    public static class ClusterSummary implements Serializable {
        /**
         * Id of the cluster.
         */
        public long clusterId;

        /**
         * Weight of the cluster.
         */
        public double weight;

        /**
         * Mean of Gaussian distribution.
         */
        public DenseVector mean;

        /**
         * Compressed covariance matrix by storing the lower triangle part.
         */
        public DenseVector cov;

        /**
         * The default constructor.
         */
        public ClusterSummary() {
        }

        /**
         * The constructor.
         *
         * @param clusterId Id of the cluster.
         * @param weight    Weight of the cluster.
         * @param mean      Mean of Gaussian distribution.
         * @param cov       Compressed covariance matrix by storing the lower triangle part.
         */
        public ClusterSummary(long clusterId, double weight, DenseVector mean, DenseVector cov) {
            this.clusterId = clusterId;
            this.weight = weight;
            this.mean = mean;
            this.cov = cov;
        }
    }

    /**
     * Get the matrix element position in the compact format.
     */
    public static int getElementPositionInCompactMatrix(int i, int j, int n) {
        Preconditions.checkArgument(i <= j);
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
