package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType;

import java.io.Serializable;
import java.util.Map;

public class BisectingKMeansModelData {
	public int k;
	public int vectorSize;
	public HasKMeansDistanceType.DistanceType distanceType;
	public String vectorColName;

	public Map <Long, ClusterSummary> summaries;

	public static class ClusterSummary implements Serializable {
		private static final long serialVersionUID = 8101449266148083555L;
		/**
		 * Cluster Id.
		 */
		public long clusterId;
		/**
		 * the number of points within this cluster
		 */
		public long size;
		/**
		 * the center of the points within this cluster
		 */
		public DenseVector center;
		/**
		 * the sum of squared distances to the center
		 */
		public double cost;

		@Override
		public String toString() {
			return String.format("size=%d,cost=%f,center=%s", size, cost, center.toString());
		}
	}
}
