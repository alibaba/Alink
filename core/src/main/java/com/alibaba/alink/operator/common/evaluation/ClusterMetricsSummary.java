package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import org.apache.commons.math3.stat.StatUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Cluster Metrics.
 */
public class ClusterMetricsSummary implements BaseMetricsSummary <ClusterMetrics, ClusterMetricsSummary> {

	private static final long serialVersionUID = -8098955200262101253L;
	/**
	 * Save the ClusterId from all clusters.
	 */
	public List <String> clusterId = new ArrayList <>();
	/**
	 * Save the ClusterCnt from all clusters, the size must be equal to k.
	 */
	List <Integer> clusterCnt = new ArrayList <>();
	/**
	 * Save the Compactness from all clusters, the size must be equal to k.
	 */
	List <Double> compactness = new ArrayList <>();
	/**
	 * Save the DistanceSquareSum from all clusters, the size must be equal to k.
	 */
	List <Double> distanceSquareSum = new ArrayList <>();
	/**
	 * Save the VectorNormL2Sum from all clusters, the size must be equal to k.
	 */
	List <Double> vectorNormL2Sum = new ArrayList <>();
	/**
	 * Save the MeanVector from all clusters, the size must be equal to k.
	 */
	List <DenseVector> meanVector = new ArrayList <>();

	/**
	 * Sum of all the samples.
	 */
	DenseVector sumVector;

	/**
	 * Cluster Number, the size of ArrayList above must be equal to k.
	 */
	int k;

	/**
	 * The number of samples.
	 */
	int total;

	/**
	 * distance to measure the distance of two vectors, it could only be EuclideanDistance or CosineDistance.
	 */
	ContinuousDistance distance;

	public ClusterMetricsSummary(String clusterId,
								 int clusterCnt,
								 double compactness,
								 double distanceSquareSum,
								 double vectorNormL2Sum,
								 DenseVector meanVector,
								 ContinuousDistance distance,
								 DenseVector sumVector) {
		this.clusterId.add(clusterId);
		this.clusterCnt.add(clusterCnt);
		this.compactness.add(compactness);
		this.distanceSquareSum.add(distanceSquareSum);
		this.vectorNormL2Sum.add(vectorNormL2Sum);
		this.meanVector.add(meanVector);

		this.k = 1;
		this.sumVector = sumVector;
		this.total = clusterCnt;
		this.distance = distance;
	}

	@Override
	public ClusterMetricsSummary merge(ClusterMetricsSummary metrics) {
		if (null == metrics) {
			return this;
		}
		this.clusterId.addAll(metrics.clusterId);
		this.clusterCnt.addAll(metrics.clusterCnt);
		this.compactness.addAll(metrics.compactness);
		this.distanceSquareSum.addAll(metrics.distanceSquareSum);
		this.vectorNormL2Sum.addAll(metrics.vectorNormL2Sum);
		this.meanVector.addAll(metrics.meanVector);
		this.k += metrics.k;
		this.sumVector.plusEqual(metrics.sumVector);
		this.total += metrics.total;
		return this;
	}

	@Override
	public ClusterMetrics toMetrics() {
		Params params = new Params();

		DenseVector meanVector = sumVector.scale(1.0 / total);
		if (distance instanceof CosineDistance) {
			meanVector.scaleEqual(1.0 / meanVector.normL2());
		}

		String[] clusters = new String[k];
		double[] countArray = new double[k];
		double ssb = 0.0;
		double ssw = 0.0;
		double compactness = 0.0;
		double seperation = 0.0;

		for (int i = 0; i < this.k; i++) {
			clusters[i] = clusterId.get(i);
			countArray[i] = clusterCnt.get(i);
			ssb += Math.pow(distance.calc(this.meanVector.get(i), meanVector), 2) * clusterCnt.get(i);
			ssw += distanceSquareSum.get(i);
			compactness += this.compactness.get(i);
		}

		double[] DBIndexArray = new double[k];
		for (int i = 0; i < k; i++) {
			for (int j = i + 1; j < k; j++) {
				double d = distance.calc(this.meanVector.get(i), this.meanVector.get(j));
				seperation += d;
				double tmp = (this.compactness.get(i) + this.compactness.get(j)) / d;
				DBIndexArray[i] = Math.max(DBIndexArray[i], tmp);
				DBIndexArray[j] = Math.max(DBIndexArray[j], tmp);
			}
		}

		double DBIndex = k > 1 ? StatUtils.sum(DBIndexArray) / k : Double.POSITIVE_INFINITY;
		params.set(ClusterMetrics.SSB, ssb);
		params.set(ClusterMetrics.SSW, ssw);
		params.set(ClusterMetrics.CP, compactness / k);
		params.set(ClusterMetrics.K, k);
		params.set(ClusterMetrics.COUNT, total);
		params.set(ClusterMetrics.SP, k > 1 ? 2 * seperation / (k * k - k) : 0.);
		params.set(ClusterMetrics.DB, DBIndex);
		params.set(ClusterMetrics.VRC, k > 1 ? ssb * (total - k) / ssw / (k - 1) : 0);
		params.set(ClusterMetrics.CLUSTER_ARRAY, clusters);
		params.set(ClusterMetrics.COUNT_ARRAY, countArray);

		return new ClusterMetrics(params);
	}

	public static ClusterMetrics createForEmptyDataset() {
		Params params = new Params();
		params.set(ClusterMetrics.SSB, 0.);
		params.set(ClusterMetrics.SSW, Double.POSITIVE_INFINITY);
		params.set(ClusterMetrics.CP, Double.POSITIVE_INFINITY);
		params.set(ClusterMetrics.K, 0);
		params.set(ClusterMetrics.COUNT, 0);
		params.set(ClusterMetrics.SP, 0.);
		params.set(ClusterMetrics.DB, Double.POSITIVE_INFINITY);
		params.set(ClusterMetrics.VRC, 0.);
		params.set(ClusterMetrics.CLUSTER_ARRAY, new String[0]);
		params.set(ClusterMetrics.COUNT_ARRAY, new double[0]);
		params.set(ClusterMetrics.SILHOUETTE_COEFFICIENT, -1.);
		return new ClusterMetrics(params);
	}
}
