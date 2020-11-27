package com.alibaba.alink.operator.common.clustering;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;

/**
 * Summary of a Gaussian Mixture Model cluster.
 */
public class GmmClusterSummary implements Serializable {
	private static final long serialVersionUID = -8205517186576977360L;
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
	public GmmClusterSummary() {
	}

	/**
	 * The constructor.
	 *
	 * @param clusterId Id of the cluster.
	 * @param weight    Weight of the cluster.
	 * @param mean      Mean of Gaussian distribution.
	 * @param cov       Compressed covariance matrix by storing the lower triangle part.
	 */
	public GmmClusterSummary(long clusterId, double weight, DenseVector mean, DenseVector cov) {
		this.clusterId = clusterId;
		this.weight = weight;
		this.mean = mean;
		this.cov = cov;
	}

	@Override
	public String toString() {
		return JsonConverter.toJson(this);
	}

	public static GmmClusterSummary fromString(String s) {
		return JsonConverter.fromJson(s, GmmClusterSummary.class);
	}
}
