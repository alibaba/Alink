package com.alibaba.alink.operator.common.clustering.kmeans;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.clustering.GeoKMeansTrainParams;
import com.alibaba.alink.params.clustering.KMeansTrainParams;
import com.alibaba.alink.params.shared.HasVectorSizeDefaultAs100;
import com.alibaba.alink.params.shared.clustering.HasKMeansWithHaversineDistanceType;

import java.io.Serializable;
import java.util.List;

/**
 * Model data for KMeans trainData.
 */
public class KMeansTrainModelData implements Serializable {
	private static final long serialVersionUID = -8409065834709001347L;
	public List <ClusterSummary> centroids;
	public ParamSummary params;

	public long getClusterId(int clusterIndex) {
		return centroids.get(clusterIndex).clusterId;
	}

	public double getClusterWeight(int clusterIndex) {
		return centroids.get(clusterIndex).weight;
	}

	public DenseVector getClusterVector(int clusterIndex) {
		return centroids.get(clusterIndex).vec;
	}

	public void setClusterWeight(int clusterIndex, double weight) {
		centroids.get(clusterIndex).weight = weight;
	}

	public static class ClusterSummary implements Serializable {
		private static final long serialVersionUID = 8905947466776009898L;
		/**
		 * Cluster Id.
		 */
		private long clusterId;

		/**
		 * Cluster Weight.
		 */
		private double weight;

		/**
		 * Cluster vector.
		 */
		private DenseVector vec;

		public ClusterSummary() {}

		public ClusterSummary(DenseVector vec, long clusterId, double weight) {
			this.vec = vec;
			this.clusterId = clusterId;
			this.weight = weight;
		}

		public double getWeight() {
			return weight;
		}

		public long getClusterId() {
			return clusterId;
		}
	}

	public static class ParamSummary implements Serializable {
		private static final long serialVersionUID = 6667640277509716708L;
		/**
		 * Cluster number.
		 */
		public int k;

		/**
		 * Cluster vector size.
		 */
		public int vectorSize;

		/**
		 * DistanceType.
		 */
		public HasKMeansWithHaversineDistanceType.DistanceType distanceType;

		/**
		 * Vector column name.
		 */
		public String vectorColName;

		/**
		 * Latitude column name.
		 */
		public String latitudeColName;

		/**
		 * Longtitude column name.
		 */
		public String longtitudeColName;

		public ParamSummary() {}

		public ParamSummary(Params params) {
			k = params.get(KMeansTrainParams.K);
			vectorSize = params.get(HasVectorSizeDefaultAs100.VECTOR_SIZE);
			distanceType = params.get(HasKMeansWithHaversineDistanceType.DISTANCE_TYPE);
			vectorColName = params.get(KMeansTrainParams.VECTOR_COL);
			latitudeColName = params.get(GeoKMeansTrainParams.LATITUDE_COL);
			longtitudeColName = params.get(GeoKMeansTrainParams.LONGITUDE_COL);
		}

		public Params toParams() {
			return new Params().set(HasKMeansWithHaversineDistanceType.DISTANCE_TYPE, distanceType)
				.set(KMeansTrainParams.K, k)
				.set(HasVectorSizeDefaultAs100.VECTOR_SIZE, vectorSize)
				.set(KMeansTrainParams.VECTOR_COL, vectorColName)
				.set(GeoKMeansTrainParams.LATITUDE_COL, latitudeColName)
				.set(GeoKMeansTrainParams.LONGITUDE_COL, longtitudeColName);
		}
	}
}
