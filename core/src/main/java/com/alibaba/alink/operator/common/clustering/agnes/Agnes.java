package com.alibaba.alink.operator.common.clustering.agnes;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.AlinkSerializable;
import com.alibaba.alink.operator.common.clustering.kmeans.KMeansInitCentroids;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author guotao.gt
 */
public class Agnes implements AlinkSerializable {
	private static final Logger LOG = LoggerFactory.getLogger(Agnes.class);

	/**
	 * @param agnesSamples
	 * @param k                 default 1
	 * @param distanceThreshold default double.MAX
	 * @return
	 */
	public static List <AgnesCluster> startAnalysis(List <AgnesSample> agnesSamples, int k, double distanceThreshold,
													Linkage linkage, ContinuousDistance distance) {
		List <AgnesCluster> originalClusters = initialCluster(agnesSamples);
		List <AgnesCluster> finalClusters = originalClusters;
		int iter = 1;
		while (true) {
			if (finalClusters.size() <= k) {
				break;
			}
			double min = Double.MAX_VALUE;
			int mergeIndexA = 0;
			int mergeIndexB = 0;
			for (int i = 0; i < finalClusters.size(); i++) {
				for (int j = 0; j < finalClusters.size(); j++) {
					if (i != j) {
						AgnesCluster clusterA = finalClusters.get(i);
						AgnesCluster clusterB = finalClusters.get(j);
						List <AgnesSample> dataPointsA = clusterA.getAgnesSamples();
						List <AgnesSample> dataPointsB = clusterB.getAgnesSamples();

						switch (linkage) {
							case MIN:
								double minDistance = Double.MAX_VALUE;
								for (int m = 0; m < dataPointsA.size(); m++) {
									for (int n = 0; n < dataPointsB.size(); n++) {
										double tempDis = distance.calc(
											dataPointsA.get(m).getVector(), dataPointsB.get(n).getVector());
										if (tempDis < minDistance) {
											minDistance = tempDis;
										}
									}
								}
								if (minDistance < min) {
									min = minDistance;
									mergeIndexA = i;
									mergeIndexB = j;
								}
								break;
							case MAX:
								double maxDistance = Double.MIN_VALUE;
								for (int m = 0; m < dataPointsA.size(); m++) {
									for (int n = 0; n < dataPointsB.size(); n++) {
										double tempDis = distance.calc(
											dataPointsA.get(m).getVector(), dataPointsB.get(n).getVector());
										if (tempDis > maxDistance) {
											maxDistance = tempDis;
										}
									}
								}
								if (maxDistance < min) {
									min = maxDistance;
									mergeIndexA = i;
									mergeIndexB = j;
								}
								break;
							case AVERAGE:
								double averageDistance = 0;
								for (int m = 0; m < dataPointsA.size(); m++) {
									for (int n = 0; n < dataPointsB.size(); n++) {
										averageDistance += distance.calc(
											dataPointsA.get(m).getVector(), dataPointsB.get(n).getVector());
									}
								}
								averageDistance /= dataPointsA.size();
								if (averageDistance < min) {
									min = averageDistance;
									mergeIndexA = i;
									mergeIndexB = j;
								}
								break;
							case MEAN:
								DenseVector vectorA = mean(dataPointsA, distance);
								DenseVector vectorB = mean(dataPointsB, distance);
								double meanDistance = distance.calc(vectorA, vectorB);
								if (meanDistance < min) {
									min = meanDistance;
									mergeIndexA = i;
									mergeIndexB = j;
								}
								break;
							default:
								throw new RuntimeException("linkage not support:" + linkage);

						}
					}
				}
			}
			finalClusters = mergeCluster(finalClusters, mergeIndexA, mergeIndexB, iter);
			LOG.info("Iteration:" + iter + "; distance:" + min);
			iter++;
			if (min > distanceThreshold) {
				break;
			}
		}

		return finalClusters;
	}

	private static List <AgnesCluster> mergeCluster(List <AgnesCluster> clusters, int mergeIndexA, int mergeIndexB,
													int mergeIter) {
		if (mergeIndexA != mergeIndexB) {
			AgnesCluster clusterA = clusters.get(mergeIndexA);
			AgnesCluster clusterB = clusters.get(mergeIndexB);
			List <AgnesSample> dpB = clusterB.getAgnesSamples();
			clusterB.getFirstSample().setParentId(clusterA.getFirstSample().getSampleId());
			clusterB.getFirstSample().setMergeIter(mergeIter);
			for (AgnesSample dp : dpB) {
				dp.setClusterId(clusterA.getClusterId());
				clusterA.addDataPoints(dp);
			}

			clusters.remove(mergeIndexB);
		}

		return clusters;
	}

	private static List <AgnesCluster> initialCluster(List <AgnesSample> agnesSamples) {
		List <AgnesCluster> originalClusters = new ArrayList <>();
		for (int i = 0; i < agnesSamples.size(); i++) {
			agnesSamples.get(i).setClusterId(i);
			originalClusters.add(new AgnesCluster(i, agnesSamples.get(i)));
		}
		return originalClusters;
	}

	/**
	 * calc the center of cluster
	 *
	 * @param agnesSamples
	 * @return
	 */
	public static DenseVector mean(List <AgnesSample> agnesSamples, ContinuousDistance distance) {
		if (null != agnesSamples && agnesSamples.size() > 0) {
			int dim = agnesSamples.get(0).getVector().size();
			DenseVector r = new DenseVector(dim);
			for (AgnesSample dp : agnesSamples) {
				r.plusEqual((DenseVector) dp.getVector());
			}
			r.scaleEqual(1.0 / agnesSamples.size());
			return r;
		}
		return null;
	}

}