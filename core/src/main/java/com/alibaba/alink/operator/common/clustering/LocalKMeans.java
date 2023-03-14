package com.alibaba.alink.operator.common.clustering;

import org.apache.flink.util.Collector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.clustering.common.Center;
import com.alibaba.alink.operator.common.clustering.common.Cluster;
import com.alibaba.alink.operator.common.clustering.common.Sample;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author guotao.gt
 */
public class LocalKMeans {

	public static void clustering(Iterable <Sample> values, Collector <Sample> out, int k, double epsilon,
								  int maxIter, ContinuousDistance distance) {
		List <Sample> samples = new ArrayList <>();
		for (Sample sample : values) {
			samples.add(sample);
		}
		Sample[] outSamples = new LocalKMeans().clustering(samples.toArray(new Sample[samples.size()]), k, epsilon,
			maxIter, distance);
		for (Sample sample : outSamples) {
			out.collect(sample);
		}
	}

	public static FindResult findCluster(Center[] centers, DenseVector sample, ContinuousDistance continuousDistance) {
		long clusterId = -1;
		double d = Double.POSITIVE_INFINITY;

		for (Center c : centers) {
			if (null != c) {
				double distance = continuousDistance.calc(sample, c.getVector());

				if (distance < d) {
					clusterId = c.getClusterId();
					d = distance;
				}
			}
		}
		return new FindResult(clusterId, d);
	}

	public static Center[] getCentersFromClusters(Sample[] samples, int k) {
		Cluster[] clusters = new Cluster[k];
		for (int i = 0; i < k; i++) {
			clusters[i] = new Cluster();
		}
		for (int i = 0; i < samples.length; i++) {
			clusters[(int) samples[i].getClusterId()].addSample(samples[i]);
		}
		List <Center> list = new ArrayList <>();
		for (int i = 0; i < clusters.length; i++) {
			if (clusters[i].getCenter().getVector() != null) {
				list.add(clusters[i].getCenter());
			}
		}
		for (int i = 0; i < list.size(); i++) {
			list.get(i).setClusterId((long) i);
		}
		return list.toArray(new Center[0]);
	}

	public Sample[] clustering(Sample[] samples, int k, double epsilon, int maxIter, ContinuousDistance distance) {
		k = k > samples.length ? samples.length : k;
		kMeansClustering(samples, epsilon, k, distance, maxIter);
		return samples;
	}

	/**
	 * get initial centers
	 *
	 * @param k
	 * @return
	 */
	private Center[] getInitialCenters(Sample[] samples, int k) {
		Center[] centers = new Center[k];
		int size = samples.length;
		boolean[] flags = new boolean[size];
		Arrays.fill(flags, false);
		//random
		if (k < size / 3) {
			int clusterId = 0;
			while (clusterId < k) {
				int randomInt = new Random().nextInt(size);
				if (!flags[randomInt]) {
					centers[clusterId] = new Center(clusterId, 0, samples[randomInt].getVector());
					clusterId++;
				}
				flags[randomInt] = true;
			}
			//topK
		} else {
			for (int i = 0; i < k; i++) {
				centers[i] = new Center(i, 0, samples[i].getVector());
			}
		}
		return centers;
	}

	private Center[] kMeansClustering(Sample[] samples, double epsilon, int k, ContinuousDistance distance,
									  int maxIter) {
		Center[] centers = getInitialCenters(samples, k);
		int iter = 0;
		double oldSsw = 0;
		while (iter++ < maxIter) {
			double newSsw = 0;
			for (int i = 0; i < samples.length; i++) {
				FindResult findResult = findCluster(centers, samples[i].getVector(), distance);
				samples[i].setClusterId(findResult.getClusterId());
				newSsw += findResult.getDistance();
			}
			centers = getCentersFromClusters(samples, k);
			if (Math.abs(newSsw - oldSsw) / samples.length < epsilon) {
				break;
			} else {
				oldSsw = newSsw;
			}
		}

		return centers;
	}

}
