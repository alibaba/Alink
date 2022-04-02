package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.operator.common.similarity.KDTree;
import com.alibaba.alink.params.shared.clustering.HasFastDistanceType.DistanceType;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class LofDetector extends OutlierDetector {

	private static final double EPS = 1e-18;
	private final DistanceType distanceType;
	private int numNeighbors;

	public LofDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		numNeighbors = params.get(LofDetectorParams.NUM_NEIGHBORS);
		distanceType = params.get(LofDetectorParams.DISTANCE_TYPE);
	}

	/**
	 * Get percentile from an unsorted double array using linear interpolation
	 */
	protected static double calcPercentile(double percentile, double[] values) {
		if (percentile < 0. || percentile > 100.) {
			throw new IllegalArgumentException("Percentile must be in the range [0., 100.].");
		}
		final Integer[] indices = IntStream.range(0, values.length).boxed().toArray(Integer[]::new);
		Arrays.sort(indices, Comparator.comparingDouble(o -> values[o]));
		// Use linear interpolation
		double inaccurateIdx = (percentile / 100) * (indices.length - 1);
		int lowIdx = Double.valueOf(Math.floor(inaccurateIdx)).intValue();
		int highIdx = Double.valueOf(Math.ceil(inaccurateIdx)).intValue();
		return lowIdx == highIdx
			? values[indices[lowIdx]]
			: (inaccurateIdx - lowIdx) * values[indices[highIdx]] + (highIdx - inaccurateIdx) * values[indices[lowIdx]];
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast)
		throws Exception {
		Vector[] values = OutlierUtil.getVectors(series, params);
		FastDistance fastDistance = distanceType.getFastDistance();
		int n = values.length;

		if (0 == n) {
			//noinspection unchecked
			return new Tuple3[0];
		}
		numNeighbors = Math.min(numNeighbors, n - 1);

		// Get vector size
		int vectorSize = values[0].size();
		if (values[0] instanceof SparseVector && values[0].size() < 0) {
			for (Vector value : values) {
				SparseVector sv = (SparseVector) value;
				int[] indices = sv.getIndices();
				vectorSize = Math.max(vectorSize, indices[indices.length - 1] + 1);
			}
		}

		// For each sample, get k-neighbors and k-distance.
		FastDistanceVectorData[] fastDistanceVectors = new FastDistanceVectorData[n];
		for (int k = 0; k < n; k += 1) {
			fastDistanceVectors[k] = fastDistance.prepareVectorData(Row.of(values[k], k), 0, 1);
		}
		// The array is cloned as KDTree will reorder it.
		KDTree kdTree = new KDTree(fastDistanceVectors.clone(), vectorSize, fastDistance);
		kdTree.buildTree();

		int[][] kNeighbors = new int[n][numNeighbors];
		double[][] kNeighborDists = new double[n][numNeighbors];
		double[] kDist = new double[n];
		for (int k = 0; k < n; k += 1) {
			// The sample itself could be returned, so fetch numNeighbors + 1 neighbors
			Tuple2 <Double, Row>[] results = kdTree.getTopN(numNeighbors + 1, fastDistanceVectors[k]);
			int p = 0;
			for (Tuple2 <Double, Row> kNeighbor : results) {
				int index = (Integer) kNeighbor.f1.getField(0);
				if (index == k) {
					continue;
				}
				kNeighborDists[k][p] = Math.max(kNeighbor.f0, EPS);
				kNeighbors[k][p] = (Integer) kNeighbor.f1.getField(0);
				p += 1;
				if (p == numNeighbors) {
					break;
				}
			}
			kDist[k] = kNeighborDists[k][numNeighbors - 1];
		}

		// Calculate lrd (local reachability density) from reach-distance: reach(p, o)=max(k-distance(p),distance(p, o))
		double[] lrd = new double[n];
		for (int k = 0; k < n; k += 1) {
			for (int i = 0; i < numNeighbors; i += 1) {
				lrd[k] += Math.max(kNeighborDists[k][i], kDist[kNeighbors[k][i]]);
			}
			lrd[k] = numNeighbors / lrd[k];
		}

		// Calculate lof
		double[] lof = new double[n];
		for (int k = 0; k < n; k += 1) {
			double sumLrd = 0.;
			for (int i = 0; i < numNeighbors; i += 1) {
				sumLrd += lrd[kNeighbors[k][i]];
			}
			lof[k] = sumLrd / numNeighbors / lrd[k];
		}

		final double threshold = 1.5;
		//noinspection unchecked
		Tuple3 <Boolean, Double, Map <String, String>>[] results = detectLast ? new Tuple3[1] : new Tuple3[n];
		if (detectLast) {
			Map <String, String> info = new HashMap <>();
			info.put("lof", String.valueOf(lof[n - 1]));
			results[0] = Tuple3.of(lof[n - 1] > threshold, lof[n - 1], info);
		} else {
			for (int k = 0; k < n; k += 1) {
				Map <String, String> info = new HashMap <>();
				info.put("lof", String.valueOf(lof[k]));
				results[k] = Tuple3.of(lof[k] > threshold, lof[k], info);
			}
		}
		return results;
	}
}
